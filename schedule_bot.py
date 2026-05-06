#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
하루 마무리 봇 (Check-in Bot)
- 매일 21:00 KST에 채영님에게 "오늘 어땠어?" 질문
- 답변을 노션 DB에 저장
- 누적/연속 일수 카운트
- 일요일 21:30에 주간 관찰 자동 발송
- 매월 1일 21:30에 월간 회고 자동 발송
- 7/30/60/100일 마일스톤 알림
"""

import os
import logging
import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters, ContextTypes
)
from notion_client import Client as NotionClient
from anthropic import Anthropic

# === 설정 ===
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_CHECKIN_BOT_TOKEN")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_DB_ID = os.getenv("NOTION_CHECKIN_DB_ID")
CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID"))

# 한국 시간대
KST = timezone(timedelta(hours=9))

# 알림 시간
DAILY_TIME = (21, 0)        # 21:00 KST 매일 질문
WEEKLY_TIME = (21, 30)      # 일요일 21:30 KST 주간 관찰
MONTHLY_TIME = (21, 30)     # 매월 1일 21:30 KST 월간 회고

# 상태 파일
STATE_FILE = Path(__file__).parent / "checkin_state.json"

# 로깅
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# 클라이언트
notion = NotionClient(auth=NOTION_API_KEY)
anthropic_client = Anthropic(api_key=ANTHROPIC_API_KEY)


# === 상태 관리 ===
def load_state():
    """상태 파일 로드. 없으면 기본값."""
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "last_record_date": None,      # 마지막 기록 날짜 (YYYY-MM-DD)
        "total_days": 0,                # 누적 일수
        "streak_days": 0,               # 연속 일수
        "awaiting_answer": False,       # 질문 던지고 답 기다리는 중인지
        "today_question_date": None,    # 오늘 질문 날짜
    }


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


# === 노션 ===
def save_to_notion(date_str, today_text, tomorrow_text, extra_text, total, streak):
    """노션 DB에 한 줄 추가."""
    title = f"{date_str}"
    
    properties = {
        "일일 기록": {
            "title": [{"text": {"content": title}}]
        },
        "날짜": {
            "date": {"start": date_str}
        },
        "오늘": {
            "rich_text": [{"text": {"content": today_text or ""}}]
        },
        "내일": {
            "rich_text": [{"text": {"content": tomorrow_text or ""}}]
        },
        "추가 메모": {
            "rich_text": [{"text": {"content": extra_text or ""}}]
        },
        "누적일수": {
            "number": total
        },
        "연속일수": {
            "number": streak
        },
    }
    
    try:
        notion.pages.create(
            parent={"database_id": NOTION_DB_ID},
            properties=properties,
        )
        logger.info(f"노션 저장 완료: {date_str}")
        return True
    except Exception as e:
        logger.error(f"노션 저장 실패: {e}")
        return False


def update_notion_extra(date_str, extra_text):
    """오늘 기록에 추가 메모 누적."""
    try:
        results = notion.databases.query(
            database_id=NOTION_DB_ID,
            filter={
                "property": "날짜",
                "date": {"equals": date_str}
            }
        )
        if not results["results"]:
            return False
        page_id = results["results"][0]["id"]
        existing = results["results"][0]["properties"]["추가 메모"]["rich_text"]
        existing_text = existing[0]["text"]["content"] if existing else ""
        new_text = f"{existing_text}\n{extra_text}" if existing_text else extra_text
        
        notion.pages.update(
            page_id=page_id,
            properties={
                "추가 메모": {
                    "rich_text": [{"text": {"content": new_text}}]
                }
            }
        )
        return True
    except Exception as e:
        logger.error(f"노션 메모 업데이트 실패: {e}")
        return False


def fetch_recent_records(days):
    """최근 N일 기록 가져오기."""
    today = datetime.now(KST).date()
    start_date = today - timedelta(days=days)
    try:
        results = notion.databases.query(
            database_id=NOTION_DB_ID,
            filter={
                "property": "날짜",
                "date": {"on_or_after": start_date.isoformat()}
            },
            sorts=[{"property": "날짜", "direction": "ascending"}]
        )
        records = []
        for page in results["results"]:
            props = page["properties"]
            date = props["날짜"]["date"]["start"] if props["날짜"]["date"] else ""
            today_text = props["오늘"]["rich_text"][0]["text"]["content"] if props["오늘"]["rich_text"] else ""
            tomorrow_text = props["내일"]["rich_text"][0]["text"]["content"] if props["내일"]["rich_text"] else ""
            extra_text = props["추가 메모"]["rich_text"][0]["text"]["content"] if props["추가 메모"]["rich_text"] else ""
            records.append({
                "date": date,
                "today": today_text,
                "tomorrow": tomorrow_text,
                "extra": extra_text,
            })
        return records
    except Exception as e:
        logger.error(f"노션 조회 실패: {e}")
        return []


# === Claude 분석 ===
def analyze_with_claude(records, period_label):
    """Claude로 패턴 관찰 + 맥락 연결."""
    if not records:
        return None
    
    records_text = "\n\n".join([
        f"[{r['date']}]\n오늘: {r['today']}\n내일: {r['tomorrow']}\n메모: {r['extra']}"
        for r in records
    ])
    
    prompt = f"""다음은 채영님의 {period_label} 하루 마무리 기록이야.

{records_text}

이 기록을 보고 채영님에게 짧게 알려줘. 톤은 평가 아니라 관찰자의 시선이야. 친한 친구가 옆에서 같이 봐주는 느낌으로.

다음 구조로:
1. 이번 {period_label} 관찰 (3-4줄): 반복된 단어, 컨디션 흐름, 눈에 띄는 패턴
2. 맥락 연결 (2-3줄): 그 패턴이 어떤 맥락에서 나왔는지, 채영님이 이미 가진 정보로 더 나은 선택을 할 수 있는 힌트

규칙:
- 칭찬도 자책도 아닌 담백한 톤
- "~하면 좋겠다" 같은 조언은 가볍게, 강요 X
- 채영님이 이미 알 만한 것은 짧게만
- 짧고 가독성 좋게. 줄바꿈 적절히
- 마크다운 강조 표시(**) 쓰지 말 것
- 텔레그램에서 자연스럽게 읽히게"""
    
    try:
        response = anthropic_client.messages.create(
            model="claude-opus-4-5",
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text
    except Exception as e:
        logger.error(f"Claude 분석 실패: {e}")
        return None


# === 텔레그램 핸들러 ===
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "안녕 채영. 매일 밤 9시에 하루 어땠는지 물어볼게.\n"
        "라이딩 중이면 짧게, 여유 있으면 길게 답해도 돼.\n\n"
        "/status - 누적/연속 일수 보기\n"
        "/recap7 - 최근 7일 회고\n"
        "/recap30 - 최근 30일 회고\n"
        "/test - 지금 바로 질문 받아보기"
    )


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    state = load_state()
    msg = (
        f"누적 {state['total_days']}일 / 연속 {state['streak_days']}일\n"
        f"마지막 기록: {state['last_record_date'] or '아직 없음'}"
    )
    await update.message.reply_text(msg)


async def cmd_test(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """테스트용 - 지금 질문 던지기."""
    await send_daily_question(context.application)


async def cmd_recap7(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_recap(context.application, days=7, label="지난 7일")


async def cmd_recap30(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_recap(context.application, days=30, label="지난 30일")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """일반 메시지 처리 - 답변 받기."""
    if update.message.chat_id != CHAT_ID:
        return
    
    text = update.message.text.strip()
    if not text:
        return
    
    state = load_state()
    today_str = datetime.now(KST).strftime("%Y-%m-%d")
    
    # 이미 오늘 기록한 경우 → 추가 메모로 누적
    if state["last_record_date"] == today_str:
        ok = update_notion_extra(today_str, text)
        if ok:
            await update.message.reply_text("그것도 같이 적어둘게. 모르는 채로 둬도 괜찮아.")
        else:
            await update.message.reply_text("저장이 잘 안 됐어. 잠시 후 다시 시도해봐.")
        return
    
    # 답변 대기 중이거나 그냥 자발적 메시지 → 새 기록
    today_text, tomorrow_text = parse_answer(text)
    
    # 누적/연속 카운트 업데이트
    yesterday_str = (datetime.now(KST).date() - timedelta(days=1)).strftime("%Y-%m-%d")
    new_total = state["total_days"] + 1
    if state["last_record_date"] == yesterday_str:
        new_streak = state["streak_days"] + 1
    else:
        new_streak = 1
    
    ok = save_to_notion(today_str, today_text, tomorrow_text, "", new_total, new_streak)
    
    if ok:
        state["last_record_date"] = today_str
        state["total_days"] = new_total
        state["streak_days"] = new_streak
        state["awaiting_answer"] = False
        save_state(state)
        
        # 응답
        reply = f"기록됐어. (누적 {new_total}일 / 연속 {new_streak}일"
        if new_streak in [7, 30, 60, 100]:
            reply += f" 🎉)"
            await update.message.reply_text(reply)
            await asyncio.sleep(1)
            await send_milestone(context.application, new_streak)
        else:
            reply += ")"
            await update.message.reply_text(reply)
    else:
        await update.message.reply_text("저장이 잘 안 됐어. 다시 시도해줘.")


def parse_answer(text):
    """답변에서 '오늘'과 '내일' 부분을 분리. 단순한 휴리스틱."""
    if "내일" in text:
        idx = text.find("내일")
        today = text[:idx].strip().rstrip(".,;").strip()
        tomorrow = text[idx:].strip()
        return today, tomorrow
    return text, ""


# === 자동 발송 ===
async def send_daily_question(app):
    """매일 21:00 KST 질문 발송."""
    state = load_state()
    today_str = datetime.now(KST).strftime("%Y-%m-%d")
    
    # 오늘 이미 기록했으면 패스
    if state["last_record_date"] == today_str:
        logger.info(f"{today_str} 이미 기록됨. 질문 발송 스킵.")
        return
    
    # 빠진 날 다음 처리
    last = state["last_record_date"]
    if last:
        last_date = datetime.strptime(last, "%Y-%m-%d").date()
        today_date = datetime.now(KST).date()
        gap = (today_date - last_date).days
        if gap >= 2:
            msg = f"채영, {gap}일만에 돌아왔네. 오늘 어땠어? 그리고 내일은 뭐가 있어?"
        else:
            msg = "채영, 오늘 어땠어? 그리고 내일은 뭐가 있어?"
    else:
        msg = "채영, 오늘 어땠어? 그리고 내일은 뭐가 있어?"
    
    await app.bot.send_message(chat_id=CHAT_ID, text=msg)
    state["awaiting_answer"] = True
    state["today_question_date"] = today_str
    save_state(state)
    logger.info(f"일일 질문 발송: {today_str}")


async def send_recap(app, days, label):
    """회고 발송."""
    records = fetch_recent_records(days)
    if not records:
        await app.bot.send_message(chat_id=CHAT_ID, text=f"{label} 기록이 아직 없어.")
        return
    
    analysis = analyze_with_claude(records, label)
    if analysis:
        msg = f"📋 {label} 관찰\n\n{analysis}"
        await app.bot.send_message(chat_id=CHAT_ID, text=msg)
    else:
        await app.bot.send_message(chat_id=CHAT_ID, text="분석이 잘 안 됐어. 잠시 후 다시 시도해봐.")


async def send_milestone(app, streak):
    """7/30/60/100일 마일스톤."""
    if streak == 7:
        msg = "채영, 7일 연속이야 🎉\n첫 일주일 마쳤다."
        await app.bot.send_message(chat_id=CHAT_ID, text=msg)
    elif streak == 30:
        msg = "채영, 30일 연속 기록 🎉\n한 달간의 흐름을 같이 살펴볼래?"
        await app.bot.send_message(chat_id=CHAT_ID, text=msg)
        await asyncio.sleep(1)
        await send_recap(app, days=30, label="지난 한 달")
    elif streak == 60:
        msg = "채영, 60일이야. 두 달 누적됐어 🎉"
        await app.bot.send_message(chat_id=CHAT_ID, text=msg)
    elif streak == 100:
        msg = "채영, 100일 연속이야 🎉🎉🎉\n100일 누적된 채영님의 흐름을 같이 보자."
        await app.bot.send_message(chat_id=CHAT_ID, text=msg)
        await asyncio.sleep(1)
        await send_recap(app, days=100, label="지난 100일")


async def send_weekly(app):
    """일요일 주간 관찰."""
    today = datetime.now(KST)
    if today.weekday() != 6:  # 6 = Sunday
        return
    await send_recap(app, days=7, label="이번 주")


async def send_monthly(app):
    """매월 1일 월간 회고."""
    today = datetime.now(KST)
    if today.day != 1:
        return
    await send_recap(app, days=30, label="지난 한 달")


# === 스케줄러 ===
async def scheduler(app):
    """매분 체크해서 정해진 시간에 작업 실행."""
    last_run_daily = None
    last_run_weekly = None
    last_run_monthly = None
    
    while True:
        try:
            now = datetime.now(KST)
            current_date = now.date().isoformat()
            
            # 일일 질문 (21:00)
            if (now.hour, now.minute) == DAILY_TIME and last_run_daily != current_date:
                await send_daily_question(app)
                last_run_daily = current_date
            
            # 주간 관찰 (일요일 21:30)
            if (now.hour, now.minute) == WEEKLY_TIME and now.weekday() == 6 and last_run_weekly != current_date:
                await send_weekly(app)
                last_run_weekly = current_date
            
            # 월간 회고 (매월 1일 21:30)
            if (now.hour, now.minute) == MONTHLY_TIME and now.day == 1 and last_run_monthly != current_date:
                await send_monthly(app)
                last_run_monthly = current_date
            
            await asyncio.sleep(30)
        except Exception as e:
            logger.error(f"스케줄러 오류: {e}")
            await asyncio.sleep(60)


# === 메인 ===
async def post_init(app):
    """봇 시작 후 스케줄러 백그라운드 시작."""
    asyncio.create_task(scheduler(app))
    logger.info("스케줄러 시작됨.")


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("test", cmd_test))
    app.add_handler(CommandHandler("recap7", cmd_recap7))
    app.add_handler(CommandHandler("recap30", cmd_recap30))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    logger.info("하루 마무리 봇 시작.")
    app.run_polling()


if __name__ == "__main__":
    main()
