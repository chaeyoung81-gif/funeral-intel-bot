"""
일정관리 봇 (schedule_bot.py)
------------------------------
기능:
  1. 텍스트 메시지 → 자동 할 일 등록
  2. 매일 저녁 9시 KST → 미완료 일정 알림
  3. /완료 [번호 또는 텍스트] → 완료 처리
  4. /목록 → 현재 할 일 목록
  5. 로컬 todos.json에 저장

봇 토큰: schedule_bot 전용
"""

import asyncio
import json
import logging
import re
import uuid
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, ContextTypes, MessageHandler, filters

load_dotenv()

# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------
SCHEDULE_BOT_TOKEN = "8640988207:AAEbTQzcZjPaA7YVQLiw7O2wrHy5ZXXZ8RA"
KST = ZoneInfo("Asia/Seoul")
BASE_DIR = Path(__file__).parent
TODO_FILE = BASE_DIR / "todos.json"
CHAT_IDS_FILE = BASE_DIR / "schedule_chat_ids.json"

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    handlers=[
        logging.FileHandler(BASE_DIR / "schedule.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 데이터 관리
# ---------------------------------------------------------------------------
def load_todos() -> list[dict]:
    """todos.json 로드. 없으면 빈 리스트."""
    try:
        if TODO_FILE.exists():
            return json.loads(TODO_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"todos 로드 실패: {e}")
    return []


def save_todos(todos: list[dict]) -> None:
    TODO_FILE.write_text(
        json.dumps(todos, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def load_chat_ids() -> set[int]:
    """알림 수신 채팅 ID 목록 로드."""
    try:
        if CHAT_IDS_FILE.exists():
            return set(json.loads(CHAT_IDS_FILE.read_text(encoding="utf-8")))
    except Exception:
        pass
    return set()


def save_chat_ids(ids: set[int]) -> None:
    CHAT_IDS_FILE.write_text(
        json.dumps(list(ids), ensure_ascii=False), encoding="utf-8"
    )


def pending_todos(todos: list[dict]) -> list[dict]:
    """완료되지 않은 항목만 반환 (인덱스 유지)."""
    return [t for t in todos if not t.get("completed")]


def format_todo_list(todos: list[dict]) -> str:
    """미완료 항목을 번호 포함 텍스트로 포맷."""
    items = pending_todos(todos)
    if not items:
        return "✅ 할 일이 없습니다!"
    lines = ["📋 *할 일 목록*\n"]
    # 전체 리스트에서 번호를 매겨야 /완료 1 등이 직관적
    pending_indexed = [(i + 1, t) for i, t in enumerate(todos) if not t.get("completed")]
    for seq, (num, t) in enumerate(pending_indexed, 1):
        created = t.get("created_at", "")[:10]
        lines.append(f"{seq}\\. {t['text']}  _{created}_")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 핸들러
# ---------------------------------------------------------------------------
async def handle_add(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """일반 텍스트 → 할 일 등록."""
    chat_id = update.effective_chat.id
    text = (update.message.text or "").strip()

    if not text:
        return

    # 채팅 ID 저장 (알림 수신 대상)
    ids = load_chat_ids()
    if chat_id not in ids:
        ids.add(chat_id)
        save_chat_ids(ids)

    todo = {
        "id": str(uuid.uuid4()),
        "text": text,
        "created_at": datetime.now(KST).isoformat(),
        "completed": False,
        "completed_at": None,
    }

    todos = load_todos()
    todos.append(todo)
    save_todos(todos)

    pending = pending_todos(todos)
    seq = len(pending)  # 방금 추가됐으므로 마지막 순서
    await update.message.reply_text(
        f"✅ 할 일이 등록됐습니다\\!\n\n*{seq}\\. {_esc(text)}*\n\n"
        f"현재 미완료 항목: {len(pending)}개",
        parse_mode="MarkdownV2",
    )
    logger.info(f"할 일 추가: {text}")


async def handle_wanryo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/완료 [번호 또는 텍스트] → 완료 처리."""
    text = (update.message.text or "").strip()
    # '/완료' 뒤 인자 파싱
    arg = re.sub(r"^/완료\s*", "", text).strip()

    todos = load_todos()
    pending = [(i, t) for i, t in enumerate(todos) if not t.get("completed")]

    if not pending:
        await update.message.reply_text("✅ 완료할 항목이 없습니다!")
        return

    if not arg:
        # 인자 없으면 목록 안내
        lines = ["완료할 항목 번호나 텍스트를 입력하세요.\n", "/완료 1  또는  /완료 장보기\n"]
        for seq, (_, t) in enumerate(pending, 1):
            lines.append(f"{seq}. {t['text']}")
        await update.message.reply_text("\n".join(lines))
        return

    matched_idx = None

    # 숫자면 순번으로 처리
    if arg.isdigit():
        seq = int(arg) - 1
        if 0 <= seq < len(pending):
            matched_idx, matched_todo = pending[seq]
        else:
            await update.message.reply_text(f"❌ {arg}번 항목이 없습니다. (미완료: {len(pending)}개)")
            return
    else:
        # 텍스트 포함 검색 (대소문자 무시)
        arg_lower = arg.lower()
        for orig_idx, t in pending:
            if arg_lower in t["text"].lower():
                matched_idx = orig_idx
                matched_todo = t
                break
        if matched_idx is None:
            await update.message.reply_text(f"❌ '{arg}'과 일치하는 항목을 찾지 못했습니다.")
            return

    todos[matched_idx]["completed"] = True
    todos[matched_idx]["completed_at"] = datetime.now(KST).isoformat()
    save_todos(todos)

    remaining = len(pending_todos(todos))
    await update.message.reply_text(
        f"✅ 완료 처리됐습니다\\!\n\n~~{_esc(matched_todo['text'])}~~\n\n"
        f"남은 항목: {remaining}개",
        parse_mode="MarkdownV2",
    )
    logger.info(f"완료 처리: {matched_todo['text']}")


async def handle_mokrok(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/목록 → 현재 할 일 목록."""
    # 채팅 ID 저장
    chat_id = update.effective_chat.id
    ids = load_chat_ids()
    if chat_id not in ids:
        ids.add(chat_id)
        save_chat_ids(ids)

    todos = load_todos()
    msg = format_todo_list(todos)
    try:
        await update.message.reply_text(msg, parse_mode="MarkdownV2")
    except Exception:
        await update.message.reply_text(msg.replace("\\", "").replace("*", "").replace("_", ""))


# ---------------------------------------------------------------------------
# 저녁 9시 알림
# ---------------------------------------------------------------------------
async def send_evening_reminder(bot) -> None:
    """매일 21:00 KST — 미완료 항목 알림."""
    todos = load_todos()
    items = pending_todos(todos)
    if not items:
        logger.info("저녁 알림: 미완료 없음")
        return

    today_str = datetime.now(KST).strftime("%m월 %d일")
    lines = [f"🌙 *{today_str} 저녁 9시 알림*\n", f"오늘 아직 못 한 일이 {len(items)}개 있어요\\!\n"]
    for seq, t in enumerate(items, 1):
        lines.append(f"{seq}\\. {_esc(t['text'])}")
    lines.append("\n/완료 \\[번호\\] 로 완료 처리하세요\\.")
    msg = "\n".join(lines)

    ids = load_chat_ids()
    for chat_id in ids:
        try:
            await bot.send_message(chat_id=chat_id, text=msg, parse_mode="MarkdownV2")
            logger.info(f"저녁 알림 전송: chat_id={chat_id}, 미완료={len(items)}개")
        except Exception as e:
            logger.error(f"저녁 알림 실패 (chat_id={chat_id}): {e}")


# ---------------------------------------------------------------------------
# 유틸
# ---------------------------------------------------------------------------
def _esc(text: str) -> str:
    """MarkdownV2 이스케이프."""
    special = r"\_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in special else c for c in text)


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------
async def main() -> None:
    app = Application.builder().token(SCHEDULE_BOT_TOKEN).build()

    # 한글 명령어: Regex 필터로 처리 (CommandHandler는 영문만 지원)
    app.add_handler(MessageHandler(filters.Regex(r"^/완료"), handle_wanryo))
    app.add_handler(MessageHandler(filters.Regex(r"^/목록"), handle_mokrok))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_add))

    # 스케줄러: 매일 21:00 KST
    scheduler = AsyncIOScheduler(timezone=KST)
    scheduler.add_job(
        send_evening_reminder,
        trigger="cron",
        hour=21,
        minute=0,
        args=[app.bot],
    )
    scheduler.start()
    logger.info("스케줄러 시작 (매일 21:00 KST 알림)")

    async with app:
        await app.start()
        await app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        logger.info("일정관리 봇 시작. Ctrl+C로 종료.")
        await asyncio.Event().wait()  # 영구 대기
        await app.updater.stop()
        await app.stop()


if __name__ == "__main__":
    asyncio.run(main())
