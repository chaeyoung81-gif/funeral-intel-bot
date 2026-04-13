"""
Telegram + Claude API + Notion 통합 봇
--------------------------------------
동작 흐름:
  1. 텔레그램 메시지 수신
  2. Claude API로 분석 및 답변 생성
  3. 텔레그램으로 답장 전송
  4. 대화 내용을 Notion 데이터베이스에 저장

환경 변수 (.env):
  TELEGRAM_BOT_TOKEN  - BotFather에서 발급받은 텔레그램 봇 토큰
  ANTHROPIC_API_KEY   - Anthropic API 키
  NOTION_API_KEY      - Notion Integration 토큰
  NOTION_DATABASE_ID  - 저장할 Notion 데이터베이스 ID

Notion 데이터베이스 필수 속성:
  Message  (Title)      - 사용자 메시지
  Response (Rich Text)  - Claude 답변
  Username (Rich Text)  - 텔레그램 사용자명
  User ID  (Rich Text)  - 텔레그램 사용자 ID
  Date     (Date)       - 대화 시각
"""

import logging
import os
from datetime import datetime, timezone

import anthropic
from dotenv import load_dotenv
from notion_client import AsyncClient as NotionAsyncClient
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------
TELEGRAM_BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
ANTHROPIC_API_KEY: str = os.environ["ANTHROPIC_API_KEY"]
NOTION_API_KEY: str = os.environ["NOTION_API_KEY"]
NOTION_DATABASE_ID: str = os.environ["NOTION_DATABASE_ID"]

TELEGRAM_LIMIT = 4096   # 텔레그램 메시지 최대 길이
NOTION_LIMIT = 2000     # Notion rich_text 블록 1개 최대 길이

# ---------------------------------------------------------------------------
# 클라이언트
# ---------------------------------------------------------------------------
claude = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
notion = NotionAsyncClient(auth=NOTION_API_KEY)

# ---------------------------------------------------------------------------
# 시스템 프롬프트 (Claude API prompt caching 적용)
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = (
    "You are a helpful AI assistant integrated into a Telegram bot. "
    "Provide thoughtful, accurate, and concise responses. "
    "Be friendly, direct, and appropriately brief for a chat interface. "
    "When responding in Korean, maintain natural conversational Korean."
)


# ---------------------------------------------------------------------------
# 유틸
# ---------------------------------------------------------------------------
def chunk_text(text: str, limit: int) -> list[str]:
    """텍스트를 limit 길이 단위로 분할."""
    if len(text) <= limit:
        return [text]
    return [text[i : i + limit] for i in range(0, len(text), limit)]


def build_notion_rich_text(text: str) -> list[dict]:
    """Notion rich_text 배열 생성 (2000자 제한 자동 처리)."""
    return [{"text": {"content": chunk}} for chunk in chunk_text(text, NOTION_LIMIT)]


# ---------------------------------------------------------------------------
# Claude API
# ---------------------------------------------------------------------------
async def get_claude_response(user_message: str) -> str:
    """Claude API에 메시지를 전송하고 답변을 반환합니다.

    시스템 프롬프트에 prompt caching을 적용하여 반복 호출 비용을 절감합니다.
    """
    response = await claude.messages.create(
        model="claude-opus-4-6",
        max_tokens=1024,
        system=[
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},  # 시스템 프롬프트 캐싱
            }
        ],
        messages=[{"role": "user", "content": user_message}],
    )
    return next(
        (block.text for block in response.content if block.type == "text"),
        "응답을 생성할 수 없습니다.",
    )


# ---------------------------------------------------------------------------
# Notion 저장
# ---------------------------------------------------------------------------
async def save_to_notion(
    user_id: int,
    username: str,
    user_message: str,
    claude_response: str,
) -> None:
    """대화 내용을 Notion 데이터베이스에 저장합니다."""
    await notion.pages.create(
        parent={"database_id": NOTION_DATABASE_ID},
        properties={
            "이름": {
                "title": [{"text": {"content": user_message[:NOTION_LIMIT]}}]
            },
            "원문": {
                "rich_text": [{"text": {"content": user_message[:NOTION_LIMIT]}}]
            },
            "분석결과": {
                "rich_text": build_notion_rich_text(claude_response)
            },
            "출처": {
                "rich_text": [{"text": {"content": username[:100]}}]
            },
            "태그": {
                "rich_text": [{"text": {"content": str(user_id)}}]
            },
            "날짜": {
                "date": {"start": datetime.now(timezone.utc).isoformat()}
            },
        },
    )


# ---------------------------------------------------------------------------
# 텔레그램 핸들러
# ---------------------------------------------------------------------------
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """일반 텍스트 메시지를 처리합니다."""
    user = update.effective_user
    user_message = update.message.text

    # 타이핑 표시
    await context.bot.send_chat_action(
        chat_id=update.effective_chat.id, action="typing"
    )

    # Claude 답변 요청
    try:
        claude_response = await get_claude_response(user_message)
    except anthropic.APIStatusError as exc:
        await update.message.reply_text(f"⚠️ Claude API 오류 ({exc.status_code}): {exc.message}")
        return
    except anthropic.APIConnectionError:
        await update.message.reply_text("⚠️ Claude API 연결에 실패했습니다. 잠시 후 다시 시도해주세요.")
        return
    except Exception as exc:
        await update.message.reply_text(f"⚠️ 예기치 않은 오류: {exc}")
        return

    # 답변 전송 (4096자 초과 시 분할)
    for chunk in chunk_text(claude_response, TELEGRAM_LIMIT):
        await update.message.reply_text(chunk)

    # Notion 저장
    try:
        await save_to_notion(
            user_id=user.id,
            username=user.username or user.first_name or "Unknown",
            user_message=user_message,
            claude_response=claude_response,
        )
        logging.info("[Notion] 저장 성공")
    except Exception as exc:
        logging.error(f"[Notion] 저장 실패: {type(exc).__name__}: {exc}")


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/start 명령어."""
    await update.message.reply_text(
        "👋 안녕하세요! Claude AI 봇입니다.\n\n"
        "메시지를 보내주시면 Claude가 분석하고 답장해드립니다.\n"
        "모든 대화는 Notion 데이터베이스에 자동으로 저장됩니다.\n\n"
        "도움말은 /help 를 입력하세요."
    )


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/help 명령어."""
    await update.message.reply_text(
        "📌 *사용법*\n"
        "텍스트를 입력하면 Claude AI가 분석하고 답변합니다.\n"
        "대화 내용은 Notion DB에 자동 저장됩니다.\n\n"
        "📝 *명령어*\n"
        "/start \\- 봇 시작\n"
        "/help \\- 도움말",
        parse_mode="MarkdownV2",
    )


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------
def main() -> None:
    from report_bot import report_command

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    # 한글 명령어: CommandHandler는 영문만 지원하므로 Regex 필터로 처리
    # COMMAND 필터를 포함해야 /로 시작하는 메시지도 잡힘
    app.add_handler(MessageHandler(filters.Regex(r"^/보고서"), report_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("봇 시작 중... Ctrl+C로 종료합니다.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
