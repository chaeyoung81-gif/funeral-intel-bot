"""
보고서 봇 모듈
--------------
/보고서 명령어로 대화 DB + 브리핑 DB 분석 → 4섹션 보고서 생성
텔레그램 전송 + 노션 저장 + PDF 파일 저장 및 전송

환경 변수:
  ANTHROPIC_API_KEY       - Anthropic API 키
  NOTION_API_KEY          - Notion Integration 토큰
  NOTION_DATABASE_ID      - 대화 DB
  NOTION_BRIEFING_DB_ID   - 브리핑 DB (보고서도 여기 저장)
  TELEGRAM_CHAT_ID        - 브로드캐스트 채팅 ID
"""

import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo

import anthropic
import httpx
from dotenv import load_dotenv
from notion_client import AsyncClient as NotionAsyncClient
from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import ContextTypes

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
REPORT_STATE_FILE = BASE_DIR / "report_state.json"
REPORTS_DIR = BASE_DIR / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
NOTION_API_KEY = os.getenv("NOTION_API_KEY", "")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "")          # 대화 DB
NOTION_BRIEFING_DB_ID = os.getenv("NOTION_BRIEFING_DB_ID", "")    # 브리핑 DB

KST = ZoneInfo("Asia/Seoul")


# ---------------------------------------------------------------------------
# 상태 관리: 마지막 보고서 날짜
# ---------------------------------------------------------------------------
def load_last_report_date() -> datetime:
    """마지막 보고서 날짜 로드. 없으면 30일 전."""
    try:
        if REPORT_STATE_FILE.exists():
            data = json.loads(REPORT_STATE_FILE.read_text(encoding="utf-8"))
            dt_str = data.get("last_report_date", "")
            if dt_str:
                return datetime.fromisoformat(dt_str)
    except Exception:
        pass
    return datetime.now(tz=timezone.utc) - timedelta(days=30)


def save_last_report_date(dt: datetime) -> None:
    """마지막 보고서 날짜 저장."""
    REPORT_STATE_FILE.write_text(
        json.dumps({"last_report_date": dt.isoformat()}, ensure_ascii=False),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# Notion 데이터 수집
# ---------------------------------------------------------------------------
def _extract_title(prop: dict) -> str:
    try:
        parts = prop.get("title", [])
        return "".join(p.get("plain_text", "") for p in parts)
    except Exception:
        return ""


def _extract_rich_text(prop: dict) -> str:
    try:
        parts = prop.get("rich_text", [])
        return "".join(p.get("plain_text", "") for p in parts)
    except Exception:
        return ""


def _extract_date(prop: dict) -> str:
    try:
        d = prop.get("date", {})
        return d.get("start", "") if d else ""
    except Exception:
        return ""


def _extract_multi_select(prop: dict) -> list[str]:
    try:
        return [o.get("name", "") for o in prop.get("multi_select", [])]
    except Exception:
        return []


def _page_to_text(page: dict, is_briefing: bool) -> str:
    """Notion 페이지를 분석용 텍스트로 변환."""
    props = page.get("properties", {})
    lines = []

    if is_briefing:
        # 브리핑 DB 실제 속성: 이름(title), 날짜(date), 내용, 분석결과, 태그, 출처, 링크
        title = _extract_title(props.get("이름", {}))
        date_str = _extract_date(props.get("날짜", {}))
        summary = _extract_rich_text(props.get("분석결과", {}))
        content = _extract_rich_text(props.get("내용", {}))
        # 태그는 select 또는 multi_select 모두 처리
        tag_prop = props.get("태그", {})
        if tag_prop.get("type") == "multi_select":
            tags = _extract_multi_select(tag_prop)
        elif tag_prop.get("type") == "select":
            sel = tag_prop.get("select") or {}
            tags = [sel.get("name", "")] if sel else []
        else:
            tags = []

        if date_str:
            lines.append(f"[브리핑 {date_str}]")
        if title:
            lines.append(f"제목: {title}")
        if tags:
            lines.append(f"태그: {', '.join(t for t in tags if t)}")
        if summary:
            lines.append(f"요약: {summary[:500]}")
        if content:
            lines.append(f"기사 목록: {content[:400]}")
    else:
        # 대화 DB 실제 속성: 이름(title), 날짜(date), 분석결과, 출처, 원문, 태그
        title = _extract_title(props.get("이름", {}))
        date_str = _extract_date(props.get("날짜", {}))
        analysis = _extract_rich_text(props.get("분석결과", {}))
        original = _extract_rich_text(props.get("원문", {}))

        if date_str:
            lines.append(f"[대화 {date_str}]")
        if title:
            lines.append(f"질문: {title[:300]}")
        if analysis:
            lines.append(f"분석: {analysis[:300]}")
        elif original:
            lines.append(f"내용: {original[:300]}")

    return "\n".join(lines) if lines else ""


async def fetch_notion_pages(db_id: str, since: datetime) -> list[dict]:
    """Notion DB에서 since 이후의 페이지를 모두 가져옴.

    notion-client v3이 databases.query를 제거했으므로 httpx로 직접 호출.
    """
    since_str = since.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    pages: list[dict] = []
    cursor = None

    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            body: dict = {
                "page_size": 100,
                "filter": {
                    "property": "날짜",
                    "date": {"on_or_after": since_str},
                },
            }
            if cursor:
                body["start_cursor"] = cursor

            try:
                resp = await client.post(url, headers=headers, json=body)
                data = resp.json()
            except Exception as e:
                logger.error(f"Notion HTTP error (db={db_id}): {e}")
                break

            if "results" not in data:
                logger.warning(f"Notion query failed (db={db_id}): {data.get('message', data)}")
                # 날짜 필터 없이 전체 조회 후 클라이언트 필터링
                try:
                    body2: dict = {"page_size": 100}
                    if cursor:
                        body2["start_cursor"] = cursor
                    resp2 = await client.post(url, headers=headers, json=body2)
                    data = resp2.json()
                    if "results" not in data:
                        logger.error(f"Notion fallback also failed: {data.get('message')}")
                        break
                    # 날짜 수동 필터
                    for p in data["results"]:
                        date_val = (
                            p.get("properties", {}).get("날짜", {}).get("date", {}) or {}
                        ).get("start", "")
                        if date_val and date_val >= since_str[:10]:
                            pages.append(p)
                except Exception as e2:
                    logger.error(f"Notion fallback error: {e2}")
                    break
            else:
                pages.extend(data["results"])

            if data.get("has_more") and data.get("next_cursor"):
                cursor = data["next_cursor"]
            else:
                break

    logger.info(f"Notion fetch: db={db_id[-8:]} → {len(pages)}건")
    return pages


# ---------------------------------------------------------------------------
# 보고서 생성 (Claude claude-opus-4-6)
# ---------------------------------------------------------------------------
async def generate_report_text(
    conv_pages: list[dict],
    briefing_pages: list[dict],
    since: datetime,
    until: datetime,
) -> str:
    """Claude claude-opus-4-6으로 4섹션 보고서 생성."""
    client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)

    since_str = since.astimezone(KST).strftime("%Y년 %m월 %d일")
    until_str = until.astimezone(KST).strftime("%Y년 %m월 %d일")

    # 대화 데이터 요약
    conv_texts = []
    for p in conv_pages:
        t = _page_to_text(p, is_briefing=False)
        if t:
            conv_texts.append(t)

    # 브리핑 데이터 요약
    briefing_texts = []
    for p in briefing_pages:
        t = _page_to_text(p, is_briefing=True)
        if t:
            briefing_texts.append(t)

    conv_block = "\n\n".join(conv_texts) if conv_texts else "없음"
    briefing_block = "\n\n".join(briefing_texts) if briefing_texts else "없음"

    # 토큰 절약: 각 블록 최대 15000자
    if len(conv_block) > 15000:
        conv_block = conv_block[:15000] + "\n...(이하 생략)"
    if len(briefing_block) > 15000:
        briefing_block = briefing_block[:15000] + "\n...(이하 생략)"

    prompt = f"""당신은 장례업 전문 비즈니스 애널리스트입니다.
아래는 {since_str} ~ {until_str} 기간 동안 수집된 데이터입니다.

=== 사용자 대화 기록 ({len(conv_pages)}건) ===
{conv_block}

=== 뉴스/브리핑 기록 ({len(briefing_pages)}건) ===
{briefing_block}

위 데이터를 분석하여 장례업 사업 관점의 주간 보고서를 작성하세요.
반드시 아래 XML 형식으로 출력하세요.

<REPORT>
## 📊 핵심 트렌드
(이 기간 가장 중요한 트렌드 3~5가지. 각 트렌드마다 구체적 근거 포함)

## 🔑 반복 등장 키워드
(자주 등장한 키워드 10개 내외. 각 키워드에 간략한 맥락 설명)

## 💡 사업 기회 포인트
(장례업 사업자 관점에서 주목할 기회 3~5가지. 구체적 행동 제안 포함)

## 👀 다음 주 주목할 것
(다음 주에 모니터링할 이슈/지표/일정 3~5가지)
</REPORT>"""

    try:
        response = await client.messages.create(
            model="claude-opus-4-6",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text
        match = re.search(r"<REPORT>(.*?)</REPORT>", raw, re.DOTALL)
        return match.group(1).strip() if match else raw.strip()
    except Exception as e:
        logger.error(f"Claude API error: {e}")
        raise


# ---------------------------------------------------------------------------
# PDF 생성
# ---------------------------------------------------------------------------
def generate_pdf(report_text: str, period_str: str) -> Path:
    """reportlab으로 PDF 생성. Malgun Gothic 사용."""
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.units import mm
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer
    except ImportError:
        raise RuntimeError("reportlab이 설치되지 않았습니다. pip install reportlab")

    # 한글 폰트 등록
    font_paths = [
        Path("C:/Windows/Fonts/malgun.ttf"),
        Path("C:/Windows/Fonts/gulim.ttc"),
        Path("C:/Windows/Fonts/batang.ttc"),
    ]
    font_name = "NotoSans"
    for fp in font_paths:
        if fp.exists():
            try:
                pdfmetrics.registerFont(TTFont(font_name, str(fp)))
                break
            except Exception:
                continue
    else:
        font_name = "Helvetica"

    filename = REPORTS_DIR / f"report_{datetime.now(KST).strftime('%Y%m%d_%H%M%S')}.pdf"

    doc = SimpleDocTemplate(
        str(filename),
        pagesize=A4,
        rightMargin=20 * mm,
        leftMargin=20 * mm,
        topMargin=20 * mm,
        bottomMargin=20 * mm,
    )

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "KorTitle",
        fontName=font_name,
        fontSize=16,
        leading=22,
        spaceAfter=6,
    )
    body_style = ParagraphStyle(
        "KorBody",
        fontName=font_name,
        fontSize=10,
        leading=16,
        spaceAfter=4,
    )
    h2_style = ParagraphStyle(
        "KorH2",
        fontName=font_name,
        fontSize=12,
        leading=18,
        spaceBefore=10,
        spaceAfter=4,
    )

    story = []
    story.append(Paragraph(f"장례업 사업 동향 보고서", title_style))
    story.append(Paragraph(f"분석 기간: {period_str}", body_style))
    story.append(Paragraph(
        f"생성 일시: {datetime.now(KST).strftime('%Y년 %m월 %d일 %H:%M')} KST",
        body_style,
    ))
    story.append(Spacer(1, 10 * mm))

    for line in report_text.split("\n"):
        line = line.strip()
        if not line:
            story.append(Spacer(1, 3 * mm))
            continue
        # 이모지 제거 (reportlab 미지원)
        clean = re.sub(r"[^\x00-\x7F\uAC00-\uD7A3\u3131-\u318E\u0020-\u007E]", "", line)
        if line.startswith("## "):
            story.append(Paragraph(clean.lstrip("# ").strip(), h2_style))
        else:
            story.append(Paragraph(clean, body_style))

    doc.build(story)
    return filename


# ---------------------------------------------------------------------------
# Notion 저장
# ---------------------------------------------------------------------------
async def save_report_to_notion(
    report_text: str,
    period_str: str,
    conv_count: int,
    briefing_count: int,
) -> None:
    """보고서를 브리핑 DB에 저장."""
    notion = NotionAsyncClient(auth=NOTION_API_KEY)
    now_kst = datetime.now(KST)
    title = f"[보고서] {now_kst.strftime('%Y-%m-%d')} | {period_str}"

    # Notion rich_text 한 블록 최대 2000자
    LIMIT = 1900
    chunks = [report_text[i : i + LIMIT] for i in range(0, len(report_text), LIMIT)]
    rich_text_blocks = [{"text": {"content": c}} for c in chunks[:10]]  # 최대 10블록

    props: dict = {
        "이름": {"title": [{"text": {"content": title}}]},
        "날짜": {"date": {"start": now_kst.strftime("%Y-%m-%d")}},
    }

    # 분석결과 컬럼 시도
    summary_short = report_text[:1900]
    optional_props = {
        "분석결과": {"rich_text": [{"text": {"content": summary_short}}]},
        "내용": {
            "rich_text": rich_text_blocks
            if rich_text_blocks
            else [{"text": {"content": f"대화 {conv_count}건, 브리핑 {briefing_count}건 분석"}}]
        },
    }

    try:
        await notion.pages.create(
            parent={"database_id": NOTION_BRIEFING_DB_ID},
            properties={**props, **optional_props},
        )
        logger.info("보고서 Notion 저장 완료")
    except Exception as e:
        err_str = str(e)
        logger.warning(f"Notion 저장 실패 (optional 제거 재시도): {e}")
        # 실패한 컬럼 제거 후 재시도
        retry_props = dict(props)
        for key in optional_props:
            if key not in err_str:
                retry_props[key] = optional_props[key]
        try:
            await notion.pages.create(
                parent={"database_id": NOTION_BRIEFING_DB_ID},
                properties=retry_props,
            )
            logger.info("보고서 Notion 저장 완료 (축소)")
        except Exception as e2:
            logger.error(f"Notion 저장 최종 실패: {e2}")


# ---------------------------------------------------------------------------
# 텔레그램 핸들러
# ---------------------------------------------------------------------------
async def report_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/보고서 명령어 처리."""
    chat_id = update.effective_chat.id
    msg = update.message

    await msg.reply_text("📊 보고서 생성을 시작합니다. 잠시만 기다려주세요...")

    since = load_last_report_date()
    until = datetime.now(tz=timezone.utc)

    since_kst = since.astimezone(KST)
    until_kst = until.astimezone(KST)
    period_str = f"{since_kst.strftime('%Y.%m.%d')} ~ {until_kst.strftime('%Y.%m.%d')}"

    await msg.reply_text(f"📅 분석 기간: {period_str}\n데이터 수집 중...")

    # Notion 데이터 수집
    try:
        conv_pages = await fetch_notion_pages(NOTION_DATABASE_ID, since)
        briefing_pages = await fetch_notion_pages(NOTION_BRIEFING_DB_ID, since)
        # 보고서 페이지 제외
        briefing_pages = [
            p for p in briefing_pages
            if not any(
                o.get("name") == "보고서"
                for o in p.get("properties", {})
                .get("태그", {})
                .get("multi_select", [])
            )
        ]
    except Exception as e:
        await msg.reply_text(f"❌ 데이터 수집 실패: {e}")
        return

    await msg.reply_text(
        f"✅ 데이터 수집 완료\n"
        f"• 대화: {len(conv_pages)}건\n"
        f"• 브리핑: {len(briefing_pages)}건\n\n"
        f"Claude claude-opus-4-6으로 분석 중..."
    )

    if len(conv_pages) == 0 and len(briefing_pages) == 0:
        await msg.reply_text("⚠️ 분석할 데이터가 없습니다. 기간을 확인해주세요.")
        return

    # 보고서 생성
    try:
        report_text = await generate_report_text(conv_pages, briefing_pages, since, until)
    except Exception as e:
        await msg.reply_text(f"❌ 보고서 생성 실패: {e}")
        return

    # 텔레그램 전송 (4096자 초과 시 분할)
    header = f"📋 *장례업 사업 동향 보고서*\n📅 {period_str}\n\n"
    full_text = header + report_text

    MAX_LEN = 4000
    chunks = []
    remaining = full_text
    while remaining:
        if len(remaining) <= MAX_LEN:
            chunks.append(remaining)
            break
        split_at = remaining.rfind("\n", 0, MAX_LEN)
        if split_at == -1:
            split_at = MAX_LEN
        chunks.append(remaining[:split_at])
        remaining = remaining[split_at:].lstrip("\n")

    for i, chunk in enumerate(chunks):
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=chunk,
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception:
            # Markdown 파싱 실패 시 plain text로 재전송
            try:
                await context.bot.send_message(chat_id=chat_id, text=chunk)
            except Exception as e:
                logger.error(f"텔레그램 전송 실패 (chunk {i}): {e}")

    # PDF 생성 및 전송
    await msg.reply_text("📄 PDF 생성 중...")
    try:
        pdf_path = generate_pdf(report_text, period_str)
        pdf_bytes = pdf_path.read_bytes()
        await context.bot.send_document(
            chat_id=chat_id,
            document=pdf_bytes,
            filename=pdf_path.name,
            caption=f"보고서 PDF ({period_str})",
        )
        logger.info(f"PDF 저장: {pdf_path}")
    except Exception as e:
        await msg.reply_text(f"⚠️ PDF 생성 실패: {e}\n(보고서 텍스트는 위에 전송됨)")

    # Notion 저장
    try:
        await save_report_to_notion(report_text, period_str, len(conv_pages), len(briefing_pages))
        await msg.reply_text("✅ Notion에 보고서 저장 완료!")
    except Exception as e:
        await msg.reply_text(f"⚠️ Notion 저장 실패: {e}")

    # 마지막 보고서 날짜 갱신
    save_last_report_date(until)
    logger.info(f"보고서 완료. 다음 보고서 기준일: {until.isoformat()}")
