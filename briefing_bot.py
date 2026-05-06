"""
브리핑 봇 v3 - 3분할 스케줄 + 주간 스코어보드
--------------------------------------
08:00 KST — 국내 브리핑 (RSS + 업계 소스)
18:00 KST — 아시아 브리핑 (일본·중국·대만·홍콩·싱가포르)
         → 수집 0건이면 미전송
일요일 20:00 KST — 주간 아이디어 다이제스트 (Notion 대화 기반)
월요일 08:05 KST — 주간 스코어보드 (지난주 수집 통계 + 체크인 연속 일수)

실행:
    python briefing_bot.py            # 스케줄 모드
    python briefing_bot.py --now      # 국내 브리핑 즉시 1회
    python briefing_bot.py --asia     # 아시아 브리핑 즉시 1회
    python briefing_bot.py --digest   # 주간 다이제스트 즉시 1회
    python briefing_bot.py --board    # 주간 스코어보드 즉시 1회
"""

import asyncio
import json
import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, quote

import anthropic
import feedparser
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from notion_client import AsyncClient as NotionAsyncClient
from telegram import Bot
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from zoneinfo import ZoneInfo

load_dotenv()

# ---------------------------------------------------------------------------
# 로깅
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("briefing.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 설정
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
SEEN_FILE = BASE_DIR / "seen_items.json"
WEEKLY_STATS_FILE = BASE_DIR / "weekly_stats.json"
CHECKIN_STATE_FILE = BASE_DIR / "checkin_state.json"
SEEN_MAX_DAYS = 30

TELEGRAM_BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID: str = os.environ["TELEGRAM_CHAT_ID"]
ANTHROPIC_API_KEY: str = os.environ["ANTHROPIC_API_KEY"]
NOTION_API_KEY: str = os.environ["NOTION_API_KEY"]
NOTION_BRIEFING_DB_ID: str = os.environ["NOTION_BRIEFING_DB_ID"]

TELEGRAM_LIMIT = 4096
NOTION_LIMIT = 1900
MODEL = "claude-sonnet-4-6"

KST = ZoneInfo("Asia/Seoul")

# ---------------------------------------------------------------------------
# 키워드 필터
# ---------------------------------------------------------------------------
NEWS_KEYWORDS = [
    "장례", "고령화", "초고령화", "인구", "사망", "시니어", "상속",
    "1인가구", "AI 비즈니스", "웰다잉", "호스피스", "납골", "화장",
    "고독사", "무연고", "장사시설", "봉안", "추모", "연명의료",
    "장례지도사", "상조", "요양", "돌봄", "간병",
]

BOK_KEYWORDS = [
    "장례", "고령화", "초고령화", "인구", "사망", "시니어", "상속",
    "1인가구", "경제전망", "지역경제", "소비", "인구구조",
]

# 구글 뉴스 검색 쿼리 그룹 — 주제별로 나눠서 노이즈 줄임
DOMESTIC_NEWS_QUERIES: list[tuple[str, str]] = [
    ("장례·웰다잉", "장례 OR 납골 OR 화장장 OR 호스피스 OR 웰다잉 OR 봉안 OR 연명의료 OR 장례지도사 OR 추모"),
    ("고령화·인구", "고령화 OR 초고령화 OR 1인가구 OR 고독사 OR 무연고 OR 인구절벽 OR 시니어"),
    ("상조·돌봄", "상조 OR 요양 OR 돌봄 OR 간병 OR 노인복지"),
    ("상속·법제도", "상속 OR 유언 OR 사전장례 OR 상속세"),
    ("사망 통계", "사망자수 OR 사망률 OR 인구동향"),
]

# ---------------------------------------------------------------------------
# 클라이언트
# ---------------------------------------------------------------------------
claude = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
notion = NotionAsyncClient(auth=NOTION_API_KEY)
tg_bot = Bot(token=TELEGRAM_BOT_TOKEN)

HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
}

# ---------------------------------------------------------------------------
# 유틸
# ---------------------------------------------------------------------------
def chunk_text(text: str, limit: int) -> list[str]:
    if len(text) <= limit:
        return [text]
    return [text[i : i + limit] for i in range(0, len(text), limit)]


def build_notion_rich_text(text: str) -> list[dict]:
    return [{"text": {"content": c}} for c in chunk_text(text, NOTION_LIMIT)]


def is_recent(published_str: str, hours: int = 26) -> bool:
    if not published_str:
        return True
    import email.utils
    try:
        dt = email.utils.parsedate_to_datetime(published_str)
        diff = datetime.now(timezone.utc) - dt.astimezone(timezone.utc)
        return diff.total_seconds() < hours * 3600
    except Exception:
        return True


async def claude_with_retry(prompt: str, system_prompt: str, max_tokens: int = 3500, retries: int = 3) -> str:
    """Claude API 호출 + 지수 백오프 재시도."""
    for attempt in range(retries):
        try:
            response = await claude.messages.create(
                model=MODEL,
                max_tokens=max_tokens,
                system=[{"type": "text", "text": system_prompt, "cache_control": {"type": "ephemeral"}}],
                messages=[{"role": "user", "content": prompt}],
            )
            return next((b.text for b in response.content if b.type == "text"), "")
        except Exception as e:
            wait = 2 ** attempt * 5
            log.warning(f"[Claude] 시도 {attempt+1}/{retries} 실패: {e}. {wait}초 대기...")
            if attempt < retries - 1:
                await asyncio.sleep(wait)
            else:
                raise


# ---------------------------------------------------------------------------
# 태그 정의 및 자동 추출
# ---------------------------------------------------------------------------
TAG_RULES: list[tuple[str, list[str]]] = [
    ("장례업 직결", ["장례", "납골", "화장", "호스피스", "웰다잉", "장례식장", "빈소", "추모", "장례지도", "상조", "봉안"]),
    ("고령화", ["고령화", "초고령화", "고령", "노인", "시니어", "노령"]),
    ("인구·1인가구", ["인구", "1인가구", "저출산", "출산율", "인구구조", "인구감소", "사망자수"]),
    ("상속·법제도", ["상속", "유언", "법", "제도", "정책", "규제", "세금", "세제"]),
    ("경제동향", ["경제", "금리", "물가", "소비", "GDP", "성장", "경기", "지역경제", "경제전망"]),
    ("AI·디지털", ["AI", "인공지능", "디지털", "플랫폼", "테크", "앱", "스타트업"]),
    ("해외동향", ["일본", "해외", "글로벌", "foreign", "overseas", "Japan", "funeral industry", "中国", "台灣", "香港"]),
    ("통계·조사", ["통계", "조사", "분석", "보고서", "데이터"]),
    ("사망·보건", ["사망", "사망률", "보건", "의료", "병원", "질병", "고독사"]),
]


def extract_tags(data: dict[str, list[dict]], keywords: str) -> list[str]:
    text_pool = keywords + " "
    for category, items in data.items():
        for item in items:
            text_pool += f" {item.get('title', '')} {item.get('summary', '')}"
    tags: list[str] = []
    for tag, kw_list in TAG_RULES:
        if any(kw.lower() in text_pool.lower() for kw in kw_list):
            tags.append(tag)
    return tags[:10]


# ---------------------------------------------------------------------------
# 중복 수집 방지
# ---------------------------------------------------------------------------
def _item_key(item: dict) -> str:
    return (item.get("link") or item.get("title") or "").strip()


def load_seen() -> set[str]:
    if not SEEN_FILE.exists():
        return set()
    try:
        data: dict[str, str] = json.loads(SEEN_FILE.read_text(encoding="utf-8"))
        cutoff = (datetime.now(timezone.utc) - timedelta(days=SEEN_MAX_DAYS)).isoformat()
        return {k for k, ts in data.items() if ts >= cutoff}
    except Exception as e:
        log.warning(f"[seen] 로드 실패: {e}")
        return set()


def save_seen(new_keys: set[str]) -> None:
    existing: dict[str, str] = {}
    if SEEN_FILE.exists():
        try:
            existing = json.loads(SEEN_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    now = datetime.now(timezone.utc).isoformat()
    existing.update({k: now for k in new_keys if k})
    cutoff = (datetime.now(timezone.utc) - timedelta(days=SEEN_MAX_DAYS)).isoformat()
    existing = {k: v for k, v in existing.items() if v >= cutoff}
    SEEN_FILE.write_text(json.dumps(existing, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(f"[seen] {len(new_keys)}개 신규 기록, 누적 {len(existing)}개")


def dedup(items: list[dict], seen: set[str]) -> tuple[list[dict], set[str]]:
    new_items, new_keys = [], set()
    for item in items:
        key = _item_key(item)
        if key and key not in seen:
            new_items.append(item)
            new_keys.add(key)
    skipped = len(items) - len(new_items)
    if skipped:
        log.info(f"  → 중복 {skipped}건 제외")
    return new_items, new_keys


# ---------------------------------------------------------------------------
# 주간 통계 관리 (스코어보드용)
# ---------------------------------------------------------------------------
def load_weekly_stats() -> dict:
    """주간 통계 로드. 파일 없으면 새로 생성."""
    if WEEKLY_STATS_FILE.exists():
        try:
            return json.loads(WEEKLY_STATS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "week_start": datetime.now(KST).strftime("%Y-%m-%d"),
        "domestic_items": 0,
        "asia_items": 0,
        "briefing_days": 0,
    }


def update_weekly_stats(domestic: int = 0, asia: int = 0) -> None:
    """주간 통계 업데이트. 브리핑 발송 후 호출."""
    stats = load_weekly_stats()
    stats["domestic_items"] += domestic
    stats["asia_items"] += asia
    stats["briefing_days"] += 1
    WEEKLY_STATS_FILE.write_text(
        json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def reset_weekly_stats() -> None:
    """새 주 시작 시 호출. 통계 초기화."""
    stats = {
        "week_start": datetime.now(KST).strftime("%Y-%m-%d"),
        "domestic_items": 0,
        "asia_items": 0,
        "briefing_days": 0,
    }
    WEEKLY_STATS_FILE.write_text(
        json.dumps(stats, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def load_checkin() -> dict:
    """체크인 상태 파일 읽기 (checkin_bot.py가 관리, 여기서는 read-only)."""
    if not CHECKIN_STATE_FILE.exists():
        return {}
    try:
        return json.loads(CHECKIN_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# 수집기 1: RSS
# ---------------------------------------------------------------------------
def collect_rss(
    url: str,
    source: str,
    keywords: Optional[list[str]] = None,
    max_items: int = 10,
) -> list[dict]:
    try:
        feed = feedparser.parse(url, request_headers=HTTP_HEADERS)
        if feed.bozo and not feed.entries:
            log.warning(f"[RSS:{source}] 파싱 경고: {feed.bozo_exception}")
        items = []
        for entry in feed.entries:
            title = getattr(entry, "title", "").strip()
            summary_raw = getattr(entry, "summary", "") or getattr(entry, "description", "")
            summary = BeautifulSoup(summary_raw, "html.parser").get_text()[:400].strip()
            link = getattr(entry, "link", "")
            published = getattr(entry, "published", "")
            if not is_recent(published):
                continue
            if keywords and not any(kw in f"{title} {summary}" for kw in keywords):
                continue
            items.append({"source": source, "title": title, "summary": summary, "link": link, "published": published})
            if len(items) >= max_items:
                break
        return items
    except Exception as e:
        log.error(f"[RSS:{source}] 실패: {e}")
        return []


# ---------------------------------------------------------------------------
# 수집기 2: 게시판 크롤링 (bbs/list 패턴만 허용)
# ---------------------------------------------------------------------------
def crawl_board(url: str, source: str, max_items: int = 10) -> list[dict]:
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        board_patterns = ["bbs", "board", "view", "detail", "notice", "press", "news"]

        candidate_selectors = [
            "td.subject a", "td.title a", ".board-list td a",
            ".list-title a", ".bbs-list td a", "table.bbs_list td a",
            "ul.board_list li a", ".notice-list a",
            ".board_list a", ".tb_list a", "table tbody tr td a",
        ]

        found = []
        for sel in candidate_selectors:
            found = soup.select(sel)
            if found:
                break

        if not found:
            found = [a for a in soup.find_all("a") if len(a.get_text(strip=True)) > 10]

        items, seen_titles = [], set()
        for tag in found:
            title = tag.get_text(strip=True)[:200]
            if not title or title in seen_titles:
                continue

            href = tag.get("href", "")
            if href and not href.startswith("http"):
                href = urljoin(url, href)

            if "contents_view" in href or "contents/contents" in href:
                continue
            if href and not any(p in href.lower() for p in board_patterns):
                continue

            seen_titles.add(title)
            items.append({"source": source, "title": title, "summary": "", "link": href or url, "published": ""})
            if len(items) >= max_items:
                break

        log.info(f"[CRAWL:{source}] {len(items)}건 수집")
        return items
    except Exception as e:
        log.error(f"[CRAWL:{source}] 실패: {e}")
        return []


# ---------------------------------------------------------------------------
# 수집기 3: 아시아 장례·고령화 뉴스
# ---------------------------------------------------------------------------
def collect_asia() -> list[dict]:
    """일본·중국·대만·홍콩·싱가포르의 장례·고령화 관련 뉴스 수집."""
    items: list[dict] = []

    # --- 일본 ---
    jp_keywords = ["葬", "死亡", "高齢", "超高齢", "人口", "相続", "終活", "墓", "火葬"]
    for mhlw_url in ["https://www.mhlw.go.jp/index.xml", "https://www.mhlw.go.jp/rss/news.xml"]:
        try:
            feed = feedparser.parse(mhlw_url, request_headers=HTTP_HEADERS)
            for entry in feed.entries[:30]:
                title = getattr(entry, "title", "")
                summary = getattr(entry, "summary", "")
                if any(kw in f"{title}{summary}" for kw in jp_keywords):
                    items.append({"source": "일본 후생노동성", "title": title, "summary": summary[:300],
                                  "link": getattr(entry, "link", ""), "published": getattr(entry, "published", "")})
            if items:
                break
        except Exception as e:
            log.warning(f"[아시아] 후생노동성 실패: {e}")

    jp_queries = ["葬儀 高齢化 2026", "終活 日本", "火葬場 不足"]
    for query in jp_queries:
        try:
            url = f"https://news.google.com/rss/search?q={quote(query)}&hl=ja&gl=JP&ceid=JP:ja"
            feed = feedparser.parse(url, request_headers=HTTP_HEADERS)
            for entry in feed.entries[:3]:
                raw = getattr(entry, "summary", "")
                items.append({
                    "source": "일본 뉴스",
                    "title": getattr(entry, "title", ""),
                    "summary": BeautifulSoup(raw, "html.parser").get_text()[:300],
                    "link": getattr(entry, "link", ""),
                    "published": getattr(entry, "published", ""),
                })
        except Exception as e:
            log.warning(f"[아시아] 일본 검색 실패 ({query}): {e}")

    cn_queries = ["殡葬改革 2026", "中国 老龄化 人口", "中国 丧葬 养老"]
    for query in cn_queries:
        try:
            url = f"https://news.google.com/rss/search?q={quote(query)}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
            feed = feedparser.parse(url, request_headers=HTTP_HEADERS)
            for entry in feed.entries[:3]:
                raw = getattr(entry, "summary", "")
                items.append({
                    "source": "중국 뉴스",
                    "title": getattr(entry, "title", ""),
                    "summary": BeautifulSoup(raw, "html.parser").get_text()[:300],
                    "link": getattr(entry, "link", ""),
                    "published": getattr(entry, "published", ""),
                })
        except Exception as e:
            log.warning(f"[아시아] 중국 검색 실패 ({query}): {e}")

    tw_queries = ["台灣 殯葬 高齡化", "Taiwan funeral aging"]
    for query in tw_queries:
        try:
            hl, gl, ceid = ("zh-TW", "TW", "TW:zh-Hant") if "台" in query else ("en", "US", "US:en")
            url = f"https://news.google.com/rss/search?q={quote(query)}&hl={hl}&gl={gl}&ceid={ceid}"
            feed = feedparser.parse(url, request_headers=HTTP_HEADERS)
            for entry in feed.entries[:3]:
                raw = getattr(entry, "summary", "")
                items.append({
                    "source": "대만 뉴스",
                    "title": getattr(entry, "title", ""),
                    "summary": BeautifulSoup(raw, "html.parser").get_text()[:300],
                    "link": getattr(entry, "link", ""),
                    "published": getattr(entry, "published", ""),
                })
        except Exception as e:
            log.warning(f"[아시아] 대만 검색 실패: {e}")

    hk_queries = ["Hong Kong funeral columbarium", "香港 殯葬 骨灰 龕位"]
    for query in hk_queries:
        try:
            hl, gl, ceid = ("zh-HK", "HK", "HK:zh-Hant") if "香" in query else ("en", "HK", "HK:en")
            url = f"https://news.google.com/rss/search?q={quote(query)}&hl={hl}&gl={gl}&ceid={ceid}"
            feed = feedparser.parse(url, request_headers=HTTP_HEADERS)
            for entry in feed.entries[:3]:
                raw = getattr(entry, "summary", "")
                items.append({
                    "source": "홍콩 뉴스",
                    "title": getattr(entry, "title", ""),
                    "summary": BeautifulSoup(raw, "html.parser").get_text()[:300],
                    "link": getattr(entry, "link", ""),
                    "published": getattr(entry, "published", ""),
                })
        except Exception as e:
            log.warning(f"[아시아] 홍콩 검색 실패: {e}")

    sg_queries = ["Singapore funeral aging population", "Singapore death care elderly"]
    for query in sg_queries:
        try:
            url = f"https://news.google.com/rss/search?q={quote(query)}&hl=en&gl=SG&ceid=SG:en"
            feed = feedparser.parse(url, request_headers=HTTP_HEADERS)
            for entry in feed.entries[:3]:
                raw = getattr(entry, "summary", "")
                items.append({
                    "source": "싱가포르 뉴스",
                    "title": getattr(entry, "title", ""),
                    "summary": BeautifulSoup(raw, "html.parser").get_text()[:300],
                    "link": getattr(entry, "link", ""),
                    "published": getattr(entry, "published", ""),
                })
        except Exception as e:
            log.warning(f"[아시아] 싱가포르 검색 실패: {e}")

    log.info(f"[아시아] 총 {len(items)}건 수집")
    return items[:30]


# ═══════════════════════════════════════════════════════════════════════════
#  시스템 프롬프트
# ═══════════════════════════════════════════════════════════════════════════

SYSTEM_DOMESTIC = """당신은 채영님의 장례사업 전문 브리핑 어시스턴트입니다.

채영님은 장례 '기술자'가 아니라, 장례 산업을 운용·플랫폼화하는 사업가입니다.
인구·사망 통계는 장례업과 직결되므로 특히 주목하세요.

분석 관점:
- 고령화·초고령화 트렌드가 장례 수요에 미치는 영향
- 1인가구 증가 → 고독사 → 장례 서비스 수요 변화
- 장례 관련 정책·규제·지원 변화 / 업계 동향
- 상속·사망 관련 법제도 변화
- AI·디지털 기술의 장례 서비스 적용 가능성

**중요한 브리핑 원칙:**
1. 보고서·통계·연구자료가 수집되면 단순 나열하지 말고, 어떤 내용인지 짐작되는 범위에서 소개하고
   "링크 타고 들어가서 확인 권장" 같은 안내를 덧붙이세요.
   예: "한국은행에서 '경제전망 심층연구' 보고서가 공표됨. 고령화와 소비 패턴 연관성 다룰 가능성. 링크 확인 권장."
2. 매일 억지로 시사점을 뽑지 마세요. 의미 있는 연결이 있을 때만 제시하고, 없으면 과감히 생략.
3. 출처별로 나누지 말고 '주제별로 묶어서' 정리하세요 (예: 인구·사망 / 정책 / 업계 동향 / AI·기술 / 보고서).

응답은 반드시 아래 XML 태그로 구분하세요.

<BRIEFING>
━━━━━━━━━━━━━━━━━━━━━━
📋 장례사업 브리핑 [날짜]
━━━━━━━━━━━━━━━━━━━━━━

(주제별로 묶어서 제시. 주제 섹션은 해당 내용이 있을 때만 포함.)

📊 인구·사망 동향
• [출처] 제목
  → 인사이트 (1~2줄)

🏛 정책·법제도
• [출처] 제목
  → 인사이트

🕯 업계 동향
• [출처] 제목
  → 인사이트

📄 보고서·통계자료
• [출처] 제목
  → 어떤 내용인지 짐작되는 범위 + "링크 확인 권장"

🤖 AI·기술 동향
• [출처] 제목
  → 인사이트

수집 항목이 없으면: "오늘은 신규 수집 항목이 없습니다."

💡 오늘의 시사점
• 의미 있는 시사점이 있을 때만 1~3가지. 없으면 이 섹션 전체 생략.
</BRIEFING>

<KEYWORDS>
핵심 키워드 3~5개. 쉼표 구분.
</KEYWORDS>

<SUMMARY>
핵심 요약 3줄 이내.
</SUMMARY>"""


SYSTEM_ASIA = """당신은 채영님의 장례사업 전문 브리핑 어시스턴트입니다.

채영님은 장례 산업을 운용·플랫폼화하는 사업가입니다.
아시아 국가들의 장례·고령화·인구 동향을 모니터링합니다.

각 국가 관찰 포인트:
- 일본: 장례 트렌드 선행국. 한국이 3~5년 후 따라감.
- 홍콩: 땅 부족으로 장사시설 문제 극단적. 한국 도시화의 미래.
- 대만: 인구구조·문화적 유사성.
- 중국: 규모와 영향력. 한국 경제에 직간접 영향.
- 싱가포르: 도시국가의 고령화 대응 모델.

비한국어 기사는 한국어로 번역·요약하세요.

응답 형식:

<BRIEFING>
🌏 아시아 장례·고령화 브리핑 [날짜]

🇯🇵 일본
• [출처] 내용 + 한국 시사점

🇨🇳 중국
• [출처] 내용 + 한국 시사점

🇹🇼 대만
• [출처] 내용

🇭🇰 홍콩
• [출처] 내용 + 한국 시사점

🇸🇬 싱가포르
• [출처] 내용

수집 안 된 국가는 생략.

💡 아시아 동향 시사점
• 한국 장례 사업에 주는 함의 1~3가지. 의미 있을 때만.
</BRIEFING>

<KEYWORDS>
핵심 키워드 3~5개.
</KEYWORDS>

<SUMMARY>
핵심 요약 3줄 이내.
</SUMMARY>"""


# ═══════════════════════════════════════════════════════════════════════════
#  노션 저장
# ═══════════════════════════════════════════════════════════════════════════

async def setup_notion_db() -> None:
    try:
        await notion.databases.update(
            database_id=NOTION_BRIEFING_DB_ID,
            properties={
                "내용": {"rich_text": {}},
                "링크": {"rich_text": {}},
                "태그": {"multi_select": {}},
            },
        )
        log.info("[Notion] DB 컬럼 확인/추가 완료")
    except Exception as e:
        log.warning(f"[Notion] DB 컬럼 자동 추가 실패: {e}")


async def save_to_notion(
    date_str: str,
    keywords: str,
    briefing_summary: str,
    data: dict[str, list[dict]],
    tags: list[str],
    briefing_type: str = "국내",
) -> None:
    all_items = []
    for v in data.values():
        all_items.extend(v)

    title_list = "\n".join(f"• [{i['source']}] {i['title']}" for i in all_items)
    links_text = "\n".join(f"• {i.get('link', '')}" for i in all_items if i.get("link"))
    notion_title = f"[{briefing_type}] {date_str} | {keywords}" if keywords else f"[{briefing_type}] {date_str}"

    properties: dict = {
        "이름": {"title": [{"text": {"content": notion_title[:200]}}]},
        "분석결과": {"rich_text": build_notion_rich_text(briefing_summary)},
        "날짜": {"date": {"start": datetime.now(timezone.utc).isoformat()}},
    }

    optional: dict = {
        "내용": {"rich_text": build_notion_rich_text(title_list)},
        "링크": {"rich_text": build_notion_rich_text(links_text)},
        "태그": {"multi_select": [{"name": t} for t in tags]},
    }

    try:
        await notion.pages.create(
            parent={"database_id": NOTION_BRIEFING_DB_ID},
            properties={**properties, **optional},
        )
        return
    except Exception as e:
        err_msg = str(e)
        missing = [k for k in optional if k in err_msg]
        if not missing:
            raise
        log.warning(f"[Notion] 컬럼 없음 ({missing}) — 제외 후 재시도")
        for k in missing:
            optional.pop(k, None)
        await notion.pages.create(
            parent={"database_id": NOTION_BRIEFING_DB_ID},
            properties={**properties, **optional},
        )


# ═══════════════════════════════════════════════════════════════════════════
#  텔레그램 전송
# ═══════════════════════════════════════════════════════════════════════════

async def send_telegram(text: str) -> None:
    for chunk in chunk_text(text, TELEGRAM_LIMIT):
        await tg_bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=chunk)


# ═══════════════════════════════════════════════════════════════════════════
#  Claude 응답 파싱
# ═══════════════════════════════════════════════════════════════════════════

def _extract_tag(tag: str, text: str) -> str:
    match = re.search(f"<{tag}>(.*?)</{tag}>", text, re.DOTALL)
    return match.group(1).strip() if match else ""


# ═══════════════════════════════════════════════════════════════════════════
#  작업 1: 08:00 국내 브리핑
# ═══════════════════════════════════════════════════════════════════════════

async def collect_domestic() -> dict[str, list[dict]]:
    today = datetime.now(KST)
    loop = asyncio.get_running_loop()

    def run(fn, *args):
        return loop.run_in_executor(None, fn, *args)

    log.info("=== 국내 브리핑 데이터 수집 ===")
    seen = load_seen()
    log.info(f"[seen] 기존 기록 {len(seen)}개 로드")

    # 구글 뉴스 검색 (주제 그룹별)
    google_news_tasks = [
        run(collect_rss,
            f"https://news.google.com/rss/search?q={quote(query)}&hl=ko&gl=KR&ceid=KR:ko",
            f"구글뉴스·{label}", None, 10)
        for label, query in DOMESTIC_NEWS_QUERIES
    ]

    results = await asyncio.gather(
        *google_news_tasks,
        run(crawl_board, "https://www.kfcpi.or.kr/portal/home/bbs/list.do?menuId=M0001000600010000", "한국장례문화진흥원"),
        run(crawl_board, "https://www.kfcpi.or.kr/portal/home/bbs/list.do?menuId=M0001000700000000", "장례문화진흥원 보도자료"),
        run(crawl_board, "https://www.bok.or.kr/portal/bbs/P0000559/list.do?menuNo=200690", "한국은행"),
        run(crawl_board, "https://www.sangjomagazine.com", "상조매거진"),
        run(crawl_board, "https://kfda1024.or.kr/", "대한장례지도사협회"),
        run(crawl_board, "https://www.mohw.go.kr/board.es?mid=a10503010100&bid=0027", "보건복지부"),
        run(collect_rss,
            "https://news.google.com/rss/search?q=" + quote("통계청 OR 국가데이터처 인구동향 사망자수") + "&hl=ko&gl=KR&ceid=KR:ko",
            "통계청 발표", None, 5),
        run(collect_rss,
            "https://news.google.com/rss/search?q=" + quote("KOSIS 고령화 1인가구") + "&hl=ko&gl=KR&ceid=KR:ko",
            "KOSIS 통계", None, 5),
        return_exceptions=True,
    )

    google_news_keys = [f"news_{label}" for label, _ in DOMESTIC_NEWS_QUERIES]
    other_keys = ["kfcpi", "kfcpi_press", "bok",
                  "sangjo_mag", "kfda", "mohw",
                  "kostat_news", "kosis_news"]
    keys = google_news_keys + other_keys
    parsed = {k: (v if isinstance(v, list) else []) for k, v in zip(keys, results)}

    for k, v in zip(keys, results):
        if isinstance(v, Exception):
            log.error(f"[수집 에러:{k}] {v}")

    bok_filtered = [i for i in parsed["bok"] if any(kw in i["title"] for kw in BOK_KEYWORDS)]

    # 구글 뉴스 결과를 모두 합쳐 'news' 카테고리로
    news_items: list[dict] = []
    for k in google_news_keys:
        news_items.extend(parsed[k])

    raw: dict[str, list[dict]] = {
        "news": news_items,
        "funeral_orgs": parsed["kfcpi"] + parsed["kfcpi_press"] + parsed["sangjo_mag"] + parsed["kfda"],
        "gov": bok_filtered + parsed["mohw"] + parsed["kostat_news"] + parsed["kosis_news"],
    }

    data: dict[str, list[dict]] = {}
    all_new_keys: set[str] = set()
    for category, items in raw.items():
        clean, new_keys = dedup(items, seen)
        data[category] = clean
        all_new_keys |= new_keys

    save_seen(all_new_keys)
    for key, items in data.items():
        log.info(f"  [{key}] {len(items)}건 (신규)")

    total = sum(len(v) for v in data.values())
    if total == 0:
        log.warning("⚠️ 국내 브리핑: 신규 수집 0건!")

    return data


async def run_domestic() -> None:
    log.info("=== 국내 브리핑 시작 ===")
    today_str = datetime.now(KST).strftime("%Y-%m-%d")
    date_kor = datetime.now(KST).strftime("%Y.%m.%d")
    day_kor = ["월", "화", "수", "목", "금", "토", "일"][datetime.now(KST).weekday()]

    try:
        data = await collect_domestic()
        total = sum(len(v) for v in data.values())
        log.info(f"신규 {total}건 수집")

        def fmt(items: list[dict]) -> str:
            lines = []
            for item in items:
                line = f"[{item['source']}] {item['title']}"
                if item.get("link"):
                    line += f" ← {item['link']}"
                if item.get("summary"):
                    line += f"\n  {item['summary'][:300]}"
                lines.append(line)
            return "\n".join(lines)

        sections = []
        if data["news"]:
            sections.append(f"=== 언론사 뉴스 ===\n{fmt(data['news'])}")
        if data["funeral_orgs"]:
            sections.append(f"=== 장례 업계 동향 ===\n{fmt(data['funeral_orgs'])}")
        if data["gov"]:
            sections.append(f"=== 정부·공공 기관 ===\n{fmt(data['gov'])}")

        if not sections:
            briefing_text = f"📋 장례사업 브리핑 {date_kor} ({day_kor})\n\n오늘은 신규 수집 항목이 없습니다."
            await send_telegram(briefing_text)
            log.info("[Telegram] 전송 완료 (수집 0건)")
        else:
            raw_data = "\n\n".join(sections)
            prompt = f"오늘({date_kor} {day_kor}요일) 수집된 정보로 브리핑을 작성해주세요.\n\n{raw_data}"
            full_text = await claude_with_retry(prompt, SYSTEM_DOMESTIC)

            briefing_text = _extract_tag("BRIEFING", full_text) or full_text
            keywords = _extract_tag("KEYWORDS", full_text)
            summary = _extract_tag("SUMMARY", full_text)

            await send_telegram(briefing_text)

            all_items = data["news"] + data["funeral_orgs"] + data["gov"]
            links_lines = ["📎 원문 링크"]
            for item in all_items:
                if item.get("link"):
                    links_lines.append(f"• [{item['source']}] {item['title'][:35]}\n  {item['link']}")
            if len(links_lines) > 1:
                await send_telegram("\n".join(links_lines))

            log.info("[Telegram] 국내 브리핑 전송 완료")

            try:
                tags = extract_tags(data, keywords)
                await save_to_notion(today_str, keywords, briefing_text, data, tags, "국내")
                log.info("[Notion] 저장 완료")
            except Exception as e:
                log.error(f"[Notion] 저장 실패: {e}")

        # 주간 통계 업데이트
        try:
            update_weekly_stats(domestic=total, asia=0)
        except Exception as e:
            log.warning(f"[stats] 업데이트 실패: {e}")

    except Exception as e:
        log.error(f"국내 브리핑 실패: {e}", exc_info=True)
        try:
            await send_telegram(f"⚠️ 국내 브리핑 생성 중 오류: {e}")
        except Exception:
            pass

    log.info("=== 국내 브리핑 완료 ===")


# ═══════════════════════════════════════════════════════════════════════════
#  작업 2: 18:00 아시아 브리핑
# ═══════════════════════════════════════════════════════════════════════════

async def run_asia() -> None:
    log.info("=== 아시아 브리핑 시작 ===")
    today_str = datetime.now(KST).strftime("%Y-%m-%d")
    date_kor = datetime.now(KST).strftime("%Y.%m.%d")
    day_kor = ["월", "화", "수", "목", "금", "토", "일"][datetime.now(KST).weekday()]

    try:
        loop = asyncio.get_running_loop()
        items = await loop.run_in_executor(None, collect_asia)

        seen = load_seen()
        items, new_keys = dedup(items, seen)
        save_seen(new_keys)

        if not items:
            log.info("아시아 브리핑: 수집 0건 → 미전송")
            return

        def fmt(items: list[dict]) -> str:
            lines = []
            for item in items:
                line = f"[{item['source']}] {item['title']}"
                if item.get("summary"):
                    line += f"\n  {item['summary'][:300]}"
                if item.get("link"):
                    line += f"\n  {item['link']}"
                lines.append(line)
            return "\n".join(lines)

        prompt = f"오늘({date_kor} {day_kor}요일) 수집된 아시아 장례·고령화 뉴스로 브리핑을 작성해주세요.\n\n{fmt(items)}"
        full_text = await claude_with_retry(prompt, SYSTEM_ASIA)
        briefing_text = _extract_tag("BRIEFING", full_text) or full_text
        keywords = _extract_tag("KEYWORDS", full_text)
        summary = _extract_tag("SUMMARY", full_text)

        await send_telegram(briefing_text)

        links_lines = ["📎 원문 링크"]
        for item in items:
            if item.get("link"):
                links_lines.append(f"• [{item['source']}] {item['title'][:40]}\n  {item['link']}")
        if len(links_lines) > 1:
            await send_telegram("\n".join(links_lines))

        log.info("[Telegram] 아시아 브리핑 전송 완료")

        try:
            briefing_data = {"asia": items}
            tags = ["해외동향"] + extract_tags(briefing_data, keywords)
            await save_to_notion(today_str, keywords, briefing_text, briefing_data, tags[:10], "아시아")
            log.info("[Notion] 아시아 저장 완료")
        except Exception as e:
            log.error(f"[Notion] 아시아 저장 실패: {e}")

        # 주간 통계 업데이트
        try:
            update_weekly_stats(domestic=0, asia=len(items))
        except Exception as e:
            log.warning(f"[stats] 업데이트 실패: {e}")

    except Exception as e:
        log.error(f"아시아 브리핑 실패: {e}", exc_info=True)

    log.info("=== 아시아 브리핑 완료 ===")


# ═══════════════════════════════════════════════════════════════════════════
#  작업 3: 일요일 20:00 주간 아이디어 다이제스트
# ═══════════════════════════════════════════════════════════════════════════

NOTION_CONV_DB_ID: str = os.environ.get("NOTION_DATABASE_ID", "")

SYSTEM_DIGEST = """당신은 채영님의 주간 아이디어 정리 어시스턴트입니다.

채영님은 장례 산업을 운용·플랫폼화하는 사업가입니다.
지난 7일 동안 채영님이 Claude와 나눈 대화·아이디어를 돌아보며,
시간이 지나면 퇴색되는 아이디어를 되살리고 발전시킬 수 있도록 정리하세요.

선정 기준:
- 사업 아이디어나 통찰이 담긴 대화
- 반복 언급된 주제
- 실행 가능성이 보이는 구체적 아이디어

응답 형식:

<DIGEST>
━━━━━━━━━━━━━━━━━━━━━━
💎 이번 주 아이디어 다이제스트
[기간]
━━━━━━━━━━━━━━━━━━━━━━

(아이디어 5개 이내. 없으면 있는 만큼만.)

🔹 아이디어 제목
  맥락: 어떤 대화에서 나왔는지
  핵심: 무엇에 대한 아이디어인지 1~2줄
  이어갈 질문: 더 발전시키려면 고민할 것 1가지

💡 이번 주 관통하는 흐름
(반복된 키워드나 주제가 있으면 간략히 요약. 없으면 생략.)
</DIGEST>"""


async def run_weekly_digest() -> None:
    """매주 일요일 20:00 — 지난 7일 노션 대화에서 아이디어 추출."""
    log.info("=== 주간 다이제스트 시작 ===")
    try:
        if not NOTION_CONV_DB_ID:
            log.warning("NOTION_DATABASE_ID 없음 — 다이제스트 스킵")
            return

        since = datetime.now(timezone.utc) - timedelta(days=7)
        since_str = since.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        import httpx
        headers = {
            "Authorization": f"Bearer {NOTION_API_KEY}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json",
        }
        url = f"https://api.notion.com/v1/databases/{NOTION_CONV_DB_ID}/query"
        body = {
            "page_size": 50,
            "filter": {"property": "날짜", "date": {"on_or_after": since_str}},
        }

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(url, headers=headers, json=body)
            data = resp.json()

        pages = data.get("results", [])
        if not pages:
            log.info("주간 다이제스트: 지난 7일 대화 없음")
            return

        conv_texts = []
        for p in pages:
            props = p.get("properties", {})
            try:
                title_parts = props.get("이름", {}).get("title", [])
                title = "".join(pt.get("plain_text", "") for pt in title_parts)
                analysis_parts = props.get("분석결과", {}).get("rich_text", [])
                analysis = "".join(pt.get("plain_text", "") for pt in analysis_parts)
                if title:
                    conv_texts.append(f"[대화] {title[:300]}\n{analysis[:400]}")
            except Exception:
                continue

        if not conv_texts:
            log.info("주간 다이제스트: 대화 텍스트 추출 실패")
            return

        conv_block = "\n\n".join(conv_texts)
        if len(conv_block) > 20000:
            conv_block = conv_block[:20000] + "\n...(이하 생략)"

        today = datetime.now(KST)
        week_start = (today - timedelta(days=7)).strftime("%m.%d")
        week_end = today.strftime("%m.%d")
        period_str = f"{week_start} ~ {week_end}"

        prompt = f"기간: {period_str}\n\n지난 7일 대화 {len(pages)}건:\n\n{conv_block}\n\n위 대화에서 주목할 만한 아이디어를 정리해주세요."

        full_text = await claude_with_retry(prompt, SYSTEM_DIGEST, max_tokens=3000)
        digest_text = _extract_tag("DIGEST", full_text) or full_text

        await send_telegram(digest_text)
        log.info("[Telegram] 주간 다이제스트 전송 완료")

    except Exception as e:
        log.error(f"주간 다이제스트 실패: {e}", exc_info=True)

    log.info("=== 주간 다이제스트 완료 ===")


# ═══════════════════════════════════════════════════════════════════════════
#  작업 4: 월요일 08:05 주간 스코어보드
# ═══════════════════════════════════════════════════════════════════════════

async def run_scoreboard() -> None:
    """매주 월요일 08:05 — 지난주 수집 통계와 체크인 연속 일수 발송."""
    log.info("=== 주간 스코어보드 시작 ===")
    try:
        stats = load_weekly_stats()
        week_start = stats.get("week_start", "")
        today = datetime.now(KST).strftime("%Y-%m-%d")

        domestic = stats.get("domestic_items", 0)
        asia = stats.get("asia_items", 0)
        total_items = domestic + asia
        days = stats.get("briefing_days", 0)

        # 체크인 연속 일수 (checkin_bot.py에서 관리)
        checkin = load_checkin()
        streak = checkin.get("streak", 0)

        scoreboard = (
            f"📊 지난주 인텔리전스 스코어보드\n"
            f"{week_start} ~ {today}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🇰🇷 국내 수집: {domestic}건\n"
            f"🌏 아시아 수집: {asia}건\n"
            f"📦 총 수집: {total_items}건\n"
            f"📨 브리핑 발송: {days}일\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔥 체크인 연속: {streak}일\n"
        )

        await send_telegram(scoreboard)
        log.info("[Telegram] 주간 스코어보드 전송 완료")

        # 통계 초기화 (새 주 시작)
        reset_weekly_stats()

    except Exception as e:
        log.error(f"주간 스코어보드 실패: {e}", exc_info=True)
        try:
            await send_telegram(f"⚠️ 주간 스코어보드 생성 중 오류: {e}")
        except Exception:
            pass

    log.info("=== 주간 스코어보드 완료 ===")


# ═══════════════════════════════════════════════════════════════════════════
#  스케줄러 / 메인
# ═══════════════════════════════════════════════════════════════════════════

async def main() -> None:
    await setup_notion_db()

    scheduler = AsyncIOScheduler(timezone=KST)

    scheduler.add_job(run_domestic, trigger="cron", hour=8, minute=0,
                      id="domestic", name="08:00 국내 브리핑", misfire_grace_time=300)

    scheduler.add_job(run_asia, trigger="cron", hour=18, minute=0,
                      id="asia", name="18:00 아시아 브리핑", misfire_grace_time=300)

    # 매주 일요일 20:00 KST 주간 다이제스트
    scheduler.add_job(run_weekly_digest, trigger="cron", day_of_week="sun", hour=20, minute=0,
                      id="digest", name="일요일 20:00 주간 다이제스트", misfire_grace_time=300)

    # 매주 월요일 08:05 KST 주간 스코어보드
    scheduler.add_job(run_scoreboard, trigger="cron", day_of_week="mon", hour=8, minute=5,
                      id="scoreboard", name="월요일 08:05 주간 스코어보드", misfire_grace_time=300)

    scheduler.start()
    log.info("브리핑 봇 v3 시작")
    log.info("  08:00 국내 | 18:00 아시아 | 일 20:00 다이제스트 | 월 08:05 스코어보드")

    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        log.info("브리핑 봇 종료")
        scheduler.shutdown()


if __name__ == "__main__":
    if "--now" in sys.argv:
        log.info("즉시 실행: 국내 브리핑")
        asyncio.run(run_domestic())
    elif "--asia" in sys.argv:
        log.info("즉시 실행: 아시아 브리핑")
        asyncio.run(run_asia())
    elif "--digest" in sys.argv:
        log.info("즉시 실행: 주간 다이제스트")
        asyncio.run(run_weekly_digest())
    elif "--board" in sys.argv:
        log.info("즉시 실행: 주간 스코어보드")
        asyncio.run(run_scoreboard())
    else:
        asyncio.run(main())
