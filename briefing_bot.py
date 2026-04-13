"""
브리핑 봇 - 매일 아침 8시 자동 실행
--------------------------------------
노션 저장 구조:
  이름        = 날짜 | 핵심 키워드
  내용        = 기사 제목 목록
  분석결과    = 핵심 요약 3줄 이내
  출처        = 실제 출처명 목록
  링크        = 원문 URL 목록
  날짜        = 수집 일시

텔레그램 브리핑 형식:
  【1층 — 장례업 직결】 / 【2층 — 거시 경제 흐름】
  각 기사에 원문 링크 포함
  마지막에 📎 원문 링크 모음 섹션

실행:
  python briefing_bot.py          # 매일 08:00 KST 스케줄
  python briefing_bot.py --now    # 즉시 1회 실행 (테스트)
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
SEEN_MAX_DAYS = 30

TELEGRAM_BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID: str = os.environ["TELEGRAM_CHAT_ID"]
ANTHROPIC_API_KEY: str = os.environ["ANTHROPIC_API_KEY"]
NOTION_API_KEY: str = os.environ["NOTION_API_KEY"]
NOTION_BRIEFING_DB_ID: str = os.environ["NOTION_BRIEFING_DB_ID"]
YOUTUBE_CHANNEL_URL: str = os.getenv(
    "YOUTUBE_CHANNEL_URL",
    "https://www.youtube.com/@손경제/videos",
)

TELEGRAM_LIMIT = 4096
NOTION_LIMIT = 1900
KST = ZoneInfo("Asia/Seoul")

# ---------------------------------------------------------------------------
# 키워드 필터
# ---------------------------------------------------------------------------
NEWS_KEYWORDS = [
    "장례", "고령화", "초고령화", "인구", "사망", "시니어", "상속",
    "1인가구", "AI 비즈니스", "웰다잉", "호스피스", "납골", "화장",
]
BOK_KEYWORDS = [
    "장례", "고령화", "초고령화", "인구", "사망", "시니어", "상속",
    "1인가구", "경제전망", "지역경제", "소비", "인구구조",
]

# ---------------------------------------------------------------------------
# 클라이언트
# ---------------------------------------------------------------------------
claude = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
notion = NotionAsyncClient(auth=NOTION_API_KEY)
tg_bot = Bot(token=TELEGRAM_BOT_TOKEN)

# ---------------------------------------------------------------------------
# 시스템 프롬프트
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = """당신은 채영님의 장례사업 전문 브리핑 어시스턴트입니다.
채영님은 장례 관련 사업을 운영하는 전문가입니다.

분석 관점:
- 고령화·초고령화 트렌드가 장례 수요에 미치는 영향
- 1인가구 증가에 따른 장례 서비스 수요 변화
- 장례 관련 정책·규제·지원 변화 / 업계 동향
- 상속·사망 관련 법제도 변화
- AI·디지털 기술의 장례 서비스 적용 가능성
- 해외(특히 일본) 장례 트렌드의 국내 적용 가능성

응답은 반드시 아래 세 섹션을 XML 태그로 구분해서 작성하세요.

<BRIEFING>
브리핑 전문 (텔레그램 전송용). 형식은 아래를 따르세요.
수집 항목이 없는 섹션은 통째로 생략합니다.

━━━━━━━━━━━━━━━━━━━━━━
📋 장례사업 브리핑 [날짜]
━━━━━━━━━━━━━━━━━━━━━━

【1층 — 장례업 직결】
장례·납골·화장·호스피스·웰다잉·장례기관 공지 등 장례업 직결 정보.
• [출처] 기사 제목
  → 인사이트 (1~2줄)

【2층 — 거시 경제 흐름】
고령화·인구구조·1인가구·상속·AI·경제전망·통계 등 간접 흐름.
• [출처] 내용 요약

🎙️ 손경제 유튜브
• 영상 제목: (제목)
• 내용: 자막/설명을 있는 그대로 요약. 장례업과 억지 연결 금지.

[월요일에만 포함]
🌏 해외 장례 트렌드
• [출처] 내용 + 국내 시사점

💡 오늘의 시사점
• 실행 가능한 관찰 또는 액션 1~3가지
</BRIEFING>

<KEYWORDS>
수집된 정보를 대표하는 핵심 키워드 3~5개. 쉼표로 구분. (예: 고령화, 1인가구, 장례 수요)
</KEYWORDS>

<SUMMARY>
핵심 요약 3줄 이내. 노션 기록용으로 간결하게.
</SUMMARY>"""


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


HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
}


# ---------------------------------------------------------------------------
# 태그 정의 및 자동 추출
# ---------------------------------------------------------------------------
TAG_RULES: list[tuple[str, list[str]]] = [
    ("장례업 직결",  ["장례", "납골", "화장", "호스피스", "웰다잉", "장례식장", "빈소", "추모", "장례지도"]),
    ("고령화",       ["고령화", "초고령화", "고령", "노인", "시니어", "노령"]),
    ("인구·1인가구", ["인구", "1인가구", "저출산", "출산율", "인구구조", "인구감소"]),
    ("상속·법제도",  ["상속", "유언", "법", "제도", "정책", "규제", "세금", "세제"]),
    ("경제동향",     ["경제", "금리", "물가", "소비", "GDP", "성장", "경기", "지역경제", "경제전망"]),
    ("AI·디지털",   ["AI", "인공지능", "디지털", "플랫폼", "테크", "앱", "스타트업"]),
    ("해외동향",     ["일본", "해외", "글로벌", "foreign", "overseas", "Japan", "funeral industry"]),
    ("통계·조사",    ["통계", "조사", "분석", "보고서", "데이터"]),
    ("사망·보건",    ["사망", "사망률", "보건", "의료", "병원", "질병"]),
]


def extract_tags(data: dict[str, list[dict]], keywords: str) -> list[str]:
    """수집 데이터 + Claude 키워드 기반으로 노션 태그 추출."""
    # 전체 텍스트 풀 구성
    text_pool = keywords + " "
    for category, items in data.items():
        for item in items:
            text_pool += f" {item.get('title', '')} {item.get('summary', '')}"

    # 소스 기반 태그
    all_sources = {item.get("source", "") for items in data.values() for item in items}
    tags: list[str] = []

    for tag, kw_list in TAG_RULES:
        if any(kw.lower() in text_pool.lower() for kw in kw_list):
            tags.append(tag)

    # 소스 기반 추가 태그
    if any("유튜브" in s or "손경제" in s for s in all_sources):
        if "손경제" not in tags:
            tags.append("손경제")
    if any("장례문화진흥원" in s or "장례협회" in s or "장례지도" in s for s in all_sources):
        if "장례업계" not in tags:
            tags.append("장례업계")
    if any("한국은행" in s for s in all_sources) and data.get("bok"):
        if "한국은행" not in tags:
            tags.append("한국은행")

    return tags[:10]  # 노션 멀티셀렉트 최대 10개


# ---------------------------------------------------------------------------
# 노션 DB 컬럼 자동 추가
# ---------------------------------------------------------------------------
async def setup_notion_db() -> None:
    """브리핑 DB에 필요한 컬럼 추가 시도."""
    try:
        await notion.databases.update(
            database_id=NOTION_BRIEFING_DB_ID,
            properties={
                "내용": {"rich_text": {}},
                "링크": {"rich_text": {}},
                "태그": {"multi_select": {}},
            },
        )
        log.info("[Notion] DB 컬럼 확인/추가 완료 (내용·링크·태그)")
    except Exception as e:
        log.warning(f"[Notion] DB 컬럼 자동 추가 실패 — Notion에서 수동 추가 필요: {e}")


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
# 수집기 2: 게시판 크롤링
# ---------------------------------------------------------------------------
def crawl_board(url: str, source: str, max_items: int = 10) -> list[dict]:
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        candidate_selectors = [
            "td.subject a", "td.title a", ".board-list td a",
            ".list-title a", ".bbs-list td a", "table.bbs_list td a",
            "ul.board_list li a", ".notice-list a",
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
            seen_titles.add(title)
            href = tag.get("href", "")
            if href and not href.startswith("http"):
                href = urljoin(url, href)
            items.append({"source": source, "title": title, "summary": "", "link": href or url, "published": ""})
            if len(items) >= max_items:
                break
        return items
    except Exception as e:
        log.error(f"[CRAWL:{source}] 실패: {e}")
        return []


# ---------------------------------------------------------------------------
# 수집기 3: 유튜브 자막 추출
# ---------------------------------------------------------------------------
def _parse_vtt(text: str) -> str:
    lines = text.splitlines()
    result = []
    for line in lines:
        line = line.strip()
        if not line or line.startswith("WEBVTT") or "-->" in line:
            continue
        if re.match(r"^\d+$", line):
            continue
        line = re.sub(r"<[^>]+>", "", line)
        if line:
            result.append(line)
    deduped = []
    for line in result:
        if not deduped or line != deduped[-1]:
            deduped.append(line)
    return " ".join(deduped)


def _parse_json3(text: str) -> str:
    try:
        data = json.loads(text)
        parts = []
        for event in data.get("events", []):
            for seg in event.get("segs", []):
                utf8 = seg.get("utf8", "")
                if utf8 and utf8 != "\n":
                    parts.append(utf8)
        return re.sub(r"\s+", " ", "".join(parts)).strip()
    except Exception:
        return ""


def _fetch_subtitle(sub_list: list[dict]) -> str:
    priority = ["vtt", "json3", "srv3", "ttml"]
    ordered = sorted(sub_list, key=lambda x: priority.index(x.get("ext", "")) if x.get("ext", "") in priority else 99)
    for sub in ordered[:3]:
        url = sub.get("url", "")
        if not url:
            continue
        try:
            resp = requests.get(url, timeout=15, headers=HTTP_HEADERS)
            ext = sub.get("ext", "")
            text = _parse_json3(resp.text) if ext == "json3" else _parse_vtt(resp.text)
            if text:
                return text
        except Exception as e:
            log.warning(f"[YouTube] 자막 다운로드 실패 ({ext}): {e}")
    return ""


def collect_youtube(channel_url: str, max_videos: int = 3) -> list[dict]:
    """yt-dlp로 채널/플레이리스트 최신 영상의 제목 + 한국어 자막 수집.

    /podcasts 탭처럼 플레이리스트 ID가 반환되면 자동으로 펼쳐서 개별 영상을 수집합니다.
    """
    try:
        import yt_dlp

        flat_opts = {"quiet": True, "no_warnings": True, "extract_flat": True, "playlistend": 10}

        def get_video_entries(url: str) -> list[dict]:
            """URL에서 개별 영상 엔트리 목록 반환 (플레이리스트 자동 확장)."""
            with yt_dlp.YoutubeDL(flat_opts) as ydl:
                info = ydl.extract_info(url, download=False)
            if not info:
                return []
            entries = info.get("entries") or []
            video_entries = []
            for e in entries:
                eid = e.get("id", "")
                if not eid:
                    continue
                if eid.startswith("PL") or eid.startswith("RD"):
                    # 플레이리스트 → 재귀로 펼치기
                    log.info(f"  [YouTube] 플레이리스트 확장: {eid}")
                    pl_url = f"https://www.youtube.com/playlist?list={eid}"
                    with yt_dlp.YoutubeDL(flat_opts) as ydl2:
                        pl_info = ydl2.extract_info(pl_url, download=False)
                    if pl_info:
                        for ve in (pl_info.get("entries") or []):
                            vid = ve.get("id", "")
                            if vid and not vid.startswith("PL"):
                                video_entries.append(ve)
                else:
                    video_entries.append(e)
            return video_entries

        video_entries = get_video_entries(channel_url)
        if not video_entries:
            log.warning(f"[YouTube] 수집 가능한 영상 없음: {channel_url}")
            return []

        log.info(f"  [YouTube] 후보 영상 {len(video_entries)}개 발견")

        videos = []
        detail_opts = {"quiet": True, "no_warnings": True}

        for entry in video_entries[:max_videos]:
            vid_id = entry.get("id", "")
            if not vid_id:
                continue
            try:
                with yt_dlp.YoutubeDL(detail_opts) as ydl:
                    vid = ydl.extract_info(f"https://www.youtube.com/watch?v={vid_id}", download=False)
                title = vid.get("title", entry.get("title", ""))
                subtitles = vid.get("subtitles") or {}
                auto_caps = vid.get("automatic_captions") or {}
                sub_text = ""
                for lang in ["ko", "en"]:
                    sub_list = subtitles.get(lang) or auto_caps.get(lang) or []
                    if sub_list:
                        sub_text = _fetch_subtitle(sub_list)
                        if sub_text:
                            break
                content = sub_text[:2000] if sub_text else (vid.get("description") or "")[:800]
                videos.append({
                    "source": "손경제 유튜브",
                    "title": title,
                    "summary": content.strip(),
                    "link": f"https://www.youtube.com/watch?v={vid_id}",
                    "published": vid.get("upload_date", ""),
                    "has_subtitle": bool(sub_text),
                })
                log.info(f"  [YouTube] '{title[:30]}' — {'자막' if sub_text else '설명란'} 수집")
            except Exception as e:
                log.warning(f"[YouTube] 영상 상세 실패 ({vid_id}): {e}")

        return videos

    except ImportError:
        log.warning("[YouTube] yt-dlp 미설치")
        return []
    except Exception as e:
        log.error(f"[YouTube] 수집 실패: {e}")
        return []


# ---------------------------------------------------------------------------
# 수집기 4: 해외 장례 트렌드 (월요일 전용)
# ---------------------------------------------------------------------------
def collect_overseas() -> list[dict]:
    items: list[dict] = []
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
            log.warning(f"[해외] 후생노동성 실패: {e}")
    for query in ["funeral industry trends innovation 2025", "death care aging population services", "Japan funeral industry senior trend"]:
        try:
            url = f"https://news.google.com/rss/search?q={quote(query)}&hl=en&gl=US&ceid=US:en"
            feed = feedparser.parse(url, request_headers=HTTP_HEADERS)
            for entry in feed.entries[:3]:
                raw = getattr(entry, "summary", "")
                items.append({"source": "해외 장례 트렌드", "title": getattr(entry, "title", ""),
                               "summary": BeautifulSoup(raw, "html.parser").get_text()[:300],
                               "link": getattr(entry, "link", ""), "published": getattr(entry, "published", "")})
        except Exception as e:
            log.warning(f"[해외] 검색 실패 ({query}): {e}")
    return items[:20]


# ---------------------------------------------------------------------------
# 전체 수집 + 중복 제거
# ---------------------------------------------------------------------------
async def collect_all() -> dict[str, list[dict]]:
    today = datetime.now(KST)
    is_monday = today.weekday() == 0
    loop = asyncio.get_running_loop()

    def run(fn, *args):
        return loop.run_in_executor(None, fn, *args)

    log.info("=== 브리핑 데이터 수집 시작 ===")
    seen = load_seen()
    log.info(f"[seen] 기존 기록 {len(seen)}개 로드")

    results = await asyncio.gather(
        run(collect_rss, "https://www.hani.co.kr/rss/", "한겨레", NEWS_KEYWORDS),
        run(collect_rss, "https://www.chosun.com/arc/outboundfeeds/rss/?outputType=xml", "조선일보", NEWS_KEYWORDS),
        run(collect_rss, "https://www.mk.co.kr/rss/30000001/", "매일경제", NEWS_KEYWORDS),
        run(collect_rss, "https://www.hankyung.com/feed/all-news", "한국경제", NEWS_KEYWORDS),
        run(collect_rss, "https://rss.joins.com/joins_news_list.xml", "중앙일보", NEWS_KEYWORDS),
        run(crawl_board, "https://www.kfcpi.or.kr/portal/home/bbs/list.do?menuId=M0001000600010000", "한국장례문화진흥원"),
        run(crawl_board, "https://www.kfcpi.or.kr/portal/home/bbs/list.do?menuId=M0001000700000000", "한국장례문화진흥원 보도자료"),
        run(crawl_board, "https://www.bok.or.kr/portal/bbs/P0000559/list.do?menuNo=200690", "한국은행"),
        run(collect_youtube, YOUTUBE_CHANNEL_URL),
        run(crawl_board, "https://mods.go.kr/board.es?mid=a10301010000&bid=218", "통계청"),
        return_exceptions=True,
    )

    keys = ["hani", "chosun", "mk", "hankyung", "joins", "kfcpi", "kfcpi_press", "bok", "youtube", "kostat"]
    parsed = {k: (v if isinstance(v, list) else []) for k, v in zip(keys, results)}
    bok_filtered = [i for i in parsed["bok"] if any(kw in i["title"] for kw in BOK_KEYWORDS)]

    raw: dict[str, list[dict]] = {
        "news": parsed["hani"] + parsed["chosun"] + parsed["mk"] + parsed["hankyung"] + parsed["joins"],
        "funeral_orgs": parsed["kfcpi"] + parsed["kfcpi_press"],
        "bok": bok_filtered,
        "youtube": parsed["youtube"],
        "kostat": parsed["kostat"],
        "overseas": [],
    }

    if is_monday:
        log.info("[수집] 해외 장례 트렌드 (월요일)...")
        raw["overseas"] = await loop.run_in_executor(None, collect_overseas)

    data: dict[str, list[dict]] = {}
    all_new_keys: set[str] = set()
    for category, items in raw.items():
        clean, new_keys = dedup(items, seen)
        data[category] = clean
        all_new_keys |= new_keys

    save_seen(all_new_keys)
    for key, items in data.items():
        log.info(f"  [{key}] {len(items)}건 (신규)")
    return data


# ---------------------------------------------------------------------------
# Claude 브리핑 생성 → 구조화된 결과 반환
# ---------------------------------------------------------------------------
def _extract_tag(tag: str, text: str) -> str:
    match = re.search(f"<{tag}>(.*?)</{tag}>", text, re.DOTALL)
    return match.group(1).strip() if match else ""


async def generate_briefing(data: dict[str, list[dict]]) -> dict[str, str]:
    """Claude 호출 → {telegram, keywords, summary} 반환."""
    today = datetime.now(KST)
    day_kor = ["월", "화", "수", "목", "금", "토", "일"][today.weekday()]
    date_str = today.strftime(f"%Y년 %m월 %d일 ({day_kor})")
    is_monday = today.weekday() == 0

    def fmt(items: list[dict], include_summary: bool = True) -> str:
        lines = []
        for item in items:
            line = f"[{item['source']}] {item['title']}"
            if item.get("link"):
                line += f"  ← {item['link']}"
            if include_summary and item.get("summary"):
                line += f"\n  {item['summary'][:300]}"
            lines.append(line)
        return "\n".join(lines)

    sections: list[str] = []
    if data["news"]:
        sections.append(f"=== 언론사 뉴스 ===\n{fmt(data['news'])}")
    if data["funeral_orgs"]:
        sections.append(f"=== 장례 기관 동향 ===\n{fmt(data['funeral_orgs'], include_summary=False)}")
    if data["bok"]:
        sections.append(f"=== 한국은행 ===\n{fmt(data['bok'])}")
    if data["kostat"]:
        sections.append(f"=== 통계청 ===\n{fmt(data['kostat'])}")
    if data["youtube"]:
        yt_lines = []
        for v in data["youtube"]:
            tag = "(자막)" if v.get("has_subtitle") else "(설명란)"
            yt_lines.append(f"제목: {v['title']}\n링크: {v['link']}\n내용 {tag}:\n{v['summary'][:1500]}")
        sections.append("=== 손경제 유튜브 ===\n" + "\n\n---\n\n".join(yt_lines))
    if is_monday and data["overseas"]:
        sections.append(f"=== 해외 장례 트렌드 ===\n{fmt(data['overseas'])}")

    if not sections:
        return {
            "telegram": f"📋 장례사업 브리핑 {date_str}\n\n오늘은 신규 수집 항목이 없습니다.",
            "keywords": "",
            "summary": "신규 항목 없음",
        }

    raw_data = "\n\n".join(sections)

    response = await claude.messages.create(
        model="claude-opus-4-6",
        max_tokens=3500,
        system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": f"오늘({date_str}) 수집된 정보로 브리핑을 작성해주세요.\n\n{raw_data}"}],
    )

    full_text = next((b.text for b in response.content if b.type == "text"), "")

    return {
        "telegram": _extract_tag("BRIEFING", full_text) or full_text,
        "keywords": _extract_tag("KEYWORDS", full_text),
        "summary": _extract_tag("SUMMARY", full_text),
    }


# ---------------------------------------------------------------------------
# 텔레그램 링크 모음 섹션 생성
# ---------------------------------------------------------------------------
def build_links_section(data: dict[str, list[dict]]) -> str:
    """수집된 모든 기사의 원문 링크 목록을 텔레그램 메시지용으로 포맷."""
    lines = ["📎 원문 링크"]
    for category in ["news", "funeral_orgs", "bok", "kostat", "overseas", "youtube"]:
        for item in data.get(category, []):
            link = item.get("link", "")
            title = item.get("title", "")[:35]
            source = item.get("source", "")
            if link and title:
                lines.append(f"• [{source}] {title}\n  {link}")
    return "\n".join(lines) if len(lines) > 1 else ""


# ---------------------------------------------------------------------------
# Notion 저장
# ---------------------------------------------------------------------------
async def save_to_notion(
    date_str: str,
    keywords: str,
    briefing_summary: str,
    data: dict[str, list[dict]],
    tags: list[str],
) -> None:
    all_items = (
        data.get("news", []) + data.get("funeral_orgs", []) +
        data.get("bok", []) + data.get("kostat", []) +
        data.get("overseas", []) + data.get("youtube", [])
    )

    title_list = "\n".join(f"• [{i['source']}] {i['title']}" for i in all_items)

    seen_src: set[str] = set()
    sources_ordered = []
    for i in all_items:
        s = i.get("source", "")
        if s and s not in seen_src:
            seen_src.add(s)
            sources_ordered.append(s)
    sources_text = ", ".join(sources_ordered)

    links_text = "\n".join(f"• {i.get('link', '')}" for i in all_items if i.get("link"))
    notion_title = f"{date_str} | {keywords}" if keywords else date_str

    # 필수 속성 (항상 존재)
    properties: dict = {
        "이름": {"title": [{"text": {"content": notion_title[:200]}}]},
        "분석결과": {"rich_text": build_notion_rich_text(briefing_summary)},
        "출처": {"rich_text": [{"text": {"content": sources_text[:1900]}}]},
        "날짜": {"date": {"start": datetime.now(timezone.utc).isoformat()}},
    }

    # 선택 속성 (없는 컬럼은 자동 제외 후 재시도)
    optional: dict = {
        "내용": {"rich_text": build_notion_rich_text(title_list)},
        "링크": {"rich_text": build_notion_rich_text(links_text)},
        "태그": {"multi_select": [{"name": t} for t in tags]},
    }

    # 먼저 전체 속성으로 시도
    try:
        await notion.pages.create(
            parent={"database_id": NOTION_BRIEFING_DB_ID},
            properties={**properties, **optional},
        )
        log.info(f"[Notion] 태그: {tags}")
        return
    except Exception as e:
        err_msg = str(e)
        missing = [k for k in optional if k in err_msg]
        if not missing:
            raise
        log.warning(f"[Notion] 컬럼 없음 ({missing}) — 제외 후 재시도. Notion에서 수동 추가 권장.")
        for k in missing:
            optional.pop(k, None)

    await notion.pages.create(
        parent={"database_id": NOTION_BRIEFING_DB_ID},
        properties={**properties, **optional},
    )
    log.info(f"[Notion] 태그: {tags}")


# ---------------------------------------------------------------------------
# 텔레그램 전송
# ---------------------------------------------------------------------------
async def send_telegram(text: str) -> None:
    for chunk in chunk_text(text, TELEGRAM_LIMIT):
        await tg_bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=chunk)


# ---------------------------------------------------------------------------
# 메인 브리핑 작업
# ---------------------------------------------------------------------------
async def run_briefing() -> None:
    log.info("=== 브리핑 시작 ===")
    today_str = datetime.now(KST).strftime("%Y-%m-%d")

    try:
        data = await collect_all()
        total = sum(len(v) for v in data.values())
        log.info(f"신규 {total}건 수집 완료")

        log.info("[Claude] 브리핑 생성 중...")
        result = await generate_briefing(data)
        log.info("[Claude] 생성 완료")

        # 텔레그램: 브리핑 본문 + 원문 링크 모음
        await send_telegram(result["telegram"])
        links_section = build_links_section(data)
        if links_section:
            await send_telegram(links_section)
        log.info("[Telegram] 전송 완료")

        # 노션 저장
        try:
            tags = extract_tags(data, result["keywords"])
            await save_to_notion(
                date_str=today_str,
                keywords=result["keywords"],
                briefing_summary=result["summary"],
                data=data,
                tags=tags,
            )
            log.info("[Notion] 저장 완료")
        except Exception as e:
            log.error(f"[Notion] 저장 실패: {e}")

    except Exception as e:
        log.error(f"브리핑 실패: {e}", exc_info=True)
        try:
            await send_telegram(f"⚠️ 브리핑 생성 중 오류 발생: {e}")
        except Exception:
            pass

    log.info("=== 브리핑 완료 ===")


# ---------------------------------------------------------------------------
# 스케줄러
# ---------------------------------------------------------------------------
async def main() -> None:
    await setup_notion_db()

    scheduler = AsyncIOScheduler(timezone=KST)
    scheduler.add_job(
        run_briefing,
        trigger="cron",
        hour=8,
        minute=0,
        id="daily_briefing",
        name="매일 아침 08:00 KST 브리핑",
        misfire_grace_time=300,
    )
    scheduler.start()
    next_run = scheduler.get_job("daily_briefing").next_run_time
    log.info(f"브리핑 봇 시작 — 다음 실행: {next_run.strftime('%Y-%m-%d %H:%M KST')}")
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        log.info("브리핑 봇 종료")
        scheduler.shutdown()


async def run_now() -> None:
    await setup_notion_db()
    await run_briefing()


if __name__ == "__main__":
    if "--now" in sys.argv:
        log.info("즉시 실행 모드 (--now)")
        asyncio.run(run_now())
    else:
        asyncio.run(main())
