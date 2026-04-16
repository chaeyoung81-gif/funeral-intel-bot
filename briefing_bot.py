"""
브리핑 봇 v2 - 3분할 스케줄
--------------------------------------
08:00 KST — 국내 브리핑 (RSS + 업계 소스)
11:00 KST — 손경제 브리핑 (팟캐스트 목차 + 유튜브 자막 분석)
         → 실패 시 13:00 재시도, 그래도 없으면 다음날
18:00 KST — 아시아 브리핑 (일본·중국·대만·홍콩·싱가포르)
         → 수집 0건이면 미전송

실행:
    python briefing_bot.py          # 스케줄 모드
    python briefing_bot.py --now    # 국내 브리핑 즉시 1회
    python briefing_bot.py --son    # 손경제 즉시 1회
    python briefing_bot.py --asia   # 아시아 즉시 1회
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

# 손경제 유튜브 채널
YOUTUBE_CHANNEL_URL: str = os.getenv(
    "YOUTUBE_CHANNEL_URL",
    "https://www.youtube.com/@손경제/videos",
)

# 손경제 유튜브 — 2개 소스로 수집 (본방 + 플러스 모두 잡기)
YOUTUBE_CHANNEL_URL: str = os.getenv(
    "YOUTUBE_CHANNEL_URL",
    "https://www.youtube.com/@손경제/videos",
)
# 본방 검색 키워드 (yt-dlp search)
YOUTUBE_SEARCH_MAIN: str = "ytsearch5:[손경제] 깊이있는 경제뉴스"

TELEGRAM_LIMIT = 4096
NOTION_LIMIT = 1900
MODEL = "claude-sonnet-4-6"  # Sonnet으로 통일 (비용 절감)

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
    all_sources = {item.get("source", "") for items in data.values() for item in items}
    if any("유튜브" in s or "손경제" in s for s in all_sources):
        if "손경제" not in tags:
            tags.append("손경제")
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
# 수집기 2: 게시판 크롤링 (개선 - bbs/list 패턴만 허용)
# ---------------------------------------------------------------------------
def crawl_board(url: str, source: str, max_items: int = 10) -> list[dict]:
    try:
        resp = requests.get(url, headers=HTTP_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # 게시판 글 링크 패턴 (bbs, board, view, detail 등)
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

            # 고정 메뉴 페이지 제외 (contents_view 패턴)
            if "contents_view" in href or "contents/contents" in href:
                continue
            # 게시판 글인지 확인 (최소한 URL에 숫자 ID가 있어야 함)
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
    """yt-dlp로 채널 최신 영상의 제목 + 한국어 자막 수집."""
    try:
        import yt_dlp
        flat_opts = {"quiet": True, "no_warnings": True, "extract_flat": True, "playlistend": 10}

        def get_video_entries(url: str) -> list[dict]:
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

                    # 손경제 상세 분석용: 자막 제한 해제 (최대 8000자)
                    content = sub_text[:8000] if sub_text else (vid.get("description") or "")[:800]
                    # 목차 (영상 설명란) — 항상 수집
                    description = (vid.get("description") or "")[:1500]

                    videos.append({
                        "source": "손경제 유튜브",
                        "title": title,
                        "summary": content.strip(),
                        "description": description.strip(),
                        "link": f"https://www.youtube.com/watch?v={vid_id}",
                        "published": vid.get("upload_date", ""),
                        "has_subtitle": bool(sub_text),
                    })
                    log.info(f"  [YouTube] '{title[:40]}' — {'자막' if sub_text else '설명란'} 수집")
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
# 수집기 4: 팟캐스트 에피소드 목차 (podtail에서 수집)
# ---------------------------------------------------------------------------
def collect_podcast_episodes(max_episodes: int = 5) -> list[dict]:
    """podtail에서 손경제 팟캐스트 에피소드 목차 텍스트 수집."""
    try:
        resp = requests.get(PODCAST_URL, headers=HTTP_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        episodes = []
        # podtail 에피소드 블록 파싱
        for block in soup.select(".episode-list-item, .episode"):
            title_tag = block.select_one("h3, .episode-title, a[href*='podcast']")
            if not title_tag:
                continue
            title = title_tag.get_text(strip=True)

            # 커피타임, 협찬 등 제외
            if "커피타임" in title or "협찬" in title:
                continue

            # 에피소드 설명 (목차 텍스트)
            desc_tag = block.select_one(".episode-description, .description, p")
            description = desc_tag.get_text(strip=True) if desc_tag else ""

            link_tag = title_tag if title_tag.name == "a" else block.select_one("a")
            link = link_tag.get("href", "") if link_tag else ""
            if link and not link.startswith("http"):
                link = urljoin(PODCAST_URL, link)

            episodes.append({
                "title": title,
                "description": description[:1000],
                "link": link,
            })
            if len(episodes) >= max_episodes:
                break

        log.info(f"[Podcast] {len(episodes)}개 에피소드 수집")
        return episodes
    except Exception as e:
        log.error(f"[Podcast] 수집 실패: {e}")
        return []


# ---------------------------------------------------------------------------
# 수집기 5: 아시아 장례·고령화 뉴스
# ---------------------------------------------------------------------------
def collect_asia() -> list[dict]:
    """일본·중국·대만·홍콩·싱가포르의 장례·고령화 관련 뉴스 수집."""
    items: list[dict] = []

    # --- 일본 ---
    jp_keywords = ["葬", "死亡", "高齢", "超高齢", "人口", "相続", "終活", "墓", "火葬"]
    # 후생노동성 RSS
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

    # 일본 구글뉴스
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

    # --- 중국 ---
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

    # --- 대만 ---
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

    # --- 홍콩 ---
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

    # --- 싱가포르 ---
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

# --- 08:00 국내 브리핑용 ---
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

# --- 11:00 손경제 브리핑용 ---
SYSTEM_SONECONOMY = """당신은 채영님의 장례사업 전문 브리핑 어시스턴트입니다.

채영님은 장례 산업을 운용·플랫폼화하는 사업가입니다.
손에 잡히는 경제(손경제)는 채영님에게 매우 중요한 프로그램입니다.
장례 뉴스뿐 아니라 모든 경제 이슈에서 직간접적 사업 아이디어를 얻기 때문입니다.
예: 요양보호사 대란 → 장례지도사 자격증 남발 흐름 감지
예: AI 이슈 → 장례 플랫폼 자동화 아이디어
예: 부동산 → 장례식장 입지 전략

자동자막 오류 가능성을 감안하고, 맥락상 올바른 용어로 보정하세요.

응답 형식:

<BRIEFING>
🎙️ 손경제 브리핑 [날짜]

(에피소드별로 아래 형식 반복. [손경제] 본방을 먼저, [플러스]를 뒤에 배치.)

━━━━━━━━━━━━━━━━━━━━━━
📌 [손경제 또는 손경제플러스] 에피소드 제목
━━━━━━━━━━━━━━━━━━━━━━

📎 방송 목차:
(제공된 '방송 목차' 텍스트를 그대로 포함. 뉴스 항목, 코너명, 출연자 등.)

📋 방송 내용 상세 요약
(2000~3000자. 방송 흐름 따라 핵심 논점별로 정리.
숫자·통계·전문가 발언 포함. 생략 없이 충실하게.
목차의 각 항목별로 구체적 내용을 풀어서 설명.)

💡 장례사업 관점 제언
(방송 내용에서 장례 산업과 직간접적으로 연결되는 포인트.
연결이 자연스러운 것만. 억지 연결 금지.
연결 포인트가 없으면 "직접 연결 포인트 없음"으로 표기하되,
사업가 관점에서 참고할 거시적 시사점은 언급.)
</BRIEFING>

<KEYWORDS>
핵심 키워드 3~5개.
</KEYWORDS>

<SUMMARY>
핵심 요약 3줄 이내.
</SUMMARY>"""

# --- 18:00 아시아 브리핑용 ---
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
#  Claude 분석 공통
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

    results = await asyncio.gather(
        # 뉴스 RSS
        run(collect_rss, "https://www.hani.co.kr/rss/", "한겨레", NEWS_KEYWORDS),
        run(collect_rss, "https://www.chosun.com/arc/outboundfeeds/rss/?outputType=xml", "조선일보", NEWS_KEYWORDS),
        run(collect_rss, "https://www.mk.co.kr/rss/30000001/", "매일경제", NEWS_KEYWORDS),
        run(collect_rss, "https://www.hankyung.com/feed/all-news", "한국경제", NEWS_KEYWORDS),
        run(collect_rss, "https://rss.joins.com/joins_news_list.xml", "중앙일보", NEWS_KEYWORDS),
        # 기관 게시판
        run(crawl_board, "https://www.kfcpi.or.kr/portal/home/bbs/list.do?menuId=M0001000600010000", "한국장례문화진흥원"),
        run(crawl_board, "https://www.kfcpi.or.kr/portal/home/bbs/list.do?menuId=M0001000700000000", "장례문화진흥원 보도자료"),
        run(crawl_board, "https://www.bok.or.kr/portal/bbs/P0000559/list.do?menuNo=200690", "한국은행"),
        run(crawl_board, "https://www.sangjomagazine.com", "상조매거진"),
        run(crawl_board, "https://kfda1024.or.kr/", "대한장례지도사협회"),
        run(crawl_board, "https://www.mohw.go.kr/board.es?mid=a10503010100&bid=0027", "보건복지부"),
        # 통계 — KOSIS RSS + 구글뉴스로 통계청 발표 잡기
        run(collect_rss,
            "https://news.google.com/rss/search?q=" + quote("통계청 OR 국가데이터처 인구동향 사망자수") + "&hl=ko&gl=KR&ceid=KR:ko",
            "통계청 발표", None, 5),
        run(collect_rss,
            "https://news.google.com/rss/search?q=" + quote("KOSIS 고령화 1인가구") + "&hl=ko&gl=KR&ceid=KR:ko",
            "KOSIS 통계", None, 5),
        return_exceptions=True,
    )

    keys = ["hani", "chosun", "mk", "hankyung", "joins",
            "kfcpi", "kfcpi_press", "bok",
            "sangjo_mag", "kfda", "mohw",
            "kostat_news", "kosis_news"]
    parsed = {k: (v if isinstance(v, list) else []) for k, v in zip(keys, results)}

    # 에러 로깅
    for k, v in zip(keys, results):
        if isinstance(v, Exception):
            log.error(f"[수집 에러:{k}] {v}")

    bok_filtered = [i for i in parsed["bok"] if any(kw in i["title"] for kw in BOK_KEYWORDS)]

    raw: dict[str, list[dict]] = {
        "news": parsed["hani"] + parsed["chosun"] + parsed["mk"] + parsed["hankyung"] + parsed["joins"],
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

    # 수집 현황 모니터링
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

            # 원문 링크 모음
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

    except Exception as e:
        log.error(f"국내 브리핑 실패: {e}", exc_info=True)
        try:
            await send_telegram(f"⚠️ 국내 브리핑 생성 중 오류: {e}")
        except Exception:
            pass

    log.info("=== 국내 브리핑 완료 ===")


# ═══════════════════════════════════════════════════════════════════════════
#  작업 2: 11:00 손경제 브리핑
# ═══════════════════════════════════════════════════════════════════════════

async def run_soneconomy(is_retry: bool = False) -> bool:
    """손경제 브리핑. 성공 시 True, 수집 실패 시 False."""
    log.info(f"=== 손경제 브리핑 {'(재시도)' if is_retry else ''} 시작 ===")
    today_str = datetime.now(KST).strftime("%Y-%m-%d")
    date_kor = datetime.now(KST).strftime("%Y.%m.%d")
    day_kor = ["월", "화", "수", "목", "금", "토", "일"][datetime.now(KST).weekday()]

    loop = asyncio.get_running_loop()

    try:
        # 소스 1: 채널에서 플러스 수집
        channel_vids = await loop.run_in_executor(None, collect_youtube, YOUTUBE_CHANNEL_URL, 10)
        # 소스 2: 검색으로 [손경제] 본방 수집
        search_vids = await loop.run_in_executor(None, collect_youtube, YOUTUBE_SEARCH_MAIN, 5)

        # 합치고 중복 제거 (영상 ID 기준)
        seen_ids = set()
        youtube_vids = []
        for v in search_vids + channel_vids:
            vid_id = v.get("link", "").split("v=")[-1] if "v=" in v.get("link", "") else ""
            if vid_id and vid_id not in seen_ids:
                seen_ids.add(vid_id)
                youtube_vids.append(v)

        # 커피타임·협찬 제외
        youtube_vids = [v for v in youtube_vids
                        if "커피타임" not in v.get("title", "")
                        and "협찬" not in v.get("title", "")]

        # 최근 3일 이내 영상 필터
        date_cutoff = [(datetime.now(KST) - timedelta(days=i)).strftime("%Y%m%d") for i in range(3)]
        today_vids = [v for v in youtube_vids
                      if v.get("published", "").replace("-", "") in date_cutoff]

        # [손경제]와 [플러스] 분리
        son_main = [v for v in today_vids if "[손경제]" in v.get("title", "") and "플러스" not in v.get("title", "")]
        son_plus = [v for v in today_vids if v not in son_main]
        today_vids = son_main + son_plus  # 본방 먼저, 플러스 뒤에

        log.info(f"손경제: 본방 {len(son_main)}건, 플러스 {len(son_plus)}건")

        # 최대 4개로 제한 (본방 최대 2개 + 플러스 최대 2개)
        today_vids = son_main[:2] + son_plus[:2]

        if not today_vids and not is_retry:
            log.info("손경제: 오늘/어제 영상 없음 → 13시에 재시도")
            return False

        if not today_vids:
            # 재시도에서도 없으면 가장 최근 영상 사용
            today_vids = youtube_vids[:3] if youtube_vids else []

        if not today_vids:
            log.info("손경제: 수집 가능한 영상 없음")
            return True  # 더 이상 재시도 불필요

        # Claude 분석용 데이터 구성
        sections = []
        for v in today_vids:
            tag = "(자막)" if v.get("has_subtitle") else "(설명란)"
            desc = v.get("description", "")
            section = f"제목: {v['title']}\n링크: {v['link']}"
            if desc:
                section += f"\n📌 방송 목차:\n{desc}"
            section += f"\n내용 {tag}:\n{v['summary'][:6000]}"
            sections.append(section)

        raw_data = "\n\n---\n\n".join(sections)
        prompt = f"오늘({date_kor} {day_kor}요일) 손경제 방송 내용을 분석해주세요.\n\n{raw_data}"

        full_text = await claude_with_retry(prompt, SYSTEM_SONECONOMY, max_tokens=4096)
        briefing_text = _extract_tag("BRIEFING", full_text) or full_text
        keywords = _extract_tag("KEYWORDS", full_text)
        summary = _extract_tag("SUMMARY", full_text)

        await send_telegram(briefing_text)
        log.info("[Telegram] 손경제 브리핑 전송 완료")

        return True

    except Exception as e:
        log.error(f"손경제 브리핑 실패: {e}", exc_info=True)
        try:
            await send_telegram(f"⚠️ 손경제 브리핑 생성 중 오류: {e}")
        except Exception:
            pass
        return True  # 에러는 재시도 불필요


# 11시 → 실패 시 13시 재시도 관리
_soneconomy_done = False


async def run_soneconomy_11() -> None:
    global _soneconomy_done
    _soneconomy_done = await run_soneconomy(is_retry=False)


async def run_soneconomy_13() -> None:
    global _soneconomy_done
    if not _soneconomy_done:
        _soneconomy_done = await run_soneconomy(is_retry=True)
    else:
        log.info("손경제: 11시에 이미 완료 → 13시 스킵")


# ═══════════════════════════════════════════════════════════════════════════
#  작업 3: 18:00 아시아 브리핑
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

        # 원문 링크 모음 전송
        links_lines = ["📎 원문 링크"]
        for item in items:
            if item.get("link"):
                links_lines.append(f"• [{item['source']}] {item['title'][:40]}\n  {item['link']}")
        if len(links_lines) > 1:
            await send_telegram("\n".join(links_lines))

        log.info("[Telegram] 아시아 브리핑 전송 완료")

        try:
            data = {"asia": items}
            tags = ["해외동향"] + extract_tags(data, keywords)
            await save_to_notion(today_str, keywords, briefing_text, data, tags[:10], "아시아")
            log.info("[Notion] 아시아 저장 완료")
        except Exception as e:
            log.error(f"[Notion] 아시아 저장 실패: {e}")

    except Exception as e:
        log.error(f"아시아 브리핑 실패: {e}", exc_info=True)

    log.info("=== 아시아 브리핑 완료 ===")


# ═══════════════════════════════════════════════════════════════════════════
#  작업 4: 일요일 20:00 주간 아이디어 다이제스트
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

        # 노션 대화 DB 조회
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

        # 페이지 텍스트 추출
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

        # 기간 표시
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
#  스케줄러 / 메인
# ═══════════════════════════════════════════════════════════════════════════

async def main() -> None:
    await setup_notion_db()

    scheduler = AsyncIOScheduler(timezone=KST)

    scheduler.add_job(run_domestic, trigger="cron", hour=8, minute=0,
                      id="domestic", name="08:00 국내 브리핑", misfire_grace_time=300)

    scheduler.add_job(run_soneconomy_11, trigger="cron", hour=11, minute=0,
                      id="son_11", name="11:00 손경제 브리핑", misfire_grace_time=300)

    scheduler.add_job(run_soneconomy_13, trigger="cron", hour=13, minute=0,
                      id="son_13", name="13:00 손경제 재시도", misfire_grace_time=300)

    scheduler.add_job(run_asia, trigger="cron", hour=18, minute=0,
                      id="asia", name="18:00 아시아 브리핑", misfire_grace_time=300)

    # 매주 일요일 20:00 KST 주간 다이제스트
    scheduler.add_job(run_weekly_digest, trigger="cron", day_of_week="sun", hour=20, minute=0,
                      id="digest", name="일요일 20:00 주간 다이제스트", misfire_grace_time=300)

    scheduler.start()
    log.info("브리핑 봇 v2 시작")
    log.info(f"  08:00 국내 | 11:00 손경제 | 13:00 재시도 | 18:00 아시아 | 일 20:00 다이제스트")

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
    elif "--son" in sys.argv:
        log.info("즉시 실행: 손경제 브리핑")
        asyncio.run(run_soneconomy(is_retry=True))
    elif "--asia" in sys.argv:
        log.info("즉시 실행: 아시아 브리핑")
        asyncio.run(run_asia())
    elif "--digest" in sys.argv:
        log.info("즉시 실행: 주간 다이제스트")
        asyncio.run(run_weekly_digest())
    else:
        asyncio.run(main())
