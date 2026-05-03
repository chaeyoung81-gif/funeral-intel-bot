"""
KOSIS 통계 인프라 수집기 (Funeral Intel Bot — Research)

위치: funeral-intel-bot/research/01_market_observation/02_collector.py

용도:
    장례 시장 관측을 위한 핵심 7개 지표를 KOSIS API로 수집해 CSV로 저장한다.
    분기 1회 실행 (수동 또는 cron). 매일 돌리는 텔레그램 봇과 별개.

실행:
    # 1단계: 통계표 ID 검증 (첫 실행 시 필수)
    python 02_collector.py --verify

    # 2단계: 실제 수집
    python 02_collector.py --collect

    # 특정 지표만 수집
    python 02_collector.py --collect --only deaths_by_region

    # 통계표 ID를 모를 때 검색
    python 02_collector.py --search "사망 장소별"

요구 패키지:
    pip install PublicDataReader python-dotenv pandas

환경변수 (.env):
    KOSIS_API_KEY=발급받은_인증키
"""

import os
import sys
import argparse
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

try:
    from PublicDataReader import Kosis
except ImportError:
    print("[ERR] PublicDataReader 미설치. 다음 명령으로 설치:")
    print("      pip install PublicDataReader python-dotenv pandas")
    sys.exit(1)

# ---------------------------------------------------------------------------
# 환경 설정
# ---------------------------------------------------------------------------
load_dotenv()
SERVICE_KEY = os.getenv("KOSIS_API_KEY")
if not SERVICE_KEY:
    print("[ERR] .env에 KOSIS_API_KEY가 없음. 다음 형식으로 추가:")
    print("      KOSIS_API_KEY=발급받은_인증키")
    sys.exit(1)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
TODAY = datetime.now().strftime("%Y-%m-%d")
SNAPSHOT_DIR = DATA_DIR / "snapshots" / TODAY
DATA_DIR.mkdir(parents=True, exist_ok=True)
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

api = Kosis(SERVICE_KEY)


# ---------------------------------------------------------------------------
# 통계표 매핑 (01_table_ids.md 기준)
# ---------------------------------------------------------------------------
# 각 지표는 "tblId 추정" + "검색 키워드"를 둘 다 갖는다.
# 첫 실행 검증에서 tblId가 살아있는지 확인하고, 실패하면 키워드로 재탐색.
TABLES = {
    "deaths_by_region": {
        "name": "지표1: 시도별 사망자수 (인구동향)",
        "orgId": "101",
        "tblId": "DT_1B8000G",
        "prdSe": "Y",
        "startPrdDe": "2010",
        "search_keyword": "인구동향 사망",
        "objL1": "ALL",  # 시도
        "itmId": "ALL",  # 항목 (사망자수 등)
    },
    "deaths_by_age": {
        "name": "지표2: 연령별 사망자수 (사망원인통계)",
        "orgId": "101",
        "tblId": "DT_1B34E01",
        "prdSe": "Y",
        "startPrdDe": "2010",
        "search_keyword": "사망원인 연령 5세별 사망자수",
        "objL1": "ALL",
        "itmId": "ALL",
    },
    "deaths_by_location": {
        "name": "지표3: 사망 장소별 사망자수",
        "orgId": "101",
        "tblId": "DT_1B34E14",  # 추정
        "prdSe": "Y",
        "startPrdDe": "2010",
        "search_keyword": "사망 장소별 사망자수",
        "objL1": "ALL",
        "itmId": "ALL",
    },
    "single_households": {
        "name": "지표4: 1인가구 추이 (인구총조사)",
        "orgId": "101",
        "tblId": "DT_1JC1517",  # 추정
        "prdSe": "Y",
        "startPrdDe": "2010",
        "search_keyword": "1인가구 연령 시도",
        "objL1": "ALL",
        "itmId": "ALL",
    },
    "elderly_households": {
        "name": "지표5: 고령자 가구 구성 (독거노인·무배우자)",
        "orgId": "101",
        "tblId": "DT_1JC1518",  # 추정
        "prdSe": "Y",
        "startPrdDe": "2010",
        "search_keyword": "고령자 1인가구",
        "objL1": "ALL",
        "itmId": "ALL",
    },
    "future_deaths": {
        "name": "지표6: 장래인구추계 - 장래 사망자수",
        "orgId": "101",
        "tblId": "DT_1BPA001",
        "prdSe": "Y",
        "startPrdDe": "2025",
        "endPrdDe": "2070",
        "search_keyword": "장래인구추계 사망자수",
        "objL1": "ALL",
        "itmId": "ALL",
    },
    "life_table": {
        "name": "지표7: 생명표 - 연령별 기대여명",
        "orgId": "101",
        "tblId": "DT_1B41",  # 시리즈 — 검증 필요
        "prdSe": "Y",
        "startPrdDe": "2010",
        "search_keyword": "완전생명표 기대여명",
        "objL1": "ALL",
        "itmId": "ALL",
    },
}


# ---------------------------------------------------------------------------
# 헬퍼 함수
# ---------------------------------------------------------------------------
def safe_get(category, **kwargs):
    """API 호출 안전 래퍼. 실패 시 None 반환."""
    try:
        return api.get_data(category, **kwargs)
    except Exception as e:
        print(f"  [API ERR] {e}")
        return None


def search_table(keyword, max_rows=10):
    """KOSIS 통합검색으로 통계표 ID 후보 출력."""
    print(f"\n[검색] '{keyword}'")
    df = safe_get("KOSIS통합검색", searchNm=keyword)
    if df is None or df.empty:
        print("  결과 없음")
        return None
    cols = [c for c in ["TBL_ID", "ORG_ID", "TBL_NM"] if c in df.columns]
    head = df[cols].head(max_rows)
    print(head.to_string(index=False))
    return head


def verify_table(key):
    """통계표 ID가 실제 살아있는지 확인."""
    cfg = TABLES[key]
    print(f"\n--- {cfg['name']} ---")
    print(f"  설정 tblId: {cfg['tblId']}")

    # 통계표 명칭 조회로 ID 살아있는지 확인
    title = safe_get(
        "통계표설명",
        "통계표명칭",
        orgId=cfg["orgId"],
        tblId=cfg["tblId"],
    )
    if title is not None and not title.empty:
        print(f"  ✓ ID 유효. 통계표명: {title.iloc[0].to_dict()}")
        return True
    else:
        print(f"  ✗ ID 응답 없음 — 통합검색으로 후보 출력:")
        search_table(cfg["search_keyword"])
        return False


def collect_one(key):
    """단일 지표 수집."""
    cfg = TABLES[key]
    print(f"\n[수집] {cfg['name']}")
    print(f"  tblId={cfg['tblId']}, period={cfg.get('startPrdDe', '2010')}~"
          f"{cfg.get('endPrdDe', '최신')}")

    kwargs = dict(
        orgId=cfg["orgId"],
        tblId=cfg["tblId"],
        objL1=cfg.get("objL1", "ALL"),
        itmId=cfg.get("itmId", "ALL"),
        prdSe=cfg.get("prdSe", "Y"),
        startPrdDe=cfg.get("startPrdDe", "2010"),
    )
    if "endPrdDe" in cfg:
        kwargs["endPrdDe"] = cfg["endPrdDe"]
    # objL2, objL3가 있으면 추가
    for k in ("objL2", "objL3"):
        if k in cfg:
            kwargs[k] = cfg[k]

    df = safe_get("통계자료", **kwargs)
    if df is None or df.empty:
        print(f"  ✗ 응답 없음 또는 빈 데이터프레임")
        return None

    # 저장
    out_main = DATA_DIR / f"{key}.csv"
    out_snap = SNAPSHOT_DIR / f"{key}.csv"
    df.to_csv(out_main, index=False, encoding="utf-8-sig")
    df.to_csv(out_snap, index=False, encoding="utf-8-sig")
    print(f"  ✓ {len(df):,} rows → {out_main.name} (+ snapshot)")
    return df


def write_run_log(results):
    """실행 결과를 JSON 로그로 남김."""
    log_file = SNAPSHOT_DIR / "run_log.json"
    log = {
        "run_at": datetime.now().isoformat(),
        "results": results,
    }
    with open(log_file, "w", encoding="utf-8") as f:
        json.dump(log, f, ensure_ascii=False, indent=2)
    print(f"\n[로그] {log_file}")


# ---------------------------------------------------------------------------
# 메인 진입점
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="KOSIS 7개 지표 수집기")
    parser.add_argument("--verify", action="store_true",
                        help="통계표 ID 검증 모드 (첫 실행)")
    parser.add_argument("--collect", action="store_true",
                        help="실제 수집 실행")
    parser.add_argument("--search", type=str,
                        help="키워드로 통계표 ID 검색")
    parser.add_argument("--only", type=str,
                        help="특정 지표 키만 (예: deaths_by_region)")
    args = parser.parse_args()

    if args.search:
        search_table(args.search, max_rows=20)
        return

    if not (args.verify or args.collect):
        parser.print_help()
        print("\n실행 모드 중 하나를 선택하세요: --verify 또는 --collect")
        return

    keys = [args.only] if args.only else list(TABLES.keys())
    if args.only and args.only not in TABLES:
        print(f"[ERR] '{args.only}' 지표 키 미등록. 사용 가능: {list(TABLES.keys())}")
        return

    print(f"=== KOSIS 수집기 ({TODAY}) ===")
    print(f"모드: {'VERIFY' if args.verify else 'COLLECT'}")
    print(f"대상: {len(keys)}개 지표")

    results = {}

    if args.verify:
        for key in keys:
            ok = verify_table(key)
            results[key] = "OK" if ok else "FAIL"
        ok_n = sum(1 for v in results.values() if v == "OK")
        print(f"\n=== 검증 결과: {ok_n}/{len(keys)} 통과 ===")
        for k, v in results.items():
            print(f"  {v:5s} {k}")

    if args.collect:
        for key in keys:
            df = collect_one(key)
            results[key] = {
                "rows": len(df) if df is not None else 0,
                "status": "OK" if df is not None and not df.empty else "FAIL",
            }
        write_run_log(results)
        ok_n = sum(1 for v in results.values() if v["status"] == "OK")
        print(f"\n=== 수집 결과: {ok_n}/{len(keys)} 성공 ===")


if __name__ == "__main__":
    main()
