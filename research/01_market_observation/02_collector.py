"""
KOSIS 통계 인프라 수집기 v3 (Funeral Intel Bot — Research)
위치: funeral-intel-bot/research/01_market_observation/02_collector.py

v3 변경: 모든 표의 objL 파라미터 실측 확정
    deaths_by_region  : objL1=ALL, objL2=15(사망자수), itmId=T1
    deaths_by_age     : objL1=0(전체원인계), objL2=ALL(연령), objL3=ALL(성), itmId=T1
    single_households : tblId=DT_1JC1501, objL1=00(전국), itmId=ALL
    future_deaths     : objL1=ALL, objL2=ALL, objL3=ALL
    life_table        : 396행 성공 (파라미터 그대로)

수동 다운로드:
    deaths_by_location (지표3): KOSIS API 표 없음
    → https://kostat.go.kr 사망원인통계 연보 (매년 9월)

실행:
    python3 02_collector.py --verify
    python3 02_collector.py --collect
    python3 02_collector.py --collect --only deaths_by_age
"""

import os, sys, argparse, json
from datetime import datetime
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv

try:
    from PublicDataReader import Kosis
except ImportError:
    print("[ERR] pip3 install PublicDataReader python-dotenv pandas")
    sys.exit(1)

load_dotenv(Path.home() / "funeral-intel-bot" / ".env")
SERVICE_KEY = os.getenv("KOSIS_API_KEY")
if not SERVICE_KEY:
    print("[ERR] .env에 KOSIS_API_KEY 없음")
    sys.exit(1)

BASE_DIR   = Path(__file__).resolve().parent
DATA_DIR   = BASE_DIR / "data"
TODAY      = datetime.now().strftime("%Y-%m-%d")
SNAP_DIR   = DATA_DIR / "snapshots" / TODAY
DATA_DIR.mkdir(parents=True, exist_ok=True)
SNAP_DIR.mkdir(parents=True, exist_ok=True)

api = Kosis(SERVICE_KEY)

# ---------------------------------------------------------------------------
# 통계표 매핑 — 실측 파라미터 확정
# ---------------------------------------------------------------------------
TABLES = {
    "deaths_by_region": {
        "name"    : "지표1: 시도별 사망자수 (인구동향)",
        "orgId"   : "101",
        "tblId"   : "DT_1B8000G",
        "objL1"   : "ALL",     # 시도
        "objL2"   : "15",      # 종류별=사망자수(명)
        "itmId"   : "T1",      # 출생사망혼인이혼
        "prdSe"   : "Y",
        "startPrdDe": "2010",
        "endPrdDe"  : "2025",
    },
    "deaths_by_age": {
        "name"    : "지표2: 연령별 사망자수 (사망원인=전체, 성별 포함)",
        "orgId"   : "101",
        "tblId"   : "DT_1B34E01",
        "objL1"   : "0",       # 사망원인=계(전체) — 원인별 104항목 제외해 셀수 축소
        "objL2"   : "ALL",     # 연령(5세)
        "objL3"   : "ALL",     # 성별
        "itmId"   : "T1",      # 사망자수만
        "prdSe"   : "Y",
        "startPrdDe": "2010",
        "endPrdDe"  : "2024",
    },
    # 지표3 deaths_by_location: API 없음 → 수동
    "single_households": {
        "name"    : "지표4+5: 전국 가구형태별 가구 (1인가구·고령가구 겸용)",
        "orgId"   : "101",
        "tblId"   : "DT_1JC1501",
        "objL1"   : "00",      # 전국 (시도별 확장은 셀수 초과 위험)
        "itmId"   : "ALL",
        "prdSe"   : "Y",
        "startPrdDe": "2010",
        "endPrdDe"  : "2025",
        "note"    : "항목 T2100=일반가구, 수록주기 매년. 1인가구 추이는 항목 필터로 추출",
    },
    "future_deaths": {
        "name"    : "지표6: 장래인구추계 — 성·연령별 추계인구",
        "orgId"   : "101",
        "tblId"   : "DT_1BPA001",
        "objL1"   : "ALL",
        "objL2"   : "ALL",
        "objL3"   : "ALL",
        "itmId"   : "ALL",
        "prdSe"   : "Y",
        "startPrdDe": "2025",
        "endPrdDe"  : "2070",
    },
    "life_table": {
        "name"    : "지표7: 생명표 — 연령별 기대여명",
        "orgId"   : "101",
        "tblId"   : "DT_1B41",
        "objL1"   : "ALL",
        "itmId"   : "ALL",
        "prdSe"   : "Y",
        "startPrdDe": "2010",
        "endPrdDe"  : "2024",
    },
}

MANUAL = {
    "deaths_by_location": {
        "name"  : "지표3: 사망 장소별 사망자수 (수동 다운로드)",
        "url"   : "https://kostat.go.kr → 사망원인통계 연보 (매년 9월)",
        "저장처" : "data/deaths_by_location.csv",
    }
}

# ---------------------------------------------------------------------------
def safe_get(*args, **kwargs):
    try:
        return api.get_data(*args, **kwargs)
    except Exception as e:
        print(f"  [ERR] {e}")
        return None

def build_kwargs(cfg):
    kw = dict(orgId=cfg["orgId"], tblId=cfg["tblId"],
              itmId=cfg.get("itmId","ALL"),
              prdSe=cfg.get("prdSe","Y"),
              startPrdDe=cfg.get("startPrdDe","2010"))
    for k in ("objL1","objL2","objL3","objL4"):
        if k in cfg:
            kw[k] = cfg[k]
    if "endPrdDe" in cfg:
        kw["endPrdDe"] = cfg["endPrdDe"]
    return kw

def verify_table(key):
    cfg = TABLES[key]
    print(f"\n--- {cfg['name']} ---")
    r = safe_get("통계표설명","통계표명칭", orgId=cfg["orgId"], tblId=cfg["tblId"])
    if r is not None and not r.empty:
        print(f"  OK  {r.iloc[0]['통계표명']}")
        if "note" in cfg:
            print(f"  ℹ  {cfg['note']}")
        return True
    print(f"  FAIL — tblId={cfg['tblId']}")
    return False

def collect_one(key):
    cfg = TABLES[key]
    print(f"\n[수집] {cfg['name']}")
    df = safe_get("통계자료", **build_kwargs(cfg))
    if df is None or df.empty:
        print("  FAIL: 데이터 없음")
        return None
    out  = DATA_DIR / f"{key}.csv"
    snap = SNAP_DIR  / f"{key}.csv"
    df.to_csv(out,  index=False, encoding="utf-8-sig")
    df.to_csv(snap, index=False, encoding="utf-8-sig")
    print(f"  OK  {len(df):,} rows → {out.name}")
    return df

def print_manual():
    print("\n" + "="*50)
    print("[ 수동 다운로드 필요 ]")
    for k, v in MANUAL.items():
        print(f"  {v['name']}")
        print(f"  URL : {v['url']}")
        print(f"  저장: {v['저장처']}")
    print("="*50)

def write_log(results):
    f = SNAP_DIR / "run_log.json"
    json.dump({"run_at": datetime.now().isoformat(), "results": results},
              open(f,"w",encoding="utf-8"), ensure_ascii=False, indent=2)
    print(f"[로그] {f}")

# ---------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--verify",  action="store_true")
    p.add_argument("--collect", action="store_true")
    p.add_argument("--search",  type=str)
    p.add_argument("--only",    type=str)
    args = p.parse_args()

    if args.search:
        r = safe_get("KOSIS통합검색", searchNm=args.search)
        print(r[["TBL_ID","TBL_NM"]].head(15).to_string(index=False) if r is not None and not r.empty else "결과 없음")
        return

    if not (args.verify or args.collect):
        p.print_help(); return

    keys = [args.only] if args.only else list(TABLES.keys())
    if args.only and args.only not in TABLES:
        print(f"[ERR] 가능 키: {list(TABLES.keys())}"); return

    print(f"=== KOSIS 수집기 v3 ({TODAY}) | {'VERIFY' if args.verify else 'COLLECT'} | {len(keys)}개 ===")
    results = {}

    if args.verify:
        for k in keys:
            results[k] = "OK" if verify_table(k) else "FAIL"
        ok = sum(v=="OK" for v in results.values())
        print(f"\n=== {ok}/{len(keys)} 통과 ===")
        for k,v in results.items(): print(f"  {v:4s}  {k}")
        print_manual()

    if args.collect:
        for k in keys:
            df = collect_one(k)
            results[k] = {"rows": len(df) if df is not None else 0,
                          "status": "OK" if df is not None and not df.empty else "FAIL"}
        write_log(results)
        ok = sum(v["status"]=="OK" for v in results.values())
        print(f"\n=== {ok}/{len(keys)} 성공 ===")
        print_manual()

if __name__ == "__main__":
    main()
