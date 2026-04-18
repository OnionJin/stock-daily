#!/usr/bin/env python3
"""
Taiwan Stock Daily Scraper
TWSE (上市) + TPEX (上櫃) → docs/data/YYYY-MM-DD.csv
"""

import argparse
import glob
import json
import os
import shutil
import sys
import time
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import pandas as pd
import requests

# ── Config ────────────────────────────────────────────────────────────────────
TW_TZ      = timezone(timedelta(hours=8))
DATA_DIR   = "docs/data"
CACHE_FILE = os.path.join(DATA_DIR, "prices_cache.json")
LOOKBACK   = 80   # trading days back (60MA needs 60 + slope buffer + safety)
HEADERS    = {"User-Agent": "Mozilla/5.0 (compatible; stock-daily-bot/1.0)"}

# ── Date helpers ──────────────────────────────────────────────────────────────

def tw_today() -> date:
    return datetime.now(TW_TZ).date()

def prev_weekdays(from_date: date, n: int) -> list:
    """n weekday dates strictly before from_date, most-recent first."""
    result, cur = [], from_date - timedelta(days=1)
    while len(result) < n:
        if cur.weekday() < 5:
            result.append(cur)
        cur -= timedelta(days=1)
    return result

def ds(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def clean(s) -> float:
    try:
        return float(str(s).replace(",", "").replace("+", "").strip())
    except Exception:
        return float("nan")

# ── API fetchers ──────────────────────────────────────────────────────────────

def fetch_twse(d: date) -> Optional[list]:
    """Fetch TWSE STOCK_DAY_ALL for a given date."""
    url = (
        "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL"
        f"?response=json&date={d.strftime('%Y%m%d')}"
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        j = r.json()
    except Exception as e:
        print(f"  TWSE {d}: {e}")
        return None

    if j.get("stat") != "OK" or not j.get("data"):
        return None

    out = []
    for row in j["data"]:
        # fields: 證券代號, 證券名稱, 成交股數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 漲跌幅(%)
        if len(row) < 9:
            continue
        close = clean(row[7])
        if pd.isna(close) or close <= 0:
            continue
        vol = clean(row[2])
        amt = clean(row[3])
        out.append({
            "symbol":     row[0].strip(),
            "name":       row[1].strip(),
            "close":      close,
            "change_pct": clean(row[8]),
            "volume":     int(vol) if not pd.isna(vol) else 0,
            "amount":     int(amt) if not pd.isna(amt) else 0,
        })
    return out or None


def fetch_tpex(d: date) -> Optional[list]:
    """Fetch TPEX OTC quotes for a given date."""
    roc       = d.year - 1911
    tpex_date = f"{roc}/{d.month:02d}/{d.day:02d}"
    url = (
        "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/"
        f"stk_wn1430_result.php?l=zh-tw&d={tpex_date}&se=EW"
    )
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        j = r.json()
    except Exception as e:
        print(f"  TPEX {d}: {e}")
        return None

    raw = j.get("aaData", [])
    if not raw:
        return None

    out = []
    for row in raw:
        # fields: 代號, 名稱, 收盤, 漲跌(abs), 開盤, 最高, 最低,
        #         成交股數(千股), 成交金額(千元), ...
        if len(row) < 9:
            continue
        close = clean(row[2])
        if pd.isna(close) or close <= 0:
            continue
        chg  = clean(row[3])
        prev = close - chg
        chg_pct = (chg / prev * 100) if (not pd.isna(chg) and prev != 0) else float("nan")
        vol_k = clean(row[7])
        amt_k = clean(row[8])
        out.append({
            "symbol":     row[0].strip(),
            "name":       row[1].strip(),
            "close":      close,
            "change_pct": chg_pct,
            "volume":     int(vol_k * 1000) if not pd.isna(vol_k) else 0,
            "amount":     int(amt_k * 1000) if not pd.isna(amt_k) else 0,
        })
    return out or None


def fetch_combined(d: date) -> Optional[list]:
    if d.weekday() >= 5:
        return None
    twse = fetch_twse(d)
    time.sleep(0.5)
    tpex = fetch_tpex(d)
    combined = (twse or []) + (tpex or [])
    return combined or None

# ── Cache management ──────────────────────────────────────────────────────────

def load_cache() -> dict:
    """Load {date_str: {symbol: [close, volume]}} from cache file."""
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_cache(cache: dict, keep_from: str):
    """Persist cache, pruning entries older than keep_from date string."""
    pruned = {d: v for d, v in cache.items() if d >= keep_from}
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(pruned, f, ensure_ascii=False, separators=(",", ":"))


def cache_to_hist(cache: dict):
    """Convert cache dict to (close_hist, vol_hist) lookup dicts."""
    close_hist: dict = {}
    vol_hist:   dict = {}
    for date_str, stocks in cache.items():
        for sym, cv in stocks.items():
            close_hist.setdefault(sym, {})[date_str] = cv[0]
            vol_hist.setdefault(sym, {})[date_str]   = cv[1]
    return close_hist, vol_hist


def add_records_to_cache(cache: dict, close_hist: dict, vol_hist: dict,
                          records: list, date_str: str):
    """Add a fetched day's records into both cache and history dicts."""
    cache[date_str] = {rec["symbol"]: [rec["close"], rec["volume"]] for rec in records}
    for rec in records:
        sym = rec["symbol"]
        close_hist.setdefault(sym, {})[date_str] = rec["close"]
        vol_hist.setdefault(sym, {})[date_str]   = rec["volume"]

# ── Technical indicators ──────────────────────────────────────────────────────

def calc_ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()


def macd_bar_state(prices: list) -> str:
    """Classify today's MACD histogram bar state."""
    if len(prices) < 27:
        return ""
    s   = pd.Series(prices, dtype=float)
    dif = calc_ema(s, 12) - calc_ema(s, 26)
    dea = calc_ema(dif, 9)
    bar = dif - dea
    if len(bar) < 2:
        return ""
    t, p = bar.iloc[-1], bar.iloc[-2]
    if p > 0 and t < 0: return "紅轉綠"
    if p < 0 and t > 0: return "綠轉紅"
    if t > 0: return "紅增" if t >= p else "紅縮"
    if t < 0: return "綠增" if t <= p else "綠縮"
    return ""


def compute_indicators(sym: str, today_ds: str,
                        close_hist: dict, vol_hist: dict,
                        all_dates: list, today_vol: int) -> dict:
    empty = {k: "" for k in [
        "macd_state", "vol_surge", "above60",
        "ma5", "ma10", "ma20", "ma60",
        "slope5", "slope10", "slope20",
    ]}
    ch = close_hist.get(sym, {})
    prices = [ch[d] for d in all_dates if d in ch]
    n = len(prices)
    if n == 0:
        return empty

    s = pd.Series(prices, dtype=float)

    def ma(p):
        return float(s.rolling(p).mean().iloc[-1]) if n >= p else float("nan")

    def ma_5ago(p):
        # MA value 5 trading-days ago: use all prices except last 5
        if n < p + 5:
            return float("nan")
        return float(pd.Series(prices[:-5]).rolling(p).mean().iloc[-1])

    def slope(cur, ago):
        if pd.isna(cur) or pd.isna(ago) or ago == 0:
            return float("nan")
        return (cur - ago) / ago * 100

    def fmt(v):
        return round(v, 2) if not pd.isna(v) else ""

    def fmt_slope(v):
        return round(v, 4) if not pd.isna(v) else ""

    ma5, ma10, ma20, ma60 = ma(5), ma(10), ma(20), ma(60)

    # Volume surge: today >= avg of previous 5 trading days × 1.5
    vh    = vol_hist.get(sym, {})
    prev5 = [vh[d] for d in all_dates if d < today_ds and d in vh][-5:]
    if len(prev5) >= 3:
        avg5      = sum(prev5) / len(prev5)
        vol_surge = "是" if (avg5 > 0 and today_vol >= avg5 * 1.5) else "否"
    else:
        vol_surge = ""

    above60 = ""
    if not pd.isna(ma60):
        above60 = "是" if prices[-1] >= ma60 else "否"

    return {
        "macd_state": macd_bar_state(prices),
        "vol_surge":  vol_surge,
        "above60":    above60,
        "ma5":    fmt(ma5),   "ma10":  fmt(ma10),
        "ma20":   fmt(ma20),  "ma60":  fmt(ma60),
        "slope5":  fmt_slope(slope(ma5,  ma_5ago(5))),
        "slope10": fmt_slope(slope(ma10, ma_5ago(10))),
        "slope20": fmt_slope(slope(ma20, ma_5ago(20))),
    }

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="指定日期 YYYY-MM-DD（預設為今天）")
    args = parser.parse_args()

    if args.date:
        try:
            today = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            print(f"日期格式錯誤，請用 YYYY-MM-DD，例如：--date 2026-04-17")
            sys.exit(1)
        print(f"(手動指定日期: {args.date})")
    else:
        today = tw_today()

    today_ds_str = ds(today)

    if today.weekday() >= 5:
        print(f"Weekend ({today_ds_str}). Skipping.")
        sys.exit(0)

    print(f"=== Taiwan Stock Scraper: {today_ds_str} ===")

    # 1. Fetch today's data
    print("Fetching TWSE...")
    twse = fetch_twse(today)
    time.sleep(0.5)
    print("Fetching TPEX...")
    tpex = fetch_tpex(today)

    today_records = (twse or []) + (tpex or [])
    if not today_records:
        print("No data returned — likely a holiday. Skipping.")
        sys.exit(0)
    print(f"  {len(today_records)} stocks fetched.")

    # 2. Load historical cache
    os.makedirs(DATA_DIR, exist_ok=True)
    print("Loading history cache...")
    cache = load_cache()
    close_hist, vol_hist = cache_to_hist(cache)

    # Add today to cache + history
    add_records_to_cache(cache, close_hist, vol_hist, today_records, today_ds_str)

    # 3. Bootstrap missing history dates
    needed  = prev_weekdays(today, LOOKBACK)
    covered = set(cache.keys())
    missing = [d for d in needed if ds(d) not in covered]

    if missing:
        print(f"Bootstrapping {len(missing)} missing history dates...")
        for i, d in enumerate(missing):
            d_str = ds(d)
            print(f"  [{i+1}/{len(missing)}] {d_str}")
            recs = fetch_combined(d)
            time.sleep(0.3)
            if recs:
                add_records_to_cache(cache, close_hist, vol_hist, recs, d_str)

    # 4. Build sorted date list used for indicator computation
    cutoff    = ds(needed[-1]) if needed else "2000-01-01"
    all_dates = sorted(d for d in cache if d >= cutoff and d <= today_ds_str)
    print(f"History spans {len(all_dates)} trading days.")

    # 5. Compute indicators and build output rows
    print("Computing indicators...")
    rows = []
    for rec in today_records:
        sym = rec["symbol"]
        ind = compute_indicators(
            sym, today_ds_str, close_hist, vol_hist, all_dates, rec["volume"]
        )
        cp = rec["change_pct"]
        rows.append({
            "股票代號":        sym,
            "股票名稱":        rec["name"],
            "日期":            today_ds_str,
            "收盤價":          rec["close"],
            "漲跌幅(%)":       round(cp, 2) if not pd.isna(cp) else "",
            "成交量（股）":    rec["volume"],
            "成交金額（元）":  rec["amount"],
            "MACD狀態":        ind["macd_state"],
            "出量":            ind["vol_surge"],
            "站上60日均":      ind["above60"],
            "5日均價":         ind["ma5"],
            "10日均價":        ind["ma10"],
            "20日均價":        ind["ma20"],
            "60日均價":        ind["ma60"],
            "5日均斜率(%)":    ind["slope5"],
            "10日均斜率(%)":   ind["slope10"],
            "20日均斜率(%)":   ind["slope20"],
        })

    # 6. Write CSV (UTF-8 BOM)
    df       = pd.DataFrame(rows)
    out_path = os.path.join(DATA_DIR, f"{today_ds_str}.csv")
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"Written: {out_path}  ({len(df)} rows)")

    # 7. Update latest.csv
    shutil.copy(out_path, os.path.join(DATA_DIR, "latest.csv"))

    # 8. Update index.json
    all_csv_dates = sorted(
        os.path.basename(f).replace(".csv", "")
        for f in glob.glob(os.path.join(DATA_DIR, "????-??-??.csv"))
    )
    with open(os.path.join(DATA_DIR, "index.json"), "w", encoding="utf-8") as f:
        json.dump({"dates": all_csv_dates, "latest": today_ds_str},
                  f, ensure_ascii=False, indent=2)

    # 9. Prune and save cache
    keep_from = ds(needed[-1]) if needed else cutoff
    save_cache(cache, keep_from)
    print("Done.")


if __name__ == "__main__":
    main()
