
# -*- coding: utf-8 -*-
"""
GTC AI Trading System v6.0.1 FULL-INTEGRATED-TRADING-FIX

功能：
- 股票主檔分類（市場 / 產業 / 題材）
- 本地 SQLite 歷史資料庫
- TWSE/TPEX 官方資料 + Yahoo Finance 備援更新
- 技術指標：MA / MACD / RSI / KD
- 排行榜 / 類股熱度 / 題材輪動
- AI 選股 TOP5
- Tkinter 桌面 UI
"""

import sqlite3
import traceback
import requests
import sys
import csv
import re
import threading
import time
import os
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

try:
    import yfinance as yf
except Exception:
    yf = None

import tkinter as tk
from tkinter import ttk, messagebox

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


APP_NAME = "GTC AI Trading System v6.0.1 FULL-INTEGRATED-TRADING-FIX"


def get_base_dir() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path(__file__).resolve().parent


def get_runtime_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


BASE_DIR = get_base_dir()
RUNTIME_DIR = get_runtime_dir()


PACKED_DATA_DIR = BASE_DIR / "data"
EXTERNAL_DATA_DIR = RUNTIME_DIR / "data"

DEFAULT_MASTER_CSV = """stock_id,stock_name,market,industry,theme,sub_theme,is_etf,is_active,update_date
2330,台積電,上市,半導體,AI/晶圓代工,高權值,0,1,2026-03-22
2454,聯發科,上市,半導體,IC設計,高權值,0,1,2026-03-22
2317,鴻海,上市,電子代工,AI伺服器,高權值,0,1,2026-03-22
3231,緯創,上市,電子代工,AI伺服器,伺服器,0,1,2026-03-22
2382,廣達,上市,電子代工,AI伺服器,伺服器,0,1,2026-03-22
6669,緯穎,上市,電子代工,AI伺服器,伺服器,0,1,2026-03-22
2308,台達電,上市,電源/電機,電源/HVDC,電源,0,1,2026-03-22
3017,奇鋐,上市,散熱,AI散熱,液冷,0,1,2026-03-22
3324,雙鴻,上市,散熱,AI散熱,液冷,0,1,2026-03-22
3596,智易,上市,網通,網通,寬頻,0,1,2026-03-22
2345,智邦,上市,網通,資料中心交換器,高階網通,0,1,2026-03-22
4979,華星光,上櫃,光通訊,CPO/光模組,高速光通訊,0,1,2026-03-22
3443,創意,上市,半導體,ASIC,AI ASIC,0,1,2026-03-22
6533,晶心科,上市,半導體,RISC-V,IP,0,1,2026-03-22
0050,元大台灣50,ETF,ETF,大型權值,ETF,1,1,2026-03-22
0056,元大高股息,ETF,ETF,高股息,ETF,1,1,2026-03-22
00919,群益台灣精選高息,ETF,ETF,高股息,ETF,1,1,2026-03-22
00929,復華台灣科技優息,ETF,ETF,科技高息,ETF,1,1,2026-03-22
"""

def ensure_external_master_csv() -> Path:
    EXTERNAL_DATA_DIR.mkdir(parents=True, exist_ok=True)
    external_csv = EXTERNAL_DATA_DIR / "stocks_master.csv"
    if not external_csv.exists():
        external_csv.write_text(DEFAULT_MASTER_CSV, encoding="utf-8-sig")
    return external_csv

def resolve_master_csv() -> Path:
    external_csv = EXTERNAL_DATA_DIR / "stocks_master.csv"
    packed_csv = PACKED_DATA_DIR / "stocks_master.csv"
    if external_csv.exists():
        return external_csv
    if packed_csv.exists():
        return packed_csv
    return ensure_external_master_csv()

DATA_DIR = EXTERNAL_DATA_DIR if (EXTERNAL_DATA_DIR / "stocks_master.csv").exists() else PACKED_DATA_DIR
CHART_DIR = RUNTIME_DIR / "charts"
CHART_DIR.mkdir(exist_ok=True)

DB_PATH = RUNTIME_DIR / "stock_system_v6_0_1.db"
MASTER_CSV = resolve_master_csv()


def normalize_csv_cell(v: str) -> str:
    s = str(v).strip().replace("=", "").strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1]
    return s.strip()


def parse_twse_mi_index_csv(csv_text: str) -> pd.DataFrame:
    rows = []
    for raw in csv_text.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith("=") and "證券代號" in line:
            line = line.replace("=", "")
        if not re.match(r'^[="]?\d{4}', line):
            continue
        try:
            cols = next(csv.reader([line]))
        except Exception:
            continue
        cols = [normalize_csv_cell(x) for x in cols]
        if len(cols) < 11:
            continue
        code = cols[0]
        if not (code.isdigit() and len(code) == 4):
            continue
        rows.append({
            "stock_id": code,
            "stock_name": cols[1] if len(cols) > 1 else "",
            "volume": cols[2] if len(cols) > 2 else "",
            "open": cols[5] if len(cols) > 5 else "",
            "high": cols[6] if len(cols) > 6 else "",
            "low": cols[7] if len(cols) > 7 else "",
            "close": cols[8] if len(cols) > 8 else "",
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    for c in ["volume", "open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", "", regex=False), errors="coerce")
    df = df.dropna(subset=["close"])
    df["date"] = datetime.now().strftime("%Y-%m-%d")
    df["turnover"] = df["close"] * df["volume"].fillna(0)
    return df[["stock_id", "date", "open", "high", "low", "close", "volume", "turnover"]].drop_duplicates(subset=["stock_id"])


def download_twse_official_daily_csv(date_str: str | None = None, fallback_days: int = 10) -> pd.DataFrame:
    base_date = datetime.strptime(date_str, "%Y%m%d") if date_str else datetime.now()
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.twse.com.tw/"}
    for offset in range(fallback_days + 1):
        use_date = (base_date - pd.Timedelta(days=offset)).strftime("%Y%m%d")
        url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date={use_date}&type=ALLBUT0999"
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            resp.raise_for_status()
            df = parse_twse_mi_index_csv(resp.text)
            if df is not None and not df.empty:
                return df
        except Exception:
            continue
    return pd.DataFrame()



def open_path(path: Path):
    try:
        path = Path(path)
        if sys.platform.startswith("win"):
            os.startfile(str(path))
        elif sys.platform == "darwin":
            subprocess.Popen(["open", str(path)])
        else:
            subprocess.Popen(["xdg-open", str(path)])
    except Exception:
        pass


def _normalize_master_df(df: pd.DataFrame, market_label: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["stock_id", "stock_name", "market", "industry", "theme", "sub_theme", "is_etf", "is_active", "update_date"])

    x = df.copy()
    rename_map = {
        "Code": "stock_id", "證券代號": "stock_id", "SecuritiesCompanyCode": "stock_id", "CompanyCode": "stock_id", "股票代號": "stock_id",
        "Name": "stock_name", "證券名稱": "stock_name", "CompanyName": "stock_name", "股票名稱": "stock_name",
    }
    x = x.rename(columns=rename_map)
    if "stock_id" not in x.columns:
        return pd.DataFrame(columns=["stock_id", "stock_name", "market", "industry", "theme", "sub_theme", "is_etf", "is_active", "update_date"])
    if "stock_name" not in x.columns:
        x["stock_name"] = x["stock_id"]

    x["stock_id"] = x["stock_id"].astype(str).str.strip()
    x["stock_name"] = x["stock_name"].astype(str).str.strip()
    x = x[x["stock_id"].str.fullmatch(r"\d{4}", na=False)].copy()
    if x.empty:
        return pd.DataFrame(columns=["stock_id", "stock_name", "market", "industry", "theme", "sub_theme", "is_etf", "is_active", "update_date"])

    x["market"] = market_label
    x["industry"] = "未分類"
    x["theme"] = "全市場"
    x["sub_theme"] = "系統掃描"
    x["is_etf"] = x["stock_id"].str.startswith("00").astype(int)
    x["is_active"] = 1
    x["update_date"] = datetime.now().strftime("%Y-%m-%d")

    name_series = x["stock_name"].fillna("").astype(str)
    etf_mask = x["is_etf"].eq(1) | name_series.str.contains("ETF|台灣50|高股息|科技優息|精選高息", regex=True)
    x.loc[etf_mask, ["market", "industry", "theme", "is_etf"]] = ["ETF", "ETF", "ETF", 1]
    semi_mask = name_series.str.contains("積電|聯發科|創意|晶心|半導體", regex=True)
    net_mask = name_series.str.contains("智邦|智易|光|網|通|聯亞", regex=True)
    power_mask = name_series.str.contains("台達|電|控|達", regex=True)
    x.loc[semi_mask & ~etf_mask, "industry"] = "半導體"
    x.loc[net_mask & ~etf_mask, "industry"] = "網通/光通訊"
    x.loc[power_mask & ~etf_mask, "industry"] = "電源/電機"

    return x[["stock_id", "stock_name", "market", "industry", "theme", "sub_theme", "is_etf", "is_active", "update_date"]].drop_duplicates(subset=["stock_id"]).reset_index(drop=True)


def fetch_twse_universe() -> pd.DataFrame:
    urls = [
        "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL",
        "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json",
    ]
    for url in urls:
        try:
            res = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            res.raise_for_status()
            data = res.json()
            if isinstance(data, dict):
                data = data.get("data") or data.get("records") or []
            df = pd.DataFrame(data)
            if not df.empty:
                return _normalize_master_df(df, "上市")
        except Exception:
            continue
    return pd.DataFrame()


def fetch_tpex_universe() -> pd.DataFrame:
    urls = [
        "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
        "https://www.tpex.org.tw/openapi/v1/tpex_esb_quotes",
    ]
    parts = []
    for url in urls:
        try:
            res = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
            res.raise_for_status()
            df = pd.DataFrame(res.json())
            if not df.empty:
                parts.append(_normalize_master_df(df, "上櫃"))
        except Exception:
            continue
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True).drop_duplicates(subset=["stock_id"]).reset_index(drop=True)


def build_full_market_universe() -> pd.DataFrame:
    twse = fetch_twse_universe()
    tpex = fetch_tpex_universe()
    all_df = pd.concat([twse, tpex], ignore_index=True)
    if all_df.empty:
        csv_path = resolve_master_csv()
        if csv_path.exists():
            x = pd.read_csv(csv_path, dtype={"stock_id": str}).fillna("")
            return _normalize_master_df(x, "上市")
        return pd.DataFrame()

    # 用既有 CSV 更細分類覆蓋
    try:
        csv_path = resolve_master_csv()
        if csv_path.exists():
            x = pd.read_csv(csv_path, dtype={"stock_id": str}).fillna("")
            x["stock_id"] = x["stock_id"].astype(str).str.strip()
            x = x[x["stock_id"].str.fullmatch(r"\d{4}", na=False)].copy()
            keep_cols = ["stock_id", "stock_name", "market", "industry", "theme", "sub_theme", "is_etf", "is_active", "update_date"]
            for c in keep_cols:
                if c not in x.columns:
                    x[c] = ""
            x = x[keep_cols].drop_duplicates(subset=["stock_id"]).set_index("stock_id")
            all_df = all_df.drop_duplicates(subset=["stock_id"]).set_index("stock_id")
            all_df.update(x)
            all_df = all_df.reset_index()
    except Exception:
        pass

    return all_df.drop_duplicates(subset=["stock_id"]).sort_values(["market", "industry", "stock_id"]).reset_index(drop=True)



class DBManager:
    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row

    def close(self):
        with self.lock:
            self.conn.close()

    def init_db(self):
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("""
            CREATE TABLE IF NOT EXISTS stocks_master (
                stock_id TEXT PRIMARY KEY,
                stock_name TEXT,
                market TEXT,
                industry TEXT,
                theme TEXT,
                sub_theme TEXT,
                is_etf INTEGER DEFAULT 0,
                is_active INTEGER DEFAULT 1,
                update_date TEXT
            )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS price_history (
                stock_id TEXT,
                date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                turnover REAL,
                PRIMARY KEY (stock_id, date)
            )
            """)
            cur.execute("""
            CREATE TABLE IF NOT EXISTS ranking_result (
                date TEXT,
                stock_id TEXT,
                momentum_score REAL,
                trend_score REAL,
                reversal_score REAL,
                volume_score REAL,
                risk_score REAL,
                ai_score REAL,
                total_score REAL,
                signal TEXT,
                action TEXT,
                rank_all INTEGER,
                rank_industry INTEGER,
                PRIMARY KEY (date, stock_id)
            )
            """)
            self.conn.commit()

    def import_master_csv(self, csv_path: Path):
        df = pd.read_csv(csv_path, dtype={"stock_id": str}).fillna("")
        self.import_master_df(df)

    def import_master_df(self, df: pd.DataFrame):
        x = df.copy().fillna("")
        required_defaults = {
            "stock_id": "", "stock_name": "", "market": "", "industry": "", "theme": "", "sub_theme": "",
            "is_etf": 0, "is_active": 1, "update_date": datetime.now().strftime("%Y-%m-%d"),
        }
        for col, default in required_defaults.items():
            if col not in x.columns:
                x[col] = default
        x["stock_id"] = x["stock_id"].astype(str).str.strip()
        x = x[x["stock_id"].str.fullmatch(r"\d{4}", na=False)].copy()
        x["is_etf"] = pd.to_numeric(x["is_etf"], errors="coerce").fillna(0).astype(int)
        x["is_active"] = pd.to_numeric(x["is_active"], errors="coerce").fillna(1).astype(int)
        x = x[["stock_id", "stock_name", "market", "industry", "theme", "sub_theme", "is_etf", "is_active", "update_date"]]
        with self.lock:
            x.to_sql("stocks_master", self.conn, if_exists="replace", index=False)
            self.conn.commit()

    def get_master(self) -> pd.DataFrame:
        with self.lock:
            return pd.read_sql_query(
                "SELECT * FROM stocks_master WHERE is_active=1 ORDER BY market, industry, stock_id",
                self.conn,
            )

    def get_stock_row(self, stock_id: str) -> Optional[pd.Series]:
        with self.lock:
            df = pd.read_sql_query("SELECT * FROM stocks_master WHERE stock_id=?", self.conn, params=[stock_id])
        if df.empty:
            return None
        return df.iloc[0]

    def upsert_price_history(self, stock_id: str, df: pd.DataFrame):
        if df is None or df.empty:
            return
        rows = []
        for _, r in df.iterrows():
            rows.append((
                stock_id,
                str(r["date"]),
                float(r["open"]) if pd.notna(r.get("open")) else None,
                float(r["high"]) if pd.notna(r.get("high")) else None,
                float(r["low"]) if pd.notna(r.get("low")) else None,
                float(r["close"]) if pd.notna(r.get("close")) else None,
                float(r["volume"]) if pd.notna(r.get("volume")) else None,
                float(r["turnover"]) if pd.notna(r.get("turnover")) else None,
            ))
        with self.lock:
            cur = self.conn.cursor()
            cur.executemany("""
                INSERT INTO price_history(stock_id, date, open, high, low, close, volume, turnover)
                VALUES(?,?,?,?,?,?,?,?)
                ON CONFLICT(stock_id, date) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    turnover=excluded.turnover
            """, rows)
            self.conn.commit()

    def get_price_history(self, stock_id: str) -> pd.DataFrame:
        with self.lock:
            return pd.read_sql_query(
                "SELECT * FROM price_history WHERE stock_id=? ORDER BY date",
                self.conn, params=[stock_id]
            )

    def get_price_history_count(self, stock_id: str) -> int:
        with self.lock:
            row = self.conn.cursor().execute("SELECT COUNT(*) FROM price_history WHERE stock_id=?", (stock_id,)).fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    def get_last_price_date(self) -> Optional[str]:
        with self.lock:
            row = self.conn.cursor().execute("SELECT MAX(date) FROM price_history").fetchone()
        return str(row[0]) if row and row[0] else None

    def replace_ranking(self, df: pd.DataFrame):
        today = datetime.now().strftime("%Y-%m-%d")
        with self.lock:
            cur = self.conn.cursor()
            cur.execute("DELETE FROM ranking_result WHERE date=?", (today,))
            self.conn.commit()
            df.to_sql("ranking_result", self.conn, if_exists="append", index=False)
            self.conn.commit()

    def get_latest_ranking(self) -> pd.DataFrame:
        q = """
        SELECT rr.*, sm.stock_name, sm.market, sm.industry, sm.theme
        FROM ranking_result rr
        JOIN stocks_master sm ON rr.stock_id = sm.stock_id
        WHERE rr.date = (SELECT MAX(date) FROM ranking_result)
        ORDER BY rr.rank_all ASC
        """
        with self.lock:
            return pd.read_sql_query(q, self.conn)


class DataEngine:
    def __init__(self, db: DBManager):
        self.db = db

    @staticmethod
    def yahoo_symbol(stock_id: str, market: str) -> str:
        if market in ("上市", "ETF"):
            return f"{stock_id}.TW"
        if market == "上櫃":
            return f"{stock_id}.TWO"
        return stock_id

    @staticmethod
    def _to_num(series: pd.Series) -> pd.Series:
        return pd.to_numeric(series.astype(str).str.replace(",", "", regex=False).str.strip(), errors="coerce")

    def fetch_twse_daily(self) -> pd.DataFrame:
        try:
            df = download_twse_official_daily_csv()
            if df is not None and not df.empty:
                return df
        except Exception:
            pass
        return pd.DataFrame()

    def fetch_tpex_daily(self) -> pd.DataFrame:
        urls = [
            "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes",
            "https://www.tpex.org.tw/openapi/v1/tpex_esb_quotes",
        ]
        parts = []
        for url in urls:
            try:
                res = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
                res.raise_for_status()
                data = res.json()
                df = pd.DataFrame(data)
                if df.empty:
                    continue
                rename_map = {
                    "SecuritiesCompanyCode": "stock_id", "CompanyCode": "stock_id", "股票代號": "stock_id", "證券代號": "stock_id",
                    "CompanyName": "stock_name", "股票名稱": "stock_name",
                    "Open": "open", "開盤價": "open",
                    "High": "high", "最高價": "high",
                    "Low": "low", "最低價": "low",
                    "Close": "close", "收盤價": "close",
                    "TradingShares": "volume", "成交股數": "volume", "成交數量": "volume", "Volume": "volume",
                }
                df = df.rename(columns=rename_map)
                required = ["stock_id", "open", "high", "low", "close", "volume"]
                if not all(c in df.columns for c in required):
                    continue
                df["stock_id"] = df["stock_id"].astype(str).str.strip()
                df = df[df["stock_id"].str.fullmatch(r"\d{4}", na=False)].copy()
                for c in ["open", "high", "low", "close", "volume"]:
                    df[c] = self._to_num(df[c])
                df = df.dropna(subset=["close"])
                if df.empty:
                    continue
                df["date"] = datetime.now().strftime("%Y-%m-%d")
                df["turnover"] = df["close"] * df["volume"]
                parts.append(df[["stock_id", "date", "open", "high", "low", "close", "volume", "turnover"]])
            except Exception:
                continue
        if not parts:
            return pd.DataFrame()
        return pd.concat(parts, ignore_index=True).drop_duplicates(subset=["stock_id"])
    def download_history(self, stock_id: str, market: str, period: str = "2y") -> pd.DataFrame:
        if yf is None:
            return pd.DataFrame()
        symbols = []
        primary = self.yahoo_symbol(stock_id, market)
        if primary:
            symbols.append(primary)
        if f"{stock_id}.TW" not in symbols:
            symbols.append(f"{stock_id}.TW")
        if f"{stock_id}.TWO" not in symbols:
            symbols.append(f"{stock_id}.TWO")
        seen = set()
        for symbol in symbols:
            if symbol in seen:
                continue
            seen.add(symbol)
            try:
                hist = yf.Ticker(symbol).history(period=period, auto_adjust=False)
                if hist is None or hist.empty:
                    continue
                hist = hist.rename(columns={
                    "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"
                }).reset_index()
                date_col = "Date" if "Date" in hist.columns else "Datetime"
                hist["date"] = pd.to_datetime(hist[date_col]).dt.strftime("%Y-%m-%d")
                hist["turnover"] = hist["close"] * hist["volume"]
                out = hist[["date", "open", "high", "low", "close", "volume", "turnover"]].copy()
                for c in ["open", "high", "low", "close", "volume", "turnover"]:
                    out[c] = pd.to_numeric(out[c], errors="coerce")
                out = out.dropna(subset=["close"])
                if not out.empty:
                    return out
            except Exception:
                continue
        return pd.DataFrame()

    def build_full_history(self, min_days: int = 240, batch_size: int = 20, sleep_sec: float = 1.0, progress_cb=None) -> Tuple[int, int]:
        master = self.db.get_master()
        if master.empty:
            return 0, 0
        success = 0
        rows = 0
        total = len(master)
        for idx, (_, row) in enumerate(master.iterrows(), start=1):
            stock_id = str(row["stock_id"])
            market = str(row["market"])
            existing = self.db.get_price_history_count(stock_id)
            if existing < min_days:
                hist_df = self.download_history(stock_id, market, period="2y")
                if not hist_df.empty:
                    self.db.upsert_price_history(stock_id, hist_df)
                    success += 1
                    rows += len(hist_df)
            if progress_cb:
                progress_cb(idx, total, stock_id)
            if idx % batch_size == 0:
                time.sleep(sleep_sec)
        return success, rows

    def update_incremental(self, progress_cb=None) -> Tuple[int, int]:
        master = self.db.get_master()
        if master.empty:
            return 0, 0

        twse_df = self.fetch_twse_daily()
        tpex_df = self.fetch_tpex_daily()

        official_map = {}
        if not twse_df.empty:
            for _, row in twse_df.iterrows():
                official_map[str(row["stock_id"])] = pd.DataFrame([row])
        if not tpex_df.empty:
            for _, row in tpex_df.iterrows():
                official_map[str(row["stock_id"])] = pd.DataFrame([row])

        success = 0
        rows = 0

        total = len(master)
        for idx, (_, row) in enumerate(master.iterrows(), start=1):
            stock_id = str(row["stock_id"])
            official_df = official_map.get(stock_id, pd.DataFrame())

            if not official_df.empty:
                self.db.upsert_price_history(stock_id, official_df)
                rows += len(official_df)
                success += 1

            if progress_cb:
                progress_cb(idx, total, stock_id)

        return success, rows
    @staticmethod
    def attach(df: pd.DataFrame) -> pd.DataFrame:
        x = df.copy()
        x["ma5"] = x["close"].rolling(5).mean()
        x["ma10"] = x["close"].rolling(10).mean()
        x["ma20"] = x["close"].rolling(20).mean()
        x["ma60"] = x["close"].rolling(60).mean()

        ema12 = x["close"].ewm(span=12, adjust=False).mean()
        ema26 = x["close"].ewm(span=26, adjust=False).mean()
        x["macd"] = ema12 - ema26
        x["macd_signal"] = x["macd"].ewm(span=9, adjust=False).mean()
        x["macd_hist"] = x["macd"] - x["macd_signal"]

        delta = x["close"].diff()
        up = delta.clip(lower=0)
        down = -delta.clip(upper=0)
        ma_up = up.ewm(com=13, adjust=False).mean()
        ma_down = down.ewm(com=13, adjust=False).mean()
        rs = ma_up / ma_down.replace(0, np.nan)
        x["rsi14"] = 100 - (100 / (1 + rs))

        low_min = x["low"].rolling(9).min()
        high_max = x["high"].rolling(9).max()
        rsv = (x["close"] - low_min) / (high_max - low_min).replace(0, np.nan) * 100
        x["k"] = rsv.ewm(alpha=1 / 3, adjust=False).mean()
        x["d"] = x["k"].ewm(alpha=1 / 3, adjust=False).mean()
        return x


class StrategyEngine:
    @staticmethod
    def _clamp(v: float) -> float:
        return max(0.0, min(100.0, v))

    @staticmethod
    def score(df: pd.DataFrame) -> Dict[str, float]:
        last = df.iloc[-1]
        if len(df) < 60:
            return {
                "momentum_score": 0.0,
                "trend_score": 0.0,
                "reversal_score": 0.0,
                "volume_score": 0.0,
                "risk_score": 0.0,
                "ai_score": 0.0,
                "total_score": 0.0,
                "signal": "資料不足",
                "action": "等待資料",
            }

        ret20 = (last["close"] / df.iloc[-21]["close"] - 1) * 100 if len(df) >= 21 else 0
        momentum = StrategyEngine._clamp(50 + ret20 * 2)

        trend_raw = 0
        trend_raw += 1 if pd.notna(last["ma5"]) and last["close"] > last["ma5"] else 0
        trend_raw += 1 if pd.notna(last["ma10"]) and last["ma5"] > last["ma10"] else 0
        trend_raw += 1 if pd.notna(last["ma20"]) and last["ma10"] > last["ma20"] else 0
        trend_raw += 1 if pd.notna(last["ma60"]) and last["ma20"] > last["ma60"] else 0
        trend = trend_raw * 25

        rsi = float(last["rsi14"]) if pd.notna(last["rsi14"]) else 50
        macd_hist = float(last["macd_hist"]) if pd.notna(last["macd_hist"]) else 0
        reversal = StrategyEngine._clamp((100 - abs(rsi - 55) * 1.4) * 0.6 + (50 + macd_hist * 150) * 0.4)

        vol_ma20 = df["volume"].tail(20).mean()
        vol_ratio = (float(last["volume"]) / vol_ma20) if vol_ma20 and not np.isnan(vol_ma20) else 1.0
        volume = StrategyEngine._clamp(vol_ratio * 50)

        vol20 = df["close"].pct_change().tail(20).std()
        vol20 = 0.02 if pd.isna(vol20) else float(vol20)
        risk = StrategyEngine._clamp(100 - vol20 * 1500)

        ai = StrategyEngine._clamp(momentum * 0.2 + trend * 0.25 + reversal * 0.15 + volume * 0.15 + risk * 0.25)
        total = StrategyEngine._clamp(momentum * 0.22 + trend * 0.28 + reversal * 0.15 + volume * 0.15 + risk * 0.10 + ai * 0.10)

        signal, action = StrategyEngine.signal_action(last, total)
        return {
            "momentum_score": round(momentum, 2),
            "trend_score": round(trend, 2),
            "reversal_score": round(reversal, 2),
            "volume_score": round(volume, 2),
            "risk_score": round(risk, 2),
            "ai_score": round(ai, 2),
            "total_score": round(total, 2),
            "signal": signal,
            "action": action,
        }

    @staticmethod
    def signal_action(last: pd.Series, total_score: float):
        close_ = float(last["close"])
        ma20 = float(last["ma20"]) if pd.notna(last["ma20"]) else close_
        ma60 = float(last["ma60"]) if pd.notna(last["ma60"]) else close_
        macd_hist = float(last["macd_hist"]) if pd.notna(last["macd_hist"]) else 0
        rsi = float(last["rsi14"]) if pd.notna(last["rsi14"]) else 50

        if close_ > ma20 > ma60 and macd_hist > 0 and total_score >= 80:
            return "強勢追蹤", "拉回加碼"
        if close_ >= ma20 and total_score >= 65:
            return "整理偏多", "低接布局"
        if abs(close_ - ma20) / max(ma20, 1e-6) < 0.03 and 45 <= total_score < 65:
            return "區間整理", "區間操作"
        if close_ < ma20 and rsi < 45:
            return "轉弱警戒", "減碼/防守"
        if close_ < ma60 and macd_hist < 0 and total_score < 35:
            return "急跌風險", "觀望為主"
        return "中性觀察", "等待訊號"

    @staticmethod
    def fib_targets(df: pd.DataFrame):
        recent = df.tail(60)
        swing_low = float(recent["low"].min())
        swing_high = float(recent["high"].max())
        diff = max(swing_high - swing_low, 0.01)
        return (
            round(swing_high, 2),
            round(swing_low + diff * 1.382, 2),
            round(swing_low + diff * 1.618, 2),
        )

    @staticmethod
    def wave_stage(df: pd.DataFrame):
        if len(df) < 60:
            return "資料不足"
        recent = df.tail(55)["close"].reset_index(drop=True)
        hi = int(recent.idxmax())
        lo = int(recent.idxmin())
        if hi > lo and recent.iloc[-1] > recent.mean():
            return "推動浪"
        if hi < lo and recent.iloc[-1] < recent.mean():
            return "修正浪"
        return "整理浪"


class RankingEngine:
    def __init__(self, db: DBManager):
        self.db = db

    def rebuild(self):
        master = self.db.get_master()
        today = datetime.now().strftime("%Y-%m-%d")
        rows = []

        for _, row in master.iterrows():
            stock_id = str(row["stock_id"])
            hist = self.db.get_price_history(stock_id)
            if hist.empty or len(hist) < 70:
                continue
            hist = IndicatorEngine.attach(hist)
            score = StrategyEngine.score(hist)
            rows.append({
                "date": today,
                "stock_id": stock_id,
                **score,
                "rank_all": 0,
                "rank_industry": 0
            })

        if not rows:
            return 0

        df = pd.DataFrame(rows).sort_values(["total_score", "ai_score"], ascending=[False, False]).reset_index(drop=True)
        df["rank_all"] = np.arange(1, len(df) + 1)
        merged = df.merge(master[["stock_id", "industry"]], on="stock_id", how="left")
        df["rank_industry"] = merged.groupby("industry")["total_score"].rank(method="dense", ascending=False).astype(int)
        self.db.replace_ranking(df)
        return len(df)



class MarketRegimeEngine:
    def __init__(self, db: DBManager):
        self.db = db

    def _score_proxy(self, stock_id: str) -> float:
        hist = self.db.get_price_history(stock_id)
        if hist is None or hist.empty or len(hist) < 80:
            return 50.0
        x = IndicatorEngine.attach(hist)
        last = x.iloc[-1]
        score = 0.0
        if pd.notna(last["ma20"]) and last["close"] > last["ma20"]:
            score += 30
        if pd.notna(last["ma60"]) and last["close"] > last["ma60"]:
            score += 25
        if pd.notna(last["ma20"]) and pd.notna(last["ma60"]) and last["ma20"] > last["ma60"]:
            score += 20
        if pd.notna(last["macd_hist"]) and last["macd_hist"] > 0:
            score += 15
        if pd.notna(last["rsi14"]) and last["rsi14"] > 55:
            score += 10
        return score

    def get_market_regime(self) -> dict:
        s_2330 = self._score_proxy("2330")
        s_0050 = self._score_proxy("0050")
        score = round(s_2330 * 0.6 + s_0050 * 0.4, 2)

        if score >= 70:
            regime = "多頭"
            memo = "偏多環境，可主攻強勢題材與趨勢股。"
        elif score <= 40:
            regime = "空頭"
            memo = "偏弱環境，防守優先，嚴控風險。"
        else:
            regime = "震盪"
            memo = "震盪環境，以拉回布局與區間交易為主。"

        return {"regime": regime, "score": score, "memo": memo}


class ThemeStrengthEngine:
    @staticmethod
    def summarize(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame(columns=["theme", "count", "avg_total", "avg_ai", "hot_score"])
        x = (
            df.groupby("theme", as_index=False)
            .agg(
                count=("stock_id", "count"),
                avg_total=("total_score", "mean"),
                avg_ai=("ai_score", "mean"),
            )
        )
        x["hot_score"] = x["count"] * 10 + x["avg_total"] * 0.5 + x["avg_ai"] * 0.5
        return x.sort_values(["hot_score", "avg_total", "avg_ai"], ascending=False)

    @staticmethod
    def get_hot_themes(df: pd.DataFrame) -> list:
        x = ThemeStrengthEngine.summarize(df)
        if x.empty:
            return []
        return x[(x["count"] >= 2) & (x["avg_total"] >= 55)]["theme"].tolist()


class WinRateEngine:
    @staticmethod
    def estimate(hist: pd.DataFrame) -> tuple[str, float]:
        if hist is None or hist.empty or len(hist) < 80:
            return "C", 45.0

        x = IndicatorEngine.attach(hist.copy())
        future_ret = x["close"].shift(-5) / x["close"] - 1
        cond = (
            (x["close"] > x["ma20"]) &
            (x["ma20"] > x["ma60"]) &
            (x["macd_hist"] > 0)
        )
        sample = future_ret[cond].dropna()
        if len(sample) < 8:
            base = float((future_ret.tail(30) > 0).mean() * 100) if len(future_ret.dropna()) else 45.0
        else:
            base = float((sample > 0).mean() * 100)

        if base >= 60:
            grade = "A"
        elif base >= 50:
            grade = "B"
        else:
            grade = "C"
        return grade, round(base, 2)


class TradingPlanEngine:
    def __init__(self, db: DBManager):
        self.db = db
        self.market_engine = MarketRegimeEngine(db)

    @staticmethod
    def _is_etf(stock: pd.Series) -> bool:
        try:
            return int(stock.get("is_etf", 0)) == 1 or str(stock.get("market", "")) == "ETF"
        except Exception:
            return False

    @staticmethod
    def _round_price(v) -> str:
        try:
            return f"{float(v):.2f}"
        except Exception:
            return "-"

    def build_plan(self, stock_id: str) -> dict:
        stock = self.db.get_stock_row(stock_id)
        hist = self.db.get_price_history(stock_id)
        if stock is None or hist.empty or len(hist) < 70:
            return {
                "stock_id": stock_id,
                "stock_name": stock["stock_name"] if stock is not None else stock_id,
                "theme": stock["theme"] if stock is not None else "",
                "industry": stock["industry"] if stock is not None else "",
                "trade_action": "觀望",
                "entry_zone": "-",
                "stop_loss": "-",
                "target_price": "-",
                "rr": 0.0,
                "win_grade": "C",
                "win_rate": 45.0,
                "selection_score": 0.0,
                "bucket": "排除",
                "reason": "資料不足",
            }

        x = IndicatorEngine.attach(hist.copy())
        last = x.iloc[-1]
        score = StrategyEngine.score(x)
        regime = self.market_engine.get_market_regime()
        is_etf = self._is_etf(stock)

        close_ = float(last["close"])
        ma5 = float(last["ma5"]) if pd.notna(last["ma5"]) else close_
        ma10 = float(last["ma10"]) if pd.notna(last["ma10"]) else close_
        ma20 = float(last["ma20"]) if pd.notna(last["ma20"]) else close_
        ma60 = float(last["ma60"]) if pd.notna(last["ma60"]) else close_
        rsi = float(last["rsi14"]) if pd.notna(last["rsi14"]) else 50.0
        macd_hist = float(last["macd_hist"]) if pd.notna(last["macd_hist"]) else 0.0

        breakout = close_ > max(ma20, ma60) and macd_hist > 0
        trend = ma5 > ma20 > ma60
        pullback = close_ >= ma20 and close_ <= ma5 * 1.02

        recent_high = float(x.tail(30)["high"].max())
        recent_low = float(x.tail(30)["low"].min())
        fib1, fib2, fib3 = StrategyEngine.fib_targets(x)

        win_grade, win_rate = WinRateEngine.estimate(hist)

        if regime["regime"] == "多頭":
            if is_etf:
                trade_action = "區間操作"
                bucket = "防守"
                entry_low, entry_high = ma20 * 0.99, ma20 * 1.01
                stop = ma60 * 0.97
                target = recent_high
            elif score["ai_score"] >= 60 and score["total_score"] >= 65 and (breakout or trend):
                trade_action = "可買"
                bucket = "主攻"
                entry_low, entry_high = max(ma5, ma10 * 0.995), close_
                stop = ma20 * 0.97
                target = max(fib2, recent_high)
            elif score["ai_score"] >= 55 and pullback:
                trade_action = "等待"
                bucket = "次強"
                entry_low, entry_high = ma20 * 0.99, ma20 * 1.02
                stop = ma60 * 0.97
                target = max(recent_high, fib1)
            else:
                trade_action = "觀望"
                bucket = "觀察"
                entry_low = entry_high = stop = target = np.nan
        elif regime["regime"] == "空頭":
            if is_etf:
                trade_action = "可買"
                bucket = "防守"
                entry_low, entry_high = ma20 * 0.99, ma20 * 1.01
                stop = ma60 * 0.97
                target = recent_high
            else:
                trade_action = "觀望"
                bucket = "排除"
                entry_low = entry_high = stop = target = np.nan
        else:  # 震盪
            if is_etf:
                trade_action = "區間操作"
                bucket = "防守"
                entry_low, entry_high = ma20 * 0.99, ma20 * 1.01
                stop = ma60 * 0.97
                target = recent_high
            elif score["ai_score"] >= 55 and score["total_score"] >= 58 and (pullback or trend):
                trade_action = "等待"
                bucket = "次強"
                entry_low, entry_high = ma20 * 0.99, ma20 * 1.02
                stop = recent_low * 0.98
                target = max(recent_high, fib1)
            else:
                trade_action = "觀望"
                bucket = "觀察"
                entry_low = entry_high = stop = target = np.nan

        if pd.notna(entry_high) and pd.notna(stop) and pd.notna(target):
            risk = max(entry_high - stop, 0.01)
            reward = max(target - entry_high, 0.0)
            rr = round(reward / risk, 2)
        else:
            rr = 0.0

        selection_score = (
            score["total_score"] * 0.40
            + score["ai_score"] * 0.30
            + min(rr, 3.0) * 10
            + win_rate * 0.20
        )
        if trade_action == "可買":
            selection_score += 8
        elif trade_action == "等待":
            selection_score += 2
        elif trade_action == "觀望":
            selection_score -= 8
        if is_etf:
            selection_score -= 5

        reason = f"{regime['regime']}｜{score['signal']}｜AI {score['ai_score']:.1f}｜總分 {score['total_score']:.1f}｜RSI {rsi:.1f}"

        return {
            "stock_id": stock_id,
            "stock_name": stock["stock_name"],
            "industry": stock["industry"],
            "theme": stock["theme"],
            "market": stock["market"],
            "is_etf": 1 if is_etf else 0,
            "trade_action": trade_action,
            "entry_zone": f"{self._round_price(entry_low)} ~ {self._round_price(entry_high)}" if pd.notna(entry_high) else "-",
            "stop_loss": self._round_price(stop) if pd.notna(stop) else "-",
            "target_price": self._round_price(target) if pd.notna(target) else "-",
            "rr": rr,
            "win_grade": win_grade,
            "win_rate": win_rate,
            "selection_score": round(selection_score, 2),
            "bucket": bucket,
            "reason": reason,
        }


class MasterTradingEngine:
    def __init__(self, db: DBManager):
        self.db = db
        self.market_engine = MarketRegimeEngine(db)
        self.plan_engine = TradingPlanEngine(db)

    def get_trade_pool(self, filtered_df: pd.DataFrame) -> dict:
        if filtered_df.empty:
            empty = pd.DataFrame()
            return {"market": self.market_engine.get_market_regime(), "trade_top5": empty, "attack": empty, "watch": empty, "defense": empty, "theme_summary": empty}

        base = filtered_df.copy()
        hot_themes = ThemeStrengthEngine.get_hot_themes(base)

        plans = []
        for sid in base["stock_id"].astype(str).tolist():
            plans.append(self.plan_engine.build_plan(sid))
        plans_df = pd.DataFrame(plans)

        if plans_df.empty:
            empty = pd.DataFrame()
            return {"market": self.market_engine.get_market_regime(), "trade_top5": empty, "attack": empty, "watch": empty, "defense": empty, "theme_summary": ThemeStrengthEngine.summarize(base)}

        if hot_themes:
            hot_mask = plans_df["theme"].isin(hot_themes)
        else:
            hot_mask = pd.Series([True] * len(plans_df), index=plans_df.index)

        tradable = plans_df[
            hot_mask &
            (plans_df["trade_action"].isin(["可買", "等待"])) &
            (plans_df["win_grade"].isin(["A", "B"])) &
            (plans_df["rr"] >= 1.2)
        ].copy()

        attack = tradable[(tradable["bucket"] == "主攻") & (tradable["trade_action"] == "可買")].sort_values(["selection_score", "rr", "win_rate"], ascending=False)
        watch = tradable[tradable["bucket"] == "次強"].sort_values(["selection_score", "rr", "win_rate"], ascending=False)
        defense = plans_df[(plans_df["bucket"] == "防守") & (plans_df["trade_action"].isin(["可買", "區間操作"]))].sort_values(["selection_score", "rr", "win_rate"], ascending=False)

        trade_top5 = pd.concat([attack.head(3), watch.head(2)], ignore_index=True)
        if len(trade_top5) < 5:
            used = set(trade_top5["stock_id"].tolist()) if not trade_top5.empty else set()
            extra = tradable[~tradable["stock_id"].isin(list(used))].sort_values(["selection_score", "rr", "win_rate"], ascending=False).head(5 - len(trade_top5))
            trade_top5 = pd.concat([trade_top5, extra], ignore_index=True)

        return {
            "market": self.market_engine.get_market_regime(),
            "trade_top5": trade_top5.head(5),
            "attack": attack.head(5),
            "watch": watch.head(5),
            "defense": defense.head(3),
            "theme_summary": ThemeStrengthEngine.summarize(base),
        }


class SelectionEngine:
    @staticmethod
    def prepare(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df.copy()

        x = df.copy()
        x["is_etf"] = x["market"].eq("ETF").astype(int)

        def classify_bucket(row):
            signal = str(row.get("signal", ""))
            action = str(row.get("action", ""))
            ai = float(row.get("ai_score", 0) or 0)
            total = float(row.get("total_score", 0) or 0)
            is_etf = int(row.get("is_etf", 0) or 0)

            if is_etf:
                if ai >= 55 and total >= 50:
                    return "防守"
                return "觀察"

            if signal == "強勢追蹤" and action == "拉回加碼" and ai >= 65 and total >= 70:
                return "主攻"
            if signal in ("整理偏多", "強勢追蹤") and action in ("低接布局", "拉回加碼") and ai >= 55 and total >= 60:
                return "次強"
            if signal in ("區間整理", "中性觀察") and ai >= 45 and total >= 45:
                return "觀察"
            return "排除"

        def selection_score(row):
            ai = float(row.get("ai_score", 0) or 0)
            total = float(row.get("total_score", 0) or 0)
            signal = str(row.get("signal", ""))
            action = str(row.get("action", ""))

            bonus = 0.0
            if signal == "強勢追蹤":
                bonus += 8
            elif signal == "整理偏多":
                bonus += 4

            if action == "拉回加碼":
                bonus += 6
            elif action == "低接布局":
                bonus += 3
            elif action == "區間操作":
                bonus -= 2

            return round(total * 0.55 + ai * 0.45 + bonus, 2)

        x["bucket"] = x.apply(classify_bucket, axis=1)
        x["selection_score"] = x.apply(selection_score, axis=1)
        return x

    @staticmethod
    def build_trade_pool(df: pd.DataFrame) -> dict:
        x = SelectionEngine.prepare(df)
        if x.empty:
            return {"master_top5": x, "attack": x, "watch": x, "defense": x}

        attack = x[x["bucket"] == "主攻"].sort_values(["selection_score", "ai_score", "total_score"], ascending=False)
        watch = x[x["bucket"].isin(["次強", "觀察"]) & (x["is_etf"] == 0)].sort_values(["selection_score", "ai_score", "total_score"], ascending=False)
        defense = x[x["bucket"] == "防守"].sort_values(["selection_score", "ai_score", "total_score"], ascending=False)

        master_top5 = pd.concat([attack.head(3), watch.head(2)], ignore_index=True)

        if len(master_top5) < 5:
            need = 5 - len(master_top5)
            used = set(master_top5["stock_id"].tolist()) if not master_top5.empty else set()
            extra = x[(~x["stock_id"].isin(list(used))) & (x["is_etf"] == 0)].sort_values(
                ["selection_score", "ai_score", "total_score"], ascending=False
            ).head(need)
            master_top5 = pd.concat([master_top5, extra], ignore_index=True)

        return {
            "master_top5": master_top5.head(5),
            "attack": attack.head(5),
            "watch": watch.head(5),
            "defense": defense.head(3),
        }


class AppUI:
    def __init__(self, root, db: DBManager):
        self.root = root
        self.db = db
        self.data_engine = DataEngine(db)
        self.rank_engine = RankingEngine(db)
        self.master_trading_engine = MasterTradingEngine(db)
        self.last_top5_df = pd.DataFrame()
        self.last_theme_summary_df = pd.DataFrame()
        self.last_attack_df = pd.DataFrame()
        self.last_watch_df = pd.DataFrame()
        self.last_defense_df = pd.DataFrame()
        self.current_chart_path = None
        self.worker = None

        self.root.title(APP_NAME)
        self.root.geometry("1580x920")

        self.market_var = tk.StringVar(value="全部")
        self.industry_var = tk.StringVar(value="全部")
        self.theme_var = tk.StringVar(value="全部")
        self.search_var = tk.StringVar(value="")

        self._build_ui()
        self.refresh_filters()
        self.refresh_all_tables()
        self.set_status(f"PACKED={PACKED_DATA_DIR} | EXTERNAL={EXTERNAL_DATA_DIR} | CSV={MASTER_CSV}")

    def _build_ui(self):
        top = ttk.Frame(self.root, padding=8)
        top.pack(fill="x")

        ttk.Label(top, text="市場").pack(side="left")
        self.market_cb = ttk.Combobox(top, textvariable=self.market_var, width=12, state="readonly")
        self.market_cb.pack(side="left", padx=4)

        ttk.Label(top, text="產業").pack(side="left")
        self.industry_cb = ttk.Combobox(top, textvariable=self.industry_var, width=16, state="readonly")
        self.industry_cb.pack(side="left", padx=4)

        ttk.Label(top, text="題材").pack(side="left")
        self.theme_cb = ttk.Combobox(top, textvariable=self.theme_var, width=18, state="readonly")
        self.theme_cb.pack(side="left", padx=4)

        ttk.Label(top, text="搜尋").pack(side="left")
        ttk.Entry(top, textvariable=self.search_var, width=16).pack(side="left", padx=4)

        self.btn_filter = ttk.Button(top, text="套用篩選", command=self.refresh_all_tables)
        self.btn_filter.pack(side="left", padx=4)
        self.btn_init = ttk.Button(top, text="初始化全市場", command=self.init_master_data)
        self.btn_init.pack(side="left", padx=4)
        self.btn_build = ttk.Button(top, text="建立完整歷史（一次）", command=self.build_full_history_once)
        self.btn_build.pack(side="left", padx=4)
        self.btn_update = ttk.Button(top, text="每日增量更新", command=self.update_data)
        self.btn_update.pack(side="left", padx=4)
        self.btn_rebuild = ttk.Button(top, text="重建排行", command=self.rebuild_ranking)
        self.btn_rebuild.pack(side="left", padx=4)
        self.btn_top5 = ttk.Button(top, text="AI選股TOP5", command=self.show_top5)
        self.btn_top5.pack(side="left", padx=4)
        self.btn_export_top5 = ttk.Button(top, text="下載TOP5", command=self.export_top5)
        self.btn_export_top5.pack(side="left", padx=4)
        self.btn_export_excel = ttk.Button(top, text="匯出分析Excel", command=self.export_analysis_excel)
        self.btn_export_excel.pack(side="left", padx=4)
        self.btn_open_chart = ttk.Button(top, text="開啟圖表", command=self.open_current_chart)
        self.btn_open_chart.pack(side="left", padx=4)

        self.progress_var = tk.DoubleVar(value=0)
        self.progress = ttk.Progressbar(top, variable=self.progress_var, maximum=100, length=180, mode="determinate")
        self.progress.pack(side="left", padx=6)

        self.status_label = ttk.Label(top, text="系統就緒")
        self.status_label.pack(side="right")

        main = ttk.Panedwindow(self.root, orient="horizontal")
        main.pack(fill="both", expand=True, padx=8, pady=8)

        left = ttk.Notebook(main)
        right = ttk.Frame(main, padding=8)
        main.add(left, weight=3)
        main.add(right, weight=2)

        self.tab_rank = ttk.Frame(left)
        self.tab_sector = ttk.Frame(left)
        self.tab_theme = ttk.Frame(left)
        left.add(self.tab_rank, text="排行榜")
        left.add(self.tab_sector, text="類股熱度")
        left.add(self.tab_theme, text="題材輪動")

        self.rank_tree = self._make_tree(self.tab_rank, ("rank", "id", "name", "industry", "theme", "total", "ai", "signal", "action"), {
            "rank": "排名", "id": "代號", "name": "名稱", "industry": "產業", "theme": "題材", "total": "總分", "ai": "AI分", "signal": "訊號", "action": "建議"
        })
        self.rank_tree.bind("<<TreeviewSelect>>", self.on_select_stock)

        self.sector_tree = self._make_tree(self.tab_sector, ("industry", "count", "avg_total", "avg_ai", "top_name"), {
            "industry": "產業", "count": "檔數", "avg_total": "平均總分", "avg_ai": "平均AI分", "top_name": "代表股"
        })

        self.theme_tree = self._make_tree(self.tab_theme, ("theme", "count", "avg_total", "avg_ai", "top_name"), {
            "theme": "題材", "count": "檔數", "avg_total": "平均總分", "avg_ai": "平均AI分", "top_name": "代表股"
        })

        self.detail = tk.Text(right, wrap="word", font=("Consolas", 11))
        self.detail.pack(fill="both", expand=True)

    def _make_tree(self, parent, cols, headers):
        tree = ttk.Treeview(parent, columns=cols, show="headings", height=28)
        for c in cols:
            tree.heading(c, text=headers[c])
            tree.column(c, width=140 if c not in ("rank", "count", "avg_total", "avg_ai", "id", "total", "ai") else 90, anchor="center")
        tree.pack(fill="both", expand=True)
        return tree

    def set_status(self, text):
        self.status_label.config(text=text)
        self.root.update_idletasks()

    def set_progress(self, current=0, total=100):
        total = max(int(total), 1)
        current = max(0, min(int(current), total))
        self.progress.configure(maximum=total)
        self.progress_var.set(current)
        self.root.update_idletasks()

    def reset_progress(self):
        self.progress.configure(maximum=100)
        self.progress_var.set(0)
        self.root.update_idletasks()

    def set_busy(self, busy: bool):
        state = "disabled" if busy else "normal"
        for btn in [
            self.btn_filter, self.btn_init, self.btn_build, self.btn_update,
            self.btn_rebuild, self.btn_top5, self.btn_export_top5,
            self.btn_export_excel, self.btn_open_chart
        ]:
            try:
                btn.config(state=state)
            except Exception:
                pass
        self.root.update_idletasks()

    def ui_call(self, func, *args, **kwargs):
        self.root.after(0, lambda: func(*args, **kwargs))

    def _run_in_thread(self, target, name="worker"):
        if self.worker is not None and self.worker.is_alive():
            messagebox.showwarning("提醒", "背景作業進行中，請稍候。")
            return

        def runner():
            self.ui_call(self.set_busy, True)
            self.ui_call(self.reset_progress)
            try:
                target()
            finally:
                self.ui_call(self.set_busy, False)

        self.worker = threading.Thread(target=runner, name=name, daemon=True)
        self.worker.start()

    def open_current_chart(self):
        if self.current_chart_path is None or not Path(self.current_chart_path).exists():
            return messagebox.showwarning("提醒", "目前沒有可開啟的圖表，請先點選股票。")
        open_path(Path(self.current_chart_path))

    def export_top5(self):
        if self.last_top5_df is None or self.last_top5_df.empty:
            return messagebox.showwarning("提醒", "目前沒有 TOP5 可下載，請先執行 AI選股TOP5。")
        out = RUNTIME_DIR / f"AI_TOP5_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            self.last_top5_df.to_excel(writer, sheet_name="TOP5", index=False)
        messagebox.showinfo("完成", f"AI TOP5 已輸出：\n{out}")

    def export_analysis_excel(self):
        ranking = self._filtered_ranking()
        if ranking is None or ranking.empty:
            return messagebox.showwarning("提醒", "目前沒有可匯出的分析資料。")
        out = RUNTIME_DIR / f"Analysis_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        sector = pd.DataFrame()
        theme = pd.DataFrame()
        if not ranking.empty:
            sector = ranking.groupby("industry", as_index=False).agg(count=("stock_id", "count"), avg_total=("total_score", "mean"), avg_ai=("ai_score", "mean")).sort_values(["avg_total", "avg_ai"], ascending=False)
            theme = ranking.groupby("theme", as_index=False).agg(count=("stock_id", "count"), avg_total=("total_score", "mean"), avg_ai=("ai_score", "mean")).sort_values(["avg_total", "avg_ai"], ascending=False)
        with pd.ExcelWriter(out, engine="openpyxl") as writer:
            ranking.to_excel(writer, sheet_name="Ranking", index=False)
            if not sector.empty:
                sector.to_excel(writer, sheet_name="Sector", index=False)
            if not theme.empty:
                theme.to_excel(writer, sheet_name="Theme", index=False)
            if self.last_top5_df is not None and not self.last_top5_df.empty:
                self.last_top5_df.to_excel(writer, sheet_name="Trade_TOP5", index=False)
            if self.last_attack_df is not None and not self.last_attack_df.empty:
                self.last_attack_df.to_excel(writer, sheet_name="Attack", index=False)
            if self.last_watch_df is not None and not self.last_watch_df.empty:
                self.last_watch_df.to_excel(writer, sheet_name="Watch", index=False)
            if self.last_defense_df is not None and not self.last_defense_df.empty:
                self.last_defense_df.to_excel(writer, sheet_name="Defense", index=False)
            detail_text = self.detail.get("1.0", tk.END).strip()
            pd.DataFrame({"detail": [detail_text]}).to_excel(writer, sheet_name="Detail", index=False)
        messagebox.showinfo("完成", f"分析報告已輸出：\n{out}")

    def build_full_history_once(self):
        master = self.db.get_master()
        if master.empty:
            return messagebox.showwarning("提醒", "請先初始化全市場。")

        counts = master["stock_id"].astype(str).apply(self.db.get_price_history_count)
        ready = int((counts >= 240).sum())
        total = len(master)
        if ready >= int(total * 0.9):
            ok = messagebox.askyesno("確認", f"已有 {ready}/{total} 檔具備完整歷史資料。\n再次執行將只補缺漏資料，是否繼續？")
            if not ok:
                return
        else:
            ok = messagebox.askyesno("確認", f"將建立完整歷史資料。\n目前完整檔數：{ready}/{total}\n是否開始？")
            if not ok:
                return

        def worker():
            try:
                self.ui_call(self.set_status, "開始建立完整歷史資料（一次）...")
                self.ui_call(self.set_progress, 0, total)

                def progress(idx, total_count, sid):
                    self.ui_call(self.set_progress, idx, total_count)
                    if idx % 10 == 0 or idx == total_count:
                        self.ui_call(self.set_status, f"建立歷史中 {idx}/{total_count}｜{sid}")

                success, rows = self.data_engine.build_full_history(progress_cb=progress)
                self.ui_call(self.set_progress, total, total)
                self.ui_call(self.set_status, f"完整歷史建立完成：成功 {success} 檔，寫入 {rows} 筆。")
                self.ui_call(messagebox.showinfo, "完成", f"完整歷史建立完成\n成功 {success} 檔\n寫入 {rows} 筆")
            except Exception as e:
                traceback.print_exc()
                self.ui_call(messagebox.showerror, "錯誤", str(e))

        self._run_in_thread(worker, "build_history")

    def init_master_data(self):
        master = self.db.get_master()
        if not master.empty and len(master) > 500:
            ok = messagebox.askyesno("確認", f"目前已存在 {len(master)} 檔股票主檔。\n重新初始化將覆蓋現有主檔，是否繼續？")
            if not ok:
                return

        def worker():
            try:
                self.ui_call(self.set_status, "開始初始化全市場股票清單...")
                self.ui_call(self.set_progress, 10, 100)
                universe = build_full_market_universe()
                if universe is None or universe.empty:
                    csv_path = resolve_master_csv()
                    self.db.import_master_csv(csv_path)
                    master2 = self.db.get_master()
                    self.ui_call(self.refresh_filters)
                    self.ui_call(self.refresh_all_tables)
                    self.ui_call(self.set_progress, 100, 100)
                    self.ui_call(self.set_status, f"已改用本地主檔，共 {len(master2)} 檔。")
                    self.ui_call(messagebox.showinfo, "完成", f"全市場抓取失敗，已改用本地主檔\n共 {len(master2)} 檔\n\n使用主檔：{csv_path}")
                    return
                self.db.import_master_df(universe)
                master2 = self.db.get_master()
                self.ui_call(self.refresh_filters)
                self.ui_call(self.refresh_all_tables)
                self.ui_call(self.set_progress, 100, 100)
                self.ui_call(self.set_status, f"全市場初始化完成，共 {len(master2)} 檔。")
                self.ui_call(messagebox.showinfo, "完成", f"全市場股票清單初始化完成\n共 {len(master2)} 檔")
            except Exception as e:
                traceback.print_exc()
                self.ui_call(messagebox.showerror, "錯誤", f"初始化失敗：\n{e}")

        self._run_in_thread(worker, "init_market")

    def refresh_filters(self):
        master = self.db.get_master()
        if master.empty:
            self.market_cb["values"] = ["全部"]
            self.industry_cb["values"] = ["全部"]
            self.theme_cb["values"] = ["全部"]
            return
        self.market_cb["values"] = ["全部"] + sorted([x for x in master["market"].dropna().unique().tolist() if str(x).strip() != ""])
        self.industry_cb["values"] = ["全部"] + sorted([x for x in master["industry"].dropna().unique().tolist() if str(x).strip() != ""])
        self.theme_cb["values"] = ["全部"] + sorted([x for x in master["theme"].dropna().unique().tolist() if str(x).strip() != ""])

    def _filtered_ranking(self):
        df = self.db.get_latest_ranking()
        if df.empty:
            return df
        if self.market_var.get() != "全部":
            df = df[df["market"] == self.market_var.get()]
        if self.industry_var.get() != "全部":
            df = df[df["industry"] == self.industry_var.get()]
        if self.theme_var.get() != "全部":
            df = df[df["theme"] == self.theme_var.get()]
        q = self.search_var.get().strip()
        if q:
            df = df[df["stock_id"].str.contains(q, case=False) | df["stock_name"].str.contains(q, case=False)]
        return df.sort_values(["rank_all"]).reset_index(drop=True)

    def refresh_all_tables(self):
        for tree in (self.rank_tree, self.sector_tree, self.theme_tree):
            for item in tree.get_children():
                tree.delete(item)

        df = self._filtered_ranking()
        if df.empty:
            self.set_status("目前尚無排行資料，請先更新資料並重建排行。")
            return

        for i, row in df.iterrows():
            self.rank_tree.insert("", "end", values=(
                i + 1, row["stock_id"], row["stock_name"], row["industry"], row["theme"],
                f"{row['total_score']:.2f}", f"{row['ai_score']:.2f}", row["signal"], row["action"]
            ))

        sector = (
            df.groupby("industry", as_index=False)
            .agg(count=("stock_id", "count"), avg_total=("total_score", "mean"), avg_ai=("ai_score", "mean"))
            .sort_values(["avg_total", "avg_ai"], ascending=False)
        )
        for _, r in sector.iterrows():
            top_name = df[df["industry"] == r["industry"]].sort_values("total_score", ascending=False).iloc[0]["stock_name"]
            self.sector_tree.insert("", "end", values=(
                r["industry"], int(r["count"]), f"{r['avg_total']:.2f}", f"{r['avg_ai']:.2f}", top_name
            ))

        theme = (
            df.groupby("theme", as_index=False)
            .agg(count=("stock_id", "count"), avg_total=("total_score", "mean"), avg_ai=("ai_score", "mean"))
            .sort_values(["avg_total", "avg_ai"], ascending=False)
        )
        for _, r in theme.iterrows():
            top_name = df[df["theme"] == r["theme"]].sort_values("total_score", ascending=False).iloc[0]["stock_name"]
            self.theme_tree.insert("", "end", values=(
                r["theme"], int(r["count"]), f"{r['avg_total']:.2f}", f"{r['avg_ai']:.2f}", top_name
            ))

        trade = self.master_trading_engine.get_trade_pool(df)
        attack_cnt = len(trade["attack"])
        defense_cnt = len(trade["defense"])
        self.set_status(
            f"已載入資料，共 {len(df)} 檔｜市場 {trade['market']['regime']}｜主攻 {attack_cnt}｜防守 {defense_cnt}"
        )

    def update_data(self):
        last_date = self.db.get_last_price_date()
        today = datetime.now().strftime("%Y-%m-%d")
        if last_date == today:
            ok = messagebox.askyesno("確認", f"今日資料（{today}）可能已更新過。\n再次執行會覆蓋今日官方資料，是否繼續？")
            if not ok:
                return

        def worker():
            try:
                self.ui_call(self.set_status, "開始每日增量更新（官方優先，只更新今日）...")
                master = self.db.get_master()
                total = len(master) if not master.empty else 1
                self.ui_call(self.set_progress, 0, total)

                def progress(idx, total_count, sid):
                    self.ui_call(self.set_progress, idx, total_count)
                    if idx % 50 == 0 or idx == total_count:
                        self.ui_call(self.set_status, f"每日更新中 {idx}/{total_count}｜{sid}")

                success, rows = self.data_engine.update_incremental(progress_cb=progress)
                self.ui_call(self.set_status, "資料更新完成，開始重建排行...")
                rank_count = self.rank_engine.rebuild()
                self.ui_call(self.set_progress, total, total)
                self.ui_call(self.refresh_filters)
                self.ui_call(self.refresh_all_tables)
                self.ui_call(self.set_status, f"完成：成功 {success} 檔，寫入 {rows} 筆，排行 {rank_count} 檔。")
                self.ui_call(messagebox.showinfo, "完成", f"每日增量更新完成\n成功 {success} 檔\n寫入 {rows} 筆\n排行 {rank_count} 檔\n（TWSE/TPEX 官方優先，只更新今日）")
            except Exception as e:
                traceback.print_exc()
                self.ui_call(messagebox.showerror, "錯誤", str(e))

        self._run_in_thread(worker, "update_daily")

    def rebuild_ranking(self):
        def worker():
            try:
                self.ui_call(self.set_status, "開始重建排行...")
                self.ui_call(self.set_progress, 10, 100)
                count = self.rank_engine.rebuild()
                self.ui_call(self.set_progress, 90, 100)
                self.ui_call(self.refresh_filters)
                self.ui_call(self.refresh_all_tables)
                self.ui_call(self.set_progress, 100, 100)
                self.ui_call(messagebox.showinfo, "完成", f"排行已完成，共 {count} 檔")
            except Exception as e:
                traceback.print_exc()
                self.ui_call(messagebox.showerror, "錯誤", str(e))

        self._run_in_thread(worker, "rebuild_rank")

    def show_top5(self):
        df = self._filtered_ranking()
        if df.empty:
            return messagebox.showwarning("提醒", "尚無資料")

        trade = self.master_trading_engine.get_trade_pool(df)
        market = trade["market"]
        trade_top5 = trade["trade_top5"]
        attack = trade["attack"]
        watch = trade["watch"]
        defense = trade["defense"]
        theme_summary = trade["theme_summary"]

        self.last_top5_df = trade_top5.copy()
        self.last_attack_df = attack.copy()
        self.last_watch_df = watch.copy()
        self.last_defense_df = defense.copy()
        self.last_theme_summary_df = theme_summary.copy()

        lines = [
            "《v6.0 FULL 整合升級版》",
            f"市場判斷：{market['regime']}（{market['score']:.2f}）",
            f"市場說明：{market['memo']}",
            ""
        ]

        lines.append("【可下單 TOP5】")
        if trade_top5.empty:
            lines.append("目前無符合條件標的")
        else:
            for i, (_, r) in enumerate(trade_top5.iterrows(), start=1):
                lines.append(
                    f"{i}. {r['stock_id']} {r['stock_name']}｜{r['theme']}｜{r['trade_action']}\n"
                    f"   進場: {r['entry_zone']}｜停損: {r['stop_loss']}｜目標: {r['target_price']}\n"
                    f"   RR: {r['rr']:.2f}｜勝率: {r['win_grade']}({r['win_rate']:.1f}%)｜理由: {r['reason']}"
                )

        lines.append("")
        lines.append("【主攻候選】")
        if attack.empty:
            lines.append("無")
        else:
            for _, r in attack.iterrows():
                lines.append(f"- {r['stock_id']} {r['stock_name']}｜RR {r['rr']:.2f}｜{r['trade_action']}")

        lines.append("")
        lines.append("【次強觀察】")
        if watch.empty:
            lines.append("無")
        else:
            for _, r in watch.iterrows():
                lines.append(f"- {r['stock_id']} {r['stock_name']}｜進場 {r['entry_zone']}｜RR {r['rr']:.2f}")

        lines.append("")
        lines.append("【防守ETF】")
        if defense.empty:
            lines.append("無")
        else:
            for _, r in defense.iterrows():
                lines.append(f"- {r['stock_id']} {r['stock_name']}｜{r['trade_action']}｜區間 {r['entry_zone']}")

        lines.append("")
        lines.append("【主流題材】")
        if theme_summary.empty:
            lines.append("無")
        else:
            for _, r in theme_summary.head(5).iterrows():
                lines.append(f"- {r['theme']}｜檔數 {int(r['count'])}｜總分 {r['avg_total']:.1f}｜AI {r['avg_ai']:.1f}")

        messagebox.showinfo("AI選股TOP5", "\n".join(lines))

    def on_select_stock(self, event=None):
        sel = self.rank_tree.selection()
        if not sel:
            return
        vals = self.rank_tree.item(sel[0], "values")
        stock_id = str(vals[1])
        stock = self.db.get_stock_row(stock_id)
        hist = self.db.get_price_history(stock_id)
        if stock is None or hist.empty:
            return
        hist = IndicatorEngine.attach(hist)
        last = hist.iloc[-1]
        fib1, fib2, fib3 = StrategyEngine.fib_targets(hist)
        wave = StrategyEngine.wave_stage(hist)
        chart_path = self.export_chart(stock_id, hist)
        self.current_chart_path = chart_path

        trade_plan = self.master_trading_engine.plan_engine.build_plan(stock_id)

        lines = [
            f"股票：{stock['stock_name']} ({stock_id})",
            f"市場 / 產業 / 題材：{stock['market']} / {stock['industry']} / {stock['theme']}",
            f"最新收盤：{last['close']:.2f}",
            "MA20 / MA60：{:.2f} / {:.2f}".format(last["ma20"], last["ma60"]) if pd.notna(last["ma20"]) and pd.notna(last["ma60"]) else "MA20 / MA60：資料不足",
            "RSI14：{:.2f}".format(last["rsi14"]) if pd.notna(last["rsi14"]) else "RSI14：資料不足",
            "MACD Hist：{:.4f}".format(last["macd_hist"]) if pd.notna(last["macd_hist"]) else "MACD Hist：資料不足",
            "K / D：{:.2f} / {:.2f}".format(last["k"], last["d"]) if pd.notna(last["k"]) and pd.notna(last["d"]) else "K / D：資料不足",
            "",
            f"波浪階段：{wave}",
            f"Fib 1.0 / 1.382 / 1.618：{fib1:.2f} / {fib2:.2f} / {fib3:.2f}",
            "",
            "【交易計畫】",
            f"動作：{trade_plan['trade_action']}",
            f"進場區：{trade_plan['entry_zone']}",
            f"停損：{trade_plan['stop_loss']}",
            f"目標價：{trade_plan['target_price']}",
            f"RR：{trade_plan['rr']:.2f}",
            f"勝率：{trade_plan['win_grade']} ({trade_plan['win_rate']:.1f}%)",
            f"理由：{trade_plan['reason']}",
            "",
            f"圖表輸出：{chart_path}",
        ]
        self.detail.delete("1.0", tk.END)
        self.detail.insert("1.0", "\n".join(lines))

    def export_chart(self, stock_id: str, hist: pd.DataFrame):
        x = hist.tail(120).copy()
        x["date"] = pd.to_datetime(x["date"])
        fig = plt.figure(figsize=(10, 5))
        ax = fig.add_subplot(111)
        ax.plot(x["date"], x["close"], label="Close")
        ax.plot(x["date"], x["ma20"], label="MA20")
        ax.plot(x["date"], x["ma60"], label="MA60")
        ax.legend()
        ax.set_title(stock_id)
        fig.autofmt_xdate()
        out = CHART_DIR / f"{stock_id}_chart.png"
        fig.savefig(out, dpi=140, bbox_inches="tight")
        plt.close(fig)
        return out


def bootstrap():
    db = DBManager(DB_PATH)
    db.init_db()

    init_message = "股票主檔已就緒"
    try:
        master = db.get_master()
        if master.empty:
            universe = build_full_market_universe()
            if universe is not None and not universe.empty:
                db.import_master_df(universe)
                master = db.get_master()
                init_message = f"已自動建立全市場股票主檔，共 {len(master)} 檔"
            else:
                csv_path = resolve_master_csv()
                db.import_master_csv(csv_path)
                master = db.get_master()
                init_message = f"已改用本地主檔，共 {len(master)} 檔 | {csv_path}"
        else:
            init_message = f"股票主檔已載入，共 {len(master)} 檔"
    except Exception as e:
        init_message = f"股票主檔初始化失敗：{e}"

    return db, init_message


def main():
    db, init_message = bootstrap()
    root = tk.Tk()
    app = AppUI(root, db)
    app.set_status(init_message)

    def _close():
        db.close()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", _close)
    root.mainloop()


if __name__ == "__main__":
    main()
