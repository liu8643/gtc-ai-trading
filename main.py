
# -*- coding: utf-8 -*-
"""
GTC AI Trading System v6.5 PRO-TRADING-SYSTEM

功能：
- 股票主檔分類（市場 / 產業 / 題材）
- 本地 SQLite 歷史資料庫
- TWSE/TPEX 官方資料 + Yahoo Finance 備援更新
- 技術指標：MA / MACD / RSI / KD
- 排行榜 / 類股熱度 / 題材輪動
- AI 選股 TOP20
- 每日更新官方優先 + Yahoo 備援
- 自動產生下單清單
- Tkinter 桌面 UI
"""

import sqlite3
import traceback
import requests
import sys
import csv
import io
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


class OperationCancelled(Exception):
    pass


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
APP_NAME = "GTC AI Trading System v6.5 PRO-TRADING-SYSTEM"
STATE_PATH = RUNTIME_DIR / "build_history_state_v6_5_pro_trading_system.json"


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


CLASSIFICATION_BOOK_CANDIDATES = [
    RUNTIME_DIR / "股票類別對照表.xlsx",
    BASE_DIR / "股票類別對照表.xlsx",
]


def normalize_stock_id(v) -> str:
    s = str(v).strip()
    if s in ("", "nan", "None"):
        return ""
    m = re.search(r"(\d{4,5})", s)
    if not m:
        return ""
    code = m.group(1)
    return code.zfill(4) if len(code) == 4 else code


def resolve_classification_book() -> Optional[Path]:
    for p in CLASSIFICATION_BOOK_CANDIDATES:
        if p.exists():
            return p
    return None


def load_manual_theme_mapping() -> pd.DataFrame:
    manual_parts = []
    try:
        seed = pd.read_csv(io.StringIO(DEFAULT_MASTER_CSV), dtype={"stock_id": str}).fillna("")
        manual_parts.append(seed)
    except Exception:
        pass
    try:
        csv_path = resolve_master_csv()
        if csv_path.exists():
            ext = pd.read_csv(csv_path, dtype={"stock_id": str}).fillna("")
            ext = ext[[c for c in ["stock_id", "stock_name", "market", "industry", "theme", "sub_theme", "is_etf"] if c in ext.columns]]
            if not ext.empty:
                manual_parts.append(ext)
    except Exception:
        pass
    if not manual_parts:
        return pd.DataFrame(columns=["stock_id", "stock_name_manual", "market_manual", "industry_manual", "theme_manual", "sub_theme_manual", "is_etf_manual"])
    x = pd.concat(manual_parts, ignore_index=True).fillna("")
    x["stock_id"] = x["stock_id"].astype(str).map(normalize_stock_id)
    x = x[x["stock_id"] != ""].copy()
    for c in ["stock_name", "market", "industry", "theme", "sub_theme", "is_etf"]:
        if c not in x.columns:
            x[c] = ""
    x = x.drop_duplicates(subset=["stock_id"], keep="first")
    return x.rename(columns={
        "stock_name": "stock_name_manual",
        "market": "market_manual",
        "industry": "industry_manual",
        "theme": "theme_manual",
        "sub_theme": "sub_theme_manual",
        "is_etf": "is_etf_manual",
    })


def load_official_classification_book() -> pd.DataFrame:
    path = resolve_classification_book()
    if path is None:
        return pd.DataFrame(columns=["stock_id", "stock_name_official", "market_official", "industry_official"])
    parts = []
    try:
        xl = pd.ExcelFile(path)
        for sheet in xl.sheet_names:
            df = xl.parse(sheet).fillna("")
            market = "上市" if "上市" in sheet else ("上櫃" if "上櫃" in sheet else ("興櫃" if "興櫃" in sheet else ""))
            rename = {}
            for c in df.columns:
                s = str(c).strip()
                if s in ("代號", "股票代號"):
                    rename[c] = "stock_id"
                elif s in ("公司名稱", "公司簡稱"):
                    rename[c] = "stock_name_official"
                elif s in ("新產業類別", "新產業別"):
                    rename[c] = "industry_official"
            df = df.rename(columns=rename)
            if "stock_id" not in df.columns or "industry_official" not in df.columns:
                continue
            if "stock_name_official" not in df.columns:
                df["stock_name_official"] = ""
            df["stock_id"] = df["stock_id"].map(normalize_stock_id)
            df = df[df["stock_id"] != ""].copy()
            if df.empty:
                continue
            df["market_official"] = market
            parts.append(df[["stock_id", "stock_name_official", "market_official", "industry_official"]])
    except Exception:
        return pd.DataFrame(columns=["stock_id", "stock_name_official", "market_official", "industry_official"])
    if not parts:
        return pd.DataFrame(columns=["stock_id", "stock_name_official", "market_official", "industry_official"])
    out = pd.concat(parts, ignore_index=True).fillna("")
    out = out.drop_duplicates(subset=["stock_id"], keep="first")
    return out


THEME_RULES = [
    (r"台積電|創意|世芯|晶心|聯發科|聯詠|矽力|祥碩", ("半導體", "AI/晶圓代工", "半導體")),
    (r"智邦|智易|華星光|聯亞|光聖|波若威|上詮|聯鈞|眾達|環宇|前鼎|立碁|中磊|啟碁|正文|建漢|神準", ("網通/光通訊", "CPO/光模組", "高速光通訊")),
    (r"台達電|光寶科|康舒|群電|全漢|偉訓|順達|AES|新盛力|加百裕|系統電", ("電源/電機", "電源/HVDC", "電源")),
    (r"奇鋐|雙鴻|建準|超眾|力致|高力", ("散熱", "AI散熱", "液冷")),
    (r"鴻海|廣達|緯創|緯穎|仁寶|英業達|和碩|技嘉|華碩|微星|神達", ("電子代工", "AI伺服器", "伺服器")),
]

INDUSTRY_THEME_MAP = {
    "食品工業": ("食品工業", "民生消費", "食品"),
    "塑膠工業": ("塑膠工業", "基礎原物料", "塑膠"),
    "紡織纖維": ("紡織纖維", "傳產", "紡織"),
    "電機機械": ("電源/電機", "電源/HVDC", "電機"),
    "電器電纜": ("電源/電機", "電力基建", "電纜"),
    "化學工業": ("化學工業", "基礎原物料", "化工"),
    "生技醫療": ("生技醫療", "生技醫療", "醫療"),
    "玻璃陶瓷": ("玻璃陶瓷", "傳產", "玻璃陶瓷"),
    "造紙工業": ("造紙工業", "傳產", "造紙"),
    "鋼鐵工業": ("鋼鐵工業", "基礎原物料", "鋼鐵"),
    "橡膠工業": ("橡膠工業", "傳產", "橡膠"),
    "汽車工業": ("汽車工業", "電動車", "車用"),
    "電子工業": ("電子工業", "電子", "電子"),
    "半導體業": ("半導體", "半導體", "半導體"),
    "電腦及週邊設備業": ("電子代工", "AI伺服器", "伺服器"),
    "光電業": ("光電", "光電", "面板/光學"),
    "通信網路業": ("網通/光通訊", "網通/光通訊", "網通"),
    "電子零組件業": ("電子零組件", "電子零組件", "零組件"),
    "電子通路業": ("電子通路", "電子通路", "通路"),
    "資訊服務業": ("資訊服務", "軟體/資訊服務", "資訊服務"),
    "其他電子業": ("其他電子", "電子", "其他電子"),
    "建材營造": ("建材營造", "傳產", "營造"),
    "航運業": ("航運業", "運輸", "航運"),
    "觀光餐旅": ("觀光餐旅", "內需消費", "觀光"),
    "金融保險": ("金融保險", "金融", "金融"),
    "貿易百貨": ("貿易百貨", "內需消費", "百貨"),
    "油電燃氣": ("油電燃氣", "公用事業", "能源"),
    "居家生活": ("居家生活", "內需消費", "居家"),
    "綠能環保": ("綠能環保", "綠能環保", "環保"),
    "數位雲端": ("資訊服務", "軟體/雲端", "雲端"),
    "運動休閒": ("運動休閒", "內需消費", "運動"),
    "文化創意業": ("文化創意", "內需消費", "文創"),
    "農業科技業": ("農業科技", "農業科技", "農業"),
}


def infer_theme_bundle(stock_name: str, industry: str, is_etf: int) -> Tuple[str, str, str]:
    name = str(stock_name or "")
    industry = str(industry or "").strip()
    if int(is_etf or 0) == 1 or re.search(r"ETF|台灣50|高股息|中型100|科技優息|精選高息", name):
        return "ETF", "ETF", "ETF"
    for pattern, bundle in THEME_RULES:
        if re.search(pattern, name):
            return bundle
    if industry in INDUSTRY_THEME_MAP:
        return INDUSTRY_THEME_MAP[industry]
    if re.search(r"光|通|網|訊", name):
        return (industry or "網通/光通訊", "網通/光通訊", "網通")
    if re.search(r"電|控|達|機", name):
        return (industry or "電源/電機", "電源/HVDC", "電源")
    if re.search(r"積電|半導體|晶|芯", name):
        return (industry or "半導體", "半導體", "半導體")
    return (industry or "未分類", "全市場", "系統掃描")


def apply_classification_layers(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy().fillna("")
    official = load_official_classification_book()
    manual = load_manual_theme_mapping()

    x["stock_id"] = x["stock_id"].astype(str).map(normalize_stock_id)
    x = x[x["stock_id"] != ""].copy()

    if not official.empty:
        x = x.merge(official, on="stock_id", how="left")
    else:
        x["stock_name_official"] = ""
        x["market_official"] = ""
        x["industry_official"] = ""

    if not manual.empty:
        x = x.merge(manual, on="stock_id", how="left")
    else:
        x["stock_name_manual"] = ""
        x["market_manual"] = ""
        x["industry_manual"] = ""
        x["theme_manual"] = ""
        x["sub_theme_manual"] = ""
        x["is_etf_manual"] = ""

    x["stock_name"] = x["stock_name"].replace("", pd.NA)
    x["stock_name"] = x["stock_name"].fillna(x.get("stock_name_official", "").replace("", pd.NA) if hasattr(x.get("stock_name_official", ""), 'replace') else x.get("stock_name_official", ""))
    x["stock_name"] = x["stock_name"].fillna(x.get("stock_name_manual", "").replace("", pd.NA) if hasattr(x.get("stock_name_manual", ""), 'replace') else x.get("stock_name_manual", ""))
    x["stock_name"] = x["stock_name"].fillna(x["stock_id"]).astype(str)

    etf_mask = x["stock_id"].astype(str).str.startswith("00") | x["stock_name"].astype(str).str.contains("ETF|台灣50|高股息|中型100|科技優息|精選高息", regex=True)
    if "is_etf_manual" in x.columns:
        etf_mask = etf_mask | pd.to_numeric(x["is_etf_manual"], errors="coerce").fillna(0).astype(int).eq(1)
    x["is_etf"] = etf_mask.astype(int)

    x["market"] = x["market"].replace("", pd.NA)
    if "market_official" in x.columns:
        x["market"] = x["market"].fillna(x["market_official"].replace("", pd.NA))
    if "market_manual" in x.columns:
        x["market"] = x["market"].fillna(x["market_manual"].replace("", pd.NA))
    x["market"] = x["market"].fillna("上市")
    x.loc[x["is_etf"].eq(1), "market"] = "ETF"

    x["industry"] = x["industry"].replace("", pd.NA)
    if "industry_official" in x.columns:
        x["industry"] = x["industry"].fillna(x["industry_official"].replace("", pd.NA))
    if "industry_manual" in x.columns:
        x["industry"] = x["industry"].fillna(x["industry_manual"].replace("", pd.NA))
    x["industry"] = x["industry"].fillna("未分類")

    x["theme"] = x["theme"] if "theme" in x.columns else ""
    x["sub_theme"] = x["sub_theme"] if "sub_theme" in x.columns else ""
    if "theme_manual" in x.columns:
        x["theme"] = x["theme"].replace("", pd.NA).fillna(x["theme_manual"].replace("", pd.NA))
    if "sub_theme_manual" in x.columns:
        x["sub_theme"] = x["sub_theme"].replace("", pd.NA).fillna(x["sub_theme_manual"].replace("", pd.NA))

    bundles = x.apply(lambda r: infer_theme_bundle(r.get("stock_name", ""), r.get("industry", ""), r.get("is_etf", 0)), axis=1, result_type="expand")
    bundles.columns = ["industry_inferred", "theme_inferred", "sub_theme_inferred"]
    x = pd.concat([x, bundles], axis=1)

    x["industry"] = x["industry"].replace("未分類", pd.NA).fillna(x["industry_inferred"]).fillna("未分類")
    x["theme"] = x["theme"].replace("", pd.NA).fillna(x["theme_inferred"]).fillna("全市場")
    x["sub_theme"] = x["sub_theme"].replace("", pd.NA).fillna(x["sub_theme_inferred"]).fillna("系統掃描")
    x.loc[x["is_etf"].eq(1), ["industry", "theme", "sub_theme"]] = ["ETF", "ETF", "ETF"]

    x["is_active"] = 1
    x["update_date"] = datetime.now().strftime("%Y-%m-%d")
    keep = ["stock_id", "stock_name", "market", "industry", "theme", "sub_theme", "is_etf", "is_active", "update_date"]
    for c in keep:
        if c not in x.columns:
            x[c] = ""
    return x[keep].drop_duplicates(subset=["stock_id"], keep="first").reset_index(drop=True)

DATA_DIR = EXTERNAL_DATA_DIR if (EXTERNAL_DATA_DIR / "stocks_master.csv").exists() else PACKED_DATA_DIR
CHART_DIR = RUNTIME_DIR / "charts"
CHART_DIR.mkdir(exist_ok=True)

LEGACY_DB_PATH = RUNTIME_DIR / "stock_system_v6_0_1.db"
DB_PATH = RUNTIME_DIR / "stock_system_v6_2.db"
LEGACY_DB_PATH_V606 = RUNTIME_DIR / "stock_system_v6_0_6.db"
LEGACY_DB_PATH_V603 = RUNTIME_DIR / "stock_system_v6_0_3.db"
if (not DB_PATH.exists()) and LEGACY_DB_PATH_V606.exists():
    DB_PATH = LEGACY_DB_PATH_V606
elif (not DB_PATH.exists()) and LEGACY_DB_PATH_V603.exists():
    DB_PATH = LEGACY_DB_PATH_V603
elif (not DB_PATH.exists()) and LEGACY_DB_PATH.exists():
    DB_PATH = LEGACY_DB_PATH
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


def available_excel_engine() -> Optional[str]:
    try:
        import importlib.util
        if importlib.util.find_spec("xlsxwriter") is not None:
            return "xlsxwriter"
        if importlib.util.find_spec("openpyxl") is not None:
            return "openpyxl"
    except Exception:
        pass
    return None


def safe_sheet_name(name: str) -> str:
    invalid = r'[]:*?/\\'
    out = "".join("_" if ch in invalid else ch for ch in str(name))
    return out[:31] or "Sheet1"


def write_table_bundle(base_path: Path, tables: Dict[str, pd.DataFrame], preferred: str = "excel") -> tuple[Path, str]:
    clean_tables = {}
    for name, df in (tables or {}).items():
        if df is None:
            continue
        if isinstance(df, pd.DataFrame) and not df.empty:
            clean_tables[str(name)] = df.copy()
    if not clean_tables:
        raise ValueError("沒有可輸出的資料")

    preferred = (preferred or "excel").lower()
    engine = available_excel_engine()

    if preferred == "excel" and engine:
        out = base_path.with_suffix(".xlsx")
        with pd.ExcelWriter(out, engine=engine) as writer:
            for name, df in clean_tables.items():
                df.to_excel(writer, sheet_name=safe_sheet_name(name), index=False)
        return out, f"Excel（{engine}）"

    if preferred == "txt":
        if len(clean_tables) == 1:
            _, df = next(iter(clean_tables.items()))
            out = base_path.with_suffix(".txt")
            df.to_csv(out, index=False, sep="\t", encoding="utf-8-sig")
            return out, "TXT"
        out_dir = base_path.parent / f"{base_path.name}_TXT"
        out_dir.mkdir(parents=True, exist_ok=True)
        for name, df in clean_tables.items():
            df.to_csv(out_dir / f"{name}.txt", index=False, sep="\t", encoding="utf-8-sig")
        return out_dir, "TXT資料夾"

    if len(clean_tables) == 1:
        _, df = next(iter(clean_tables.items()))
        out = base_path.with_suffix(".csv")
        df.to_csv(out, index=False, encoding="utf-8-sig")
        return out, "CSV"

    out_dir = base_path.parent / f"{base_path.name}_CSV"
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, df in clean_tables.items():
        df.to_csv(out_dir / f"{name}.csv", index=False, encoding="utf-8-sig")
    return out_dir, "CSV資料夾"



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

    x["stock_id"] = x["stock_id"].astype(str).map(normalize_stock_id)
    x["stock_name"] = x["stock_name"].astype(str).str.strip()
    x = x[x["stock_id"] != ""].copy()
    x = x[x["stock_id"].str.fullmatch(r"\d{4,5}", na=False)].copy()
    if x.empty:
        return pd.DataFrame(columns=["stock_id", "stock_name", "market", "industry", "theme", "sub_theme", "is_etf", "is_active", "update_date"])

    x["market"] = market_label
    x["industry"] = ""
    x["theme"] = ""
    x["sub_theme"] = ""
    x["is_etf"] = x["stock_id"].str.startswith("00").astype(int)
    x["is_active"] = 1
    x["update_date"] = datetime.now().strftime("%Y-%m-%d")

    x = apply_classification_layers(x)
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
            x = x[x["stock_id"].str.fullmatch(r"\d{4,5}", na=False)].copy()
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
        x = x[x["stock_id"].str.fullmatch(r"\d{4,5}", na=False)].copy()
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

    def get_total_price_rows(self) -> int:
        with self.lock:
            row = self.conn.cursor().execute("SELECT COUNT(*) FROM price_history").fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    def get_ranking_rows_count(self) -> int:
        with self.lock:
            row = self.conn.cursor().execute("SELECT COUNT(*) FROM ranking_result WHERE date = (SELECT MAX(date) FROM ranking_result)").fetchone()
        return int(row[0]) if row and row[0] is not None else 0

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
                df = df[df["stock_id"].str.fullmatch(r"\d{4,5}", na=False)].copy()
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

    def download_latest_bar_yahoo(self, stock_id: str, market: str, days: str = "7d") -> pd.DataFrame:
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
        latest = pd.DataFrame()
        for symbol in symbols:
            if symbol in seen:
                continue
            seen.add(symbol)
            try:
                hist = yf.Ticker(symbol).history(period=days, auto_adjust=False)
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
                out = out.dropna(subset=["close"]).sort_values("date")
                if not out.empty:
                    latest = out.tail(1).copy()
                    break
            except Exception:
                continue
        return latest


    def build_full_history(self, min_days: int = 240, batch_size: int = 25, sleep_sec: float = 0.6, progress_cb=None, log_cb=None, cancel_cb=None) -> Tuple[int, int, int]:
        master = self.db.get_master()
        if master.empty:
            return 0, 0, 0
        success = 0
        failed = 0
        rows = 0
        total = len(master)
        for idx, (_, row) in enumerate(master.iterrows(), start=1):
            if cancel_cb and cancel_cb():
                raise OperationCancelled("使用者中斷完整歷史建庫")
            stock_id = str(row["stock_id"])
            market = str(row["market"])
            existing = self.db.get_price_history_count(stock_id)
            if existing >= min_days:
                if progress_cb:
                    progress_cb(idx, total, stock_id, existing, "skip")
                if log_cb and (idx % 25 == 0 or idx == total):
                    log_cb(f"[{idx}/{total}] {stock_id} 已具備 {existing} 筆歷史，跳過")
                continue
            try:
                hist_df = self.download_history(stock_id, market, period="2y")
                if hist_df is not None and not hist_df.empty:
                    self.db.upsert_price_history(stock_id, hist_df)
                    success += 1
                    rows += len(hist_df)
                    current_count = self.db.get_price_history_count(stock_id)
                    if log_cb:
                        log_cb(f"[{idx}/{total}] {stock_id} 補建成功，新增/覆蓋 {len(hist_df)} 筆，累計 {current_count} 筆")
                    if progress_cb:
                        progress_cb(idx, total, stock_id, current_count, "ok")
                else:
                    failed += 1
                    if log_cb:
                        log_cb(f"[{idx}/{total}] {stock_id} 無可用歷史資料")
                    if progress_cb:
                        progress_cb(idx, total, stock_id, existing, "fail")
            except Exception as e:
                failed += 1
                if log_cb:
                    log_cb(f"[{idx}/{total}] {stock_id} 下載失敗：{e}")
                if progress_cb:
                    progress_cb(idx, total, stock_id, existing, "error")
            if idx % batch_size == 0:
                if log_cb:
                    log_cb(f"--- 分批節點：已處理 {idx}/{total}，暫停 {sleep_sec:.1f} 秒，避免介面卡住 ---")
                time.sleep(sleep_sec)
        return success, failed, rows

    def update_incremental(self, progress_cb=None, log_cb=None, cancel_cb=None) -> Tuple[int, int, int]:
        master = self.db.get_master()
        if master.empty:
            return 0, 0, 0

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
        failed = 0
        rows = 0
        source_summary = {"official": 0, "yahoo": 0, "none": 0}

        total = len(master)
        for idx, (_, row) in enumerate(master.iterrows(), start=1):
            if cancel_cb and cancel_cb():
                raise OperationCancelled("使用者中斷每日增量更新")
            stock_id = str(row["stock_id"])
            market = str(row["market"])
            official_df = official_map.get(stock_id, pd.DataFrame())
            used_source = ""
            write_df = pd.DataFrame()

            if not official_df.empty:
                write_df = official_df.copy()
                used_source = "official"
            else:
                yahoo_df = self.download_latest_bar_yahoo(stock_id, market, days="7d")
                if yahoo_df is not None and not yahoo_df.empty:
                    write_df = yahoo_df.copy()
                    used_source = "yahoo"

            if not write_df.empty:
                self.db.upsert_price_history(stock_id, write_df)
                actual_rows = len(write_df)
                rows += actual_rows
                success += 1
                source_summary[used_source] += 1
                if log_cb and (idx % 20 == 0 or idx == total or used_source == "yahoo"):
                    src_name = "官方" if used_source == "official" else "Yahoo備援"
                    log_cb(f"[{idx}/{total}] {stock_id} 每日資料更新 {actual_rows} 筆｜來源 {src_name}")
                if progress_cb:
                    progress_cb(idx, total, stock_id, actual_rows, used_source)
            else:
                failed += 1
                source_summary["none"] += 1
                if log_cb and (idx % 50 == 0 or idx == total):
                    log_cb(f"[{idx}/{total}] {stock_id} 今日無官方資料，Yahoo 備援亦未取到")
                if progress_cb:
                    progress_cb(idx, total, stock_id, 0, "skip")

        if log_cb:
            log_cb(f"每日更新彙總｜官方 {source_summary['official']} 檔｜Yahoo備援 {source_summary['yahoo']} 檔｜未取到 {source_summary['none']} 檔")
        return success, failed, rows
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


class IndicatorEngine:
    """相容層：舊版仍呼叫 IndicatorEngine.attach(...)，統一導向 DataEngine.attach(...)。"""

    @staticmethod
    def attach(df: pd.DataFrame) -> pd.DataFrame:
        return DataEngine.attach(df)


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

    def rebuild(self, progress_cb=None, log_cb=None, cancel_cb=None):
        master = self.db.get_master()
        today = datetime.now().strftime("%Y-%m-%d")
        rows = []
        total = len(master)
        success = 0
        skipped = 0

        for idx, (_, row) in enumerate(master.iterrows(), start=1):
            if cancel_cb and cancel_cb():
                raise OperationCancelled("使用者中斷重建排行")
            stock_id = str(row["stock_id"])
            hist = self.db.get_price_history(stock_id)
            if hist.empty or len(hist) < 70:
                skipped += 1
                if progress_cb:
                    progress_cb(idx, total, stock_id, success, 0, skipped, "skip")
                continue
            hist = DataEngine.attach(hist)
            score = StrategyEngine.score(hist)
            rows.append({
                "date": today,
                "stock_id": stock_id,
                **score,
                "rank_all": 0,
                "rank_industry": 0
            })
            success += 1
            if progress_cb:
                progress_cb(idx, total, stock_id, success, 0, skipped, "ok")
            if log_cb and (idx % 100 == 0 or idx == total):
                log_cb(f"重排行進度 {idx}/{total}｜已納入 {success} 檔｜跳過 {skipped} 檔")

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
            score += 25
        if pd.notna(last["ma60"]) and last["close"] > last["ma60"]:
            score += 20
        if pd.notna(last["ma20"]) and pd.notna(last["ma60"]) and last["ma20"] > last["ma60"]:
            score += 20
        if pd.notna(last["macd_hist"]) and last["macd_hist"] > 0:
            score += 20
        if pd.notna(last["rsi14"]) and 50 <= last["rsi14"] <= 72:
            score += 15
        return round(score, 2)

    def _breadth_score(self) -> float:
        ranking = self.db.get_latest_ranking()
        if ranking is None or ranking.empty:
            return 50.0
        up = float((ranking["signal"].isin(["強勢追蹤", "整理偏多"])).mean() * 100)
        return round(up, 2)

    def get_market_regime(self) -> dict:
        s_2330 = self._score_proxy("2330")
        s_0050 = self._score_proxy("0050")
        breadth = self._breadth_score()
        score = round(s_2330 * 0.4 + s_0050 * 0.25 + breadth * 0.35, 2)

        if score >= 68:
            regime = "多頭"
            memo = "指數與領頭股結構偏強，可放寬門檻並增加出手檔數。"
            max_positions = 8
            min_win_rate = 70.0
            rsi_low, rsi_high = 50.0, 72.0
        elif score <= 42:
            regime = "空頭"
            memo = "市場偏弱，防守優先，只保留極少數高勝率或防守型 ETF。"
            max_positions = 1
            min_win_rate = 80.0
            rsi_low, rsi_high = 48.0, 68.0
        else:
            regime = "震盪"
            memo = "市場分化，精選出手，不為了湊數而放寬條件。"
            max_positions = 4
            min_win_rate = 75.0
            rsi_low, rsi_high = 50.0, 70.0

        return {
            "regime": regime,
            "score": score,
            "memo": memo,
            "max_positions": max_positions,
            "min_win_rate": min_win_rate,
            "rsi_low": rsi_low,
            "rsi_high": rsi_high,
            "breadth": breadth,
        }


class ThemeStrengthEngine:
    PREFERRED_KEYWORDS = ["AI", "CPO", "Server", "伺服器", "半導體", "晶圓", "ASIC", "RISC-V", "光", "散熱", "HVDC", "網通"]

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
        out = x[(x["count"] >= 1) & (x["avg_total"] >= 55)]["theme"].astype(str).tolist()
        preferred = []
        for theme in x["theme"].astype(str).tolist():
            if any(k.lower() in theme.lower() for k in ThemeStrengthEngine.PREFERRED_KEYWORDS):
                preferred.append(theme)
        return list(dict.fromkeys(preferred + out))


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




V62_KLINE_SCORE = {
    "突破強勢": 100, "強勢追蹤": 90, "整理偏多": 75, "偏多觀察": 68,
    "區間整理": 55, "轉弱警戒": 25, "急跌風險": 10,
}
V62_WAVE_SCORE = {
    "第3浪": 100, "推動浪": 92, "修正浪": 70, "整理浪": 60, "第5浪": 35,
}
V62_SAKATA_SCORE = {
    "拉回承接": 95, "偏多低接": 88, "整理偏多": 75, "區間低接": 70,
    "突破追價": 52, "觀望": 20,
}
V62_VOLUME_SCORE = {
    "買盤明顯偏強": 100, "買盤偏強": 88, "多空均衡": 60, "賣盤偏強": 28,
}
V62_WEIGHTS = {
    "kline": 0.18, "wave": 0.22, "fib": 0.14, "sakata": 0.14, "volume": 0.16, "indicator": 0.16,
}



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

    @staticmethod
    def _in_entry_zone(close_: float, entry_low: float, entry_high: float) -> bool:
        try:
            return float(entry_low) <= float(close_) <= float(entry_high)
        except Exception:
            return False

    @staticmethod
    def _ui_trade_state(decision: str, close_: float, entry_low: float, entry_high: float, rr: float, win_rate: float) -> str:
        if decision == "BUY":
            return "可買"
        if decision == "WEAK BUY":
            if TradingPlanEngine._in_entry_zone(close_, entry_low, entry_high):
                return "準備買"
            return "預掛單"
        if rr >= 1.0 and win_rate >= 50:
            return "觀察"
        return "不可買"

    @staticmethod
    def _clamp(v: float, low: float = 0.0, high: float = 100.0) -> float:
        return max(low, min(high, float(v)))

    def _map_kline_signal(self, source_signal: str, close_: float, recent_high: float, ma5: float, ma10: float, ma20: float, ma60: float, macd_hist: float, rsi: float) -> str:
        signal = str(source_signal or "").strip()
        breakout = recent_high > 0 and close_ >= recent_high * 0.995
        strong_trend = close_ > ma5 > ma10 > ma20
        mild_trend = close_ >= ma20 and ma20 >= ma60

        # 分散性優先：先用結構與過熱程度切開，不讓大量樣本都落在強勢追蹤
        if breakout and strong_trend and macd_hist > 0 and 48 <= rsi <= 62:
            return "突破強勢"
        if breakout and rsi > 72:
            return "區間整理"
        if signal == "強勢追蹤" and strong_trend and 50 <= rsi <= 70:
            return "強勢追蹤"
        if signal == "整理偏多" and mild_trend:
            return "整理偏多"
        if signal == "中性觀察":
            if mild_trend and macd_hist >= 0 and rsi < 68:
                return "偏多觀察"
            return "區間整理"
        if close_ > ma20 > ma60 and macd_hist > 0 and 45 <= rsi <= 68:
            return "強勢追蹤"
        if close_ >= ma20 and macd_hist >= -0.02 and 40 <= rsi <= 65:
            return "偏多觀察"
        if close_ < ma20 and (rsi < 40 or macd_hist < 0):
            return "轉弱警戒"
        if close_ < ma60 and rsi < 32:
            return "急跌風險"
        return "區間整理"
        if close_ > ma20 > ma60 and macd_hist > 0:
            return "強勢追蹤"
        if close_ >= ma20:
            return "偏多觀察"
        if close_ < ma20 and rsi < 40:
            return "轉弱警戒"
        return "區間整理"

    def _wave_position(self, x: pd.DataFrame) -> str:
        recent = x.tail(55).copy()
        last = recent.iloc[-1]
        close_ = float(last["close"])
        recent_high = float(recent["high"].max())
        recent_low = float(recent["low"].min())
        ma20 = float(last["ma20"]) if pd.notna(last["ma20"]) else close_
        ma60 = float(last["ma60"]) if pd.notna(last["ma60"]) else close_
        rsi = float(last["rsi14"]) if pd.notna(last["rsi14"]) else 50.0
        macd_hist = float(last["macd_hist"]) if pd.notna(last["macd_hist"]) else 0.0

        zone_width = max(recent_high - recent_low, 1e-6)
        pos = (close_ - recent_low) / zone_width
        breakout = close_ >= recent_high * 0.995

        # 分散性優先，不讓大量股票都落在第3浪
        if breakout and ma20 > ma60 and macd_hist > 0 and 52 <= rsi <= 62 and 0.55 <= pos <= 0.85:
            return "第3浪"
        if breakout and (rsi > 72 or pos > 0.9):
            return "第5浪"
        if close_ > ma20 > ma60 and macd_hist > 0 and 0.40 <= pos < 0.75:
            return "推動浪"
        if close_ < ma20 and macd_hist < 0:
            return "修正浪"
        return "整理浪"

    def _fib_score_and_targets(self, close_: float, support: float, resistance: float) -> tuple[float, float, float]:
        """
        動態費波模組（v6.5 PRO）：
        - 依 close 在 support~resistance 區間的位置評分
        - 明確拉開分布，避免大量集中在 80~90
        """
        if resistance <= support or support <= 0:
            return 0.0, 0.0, 0.0

        width = max(resistance - support, 1e-6)
        pos = (close_ - support) / width

        if pos < 0.3:
            base = 95.0
        elif pos < 0.6:
            base = 80.0
        elif pos < 0.85:
            base = 65.0
        else:
            base = 45.0

        # 若跌破支撐或過度突破壓力，再額外調整
        if pos < 0:
            base = 35.0
        elif pos > 1.05:
            base = 38.0

        fib1382 = support + width * 1.382
        fib1618 = support + width * 1.618
        return round(self._clamp(base), 2), round(fib1382, 2), round(fib1618, 2)

    def _sakata_label(self, signal: str, close_: float, ma5: float, ma10: float, ma20: float, recent_high: float) -> str:
        # 依 SOP 使用「交易類型」代理阪田模組
        if signal == "突破強勢":
            return "突破追價" if close_ >= recent_high * 0.995 else "拉回承接"
        if signal == "強勢追蹤":
            return "拉回承接" if close_ <= ma5 * 1.01 else "偏多低接"
        if signal == "整理偏多":
            return "整理偏多"
        if signal == "偏多觀察":
            return "區間低接" if close_ >= ma20 else "觀望"
        if signal == "區間整理":
            return "區間低接"
        return "觀望"

    def _volume_label(self, vol_ratio: float, close_: float, ma20: float) -> str:
        if vol_ratio >= 1.4 and close_ >= ma20:
            return "買盤明顯偏強"
        if vol_ratio >= 1.05 and close_ >= ma20:
            return "買盤偏強"
        if vol_ratio >= 0.8:
            return "多空均衡"
        return "賣盤偏強"

    def _indicator_score(self, rsi: float, macd_hist: float, k: float, d: float) -> float:
        # RSI 分段強化（v6.5 PRO）：極端值明確扣分，避免過熱/過弱仍拿高分
        if 45 <= rsi <= 65:
            base = 100.0
        elif 40 <= rsi < 45:
            base = 82.0
        elif 65 < rsi <= 70:
            base = 78.0
        elif 70 < rsi <= 72:
            base = 68.0
        elif 35 <= rsi < 40:
            base = 50.0
        elif 72 < rsi <= 75:
            base = 40.0
        elif 75 < rsi <= 80:
            base = 25.0
        elif rsi > 80:
            base = 10.0
        elif 30 <= rsi < 35:
            base = 25.0
        else:
            base = 12.0

        if macd_hist > 0:
            base += 5
        else:
            base -= 3

        if k >= d:
            base += 3
        else:
            base -= 2

        return round(self._clamp(base), 2)

    def _decision(self, total_score: float, rr: float, rsi: float, wave_label: str, signal: str) -> str:
        if total_score >= 85 and rr >= 1.6 and rsi <= 72 and wave_label in ("第3浪", "推動浪"):
            return "BUY"
        if total_score >= 75 and rr >= 1.2 and signal in ("突破強勢", "強勢追蹤", "整理偏多", "偏多觀察"):
            return "WEAK BUY"
        if total_score >= 60 and rr >= 1.0:
            return "HOLD"
        return "AVOID"

    def build_plan(self, stock_id: str) -> dict:
        stock = self.db.get_stock_row(stock_id)
        hist = self.db.get_price_history(stock_id)
        if stock is None or hist.empty or len(hist) < 70:
            return {
                "stock_id": stock_id,
                "stock_name": stock["stock_name"] if stock is not None else stock_id,
                "theme": stock["theme"] if stock is not None else "",
                "industry": stock["industry"] if stock is not None else "",
                "trade_action": "AVOID",
                "ui_state": "不可買",
                "entry_low": 0.0,
                "entry_high": 0.0,
                "entry_zone": "-",
                "stop_loss": "-",
                "target_price": "-",
                "rr": 0.0,
                "win_grade": "C",
                "win_rate": 45.0,
                "selection_score": 0.0,
                "trade_score": 0.0,
                "bucket": "排除",
                "reason": "資料不足",
                "wave": "資料不足",
                "rsi": 50.0,
                "trend_ok": 0,
                "kd_ok": 0,
                "macd_ok": 0,
                "volume_ok": 0,
                "decision": "AVOID",
                "support": 0.0,
                "resistance": 0.0,
                "model_score": 0.0,
                "kline_score": 0.0,
                "wave_score": 0.0,
                "fib_score": 0.0,
                "sakata_score": 0.0,
                "volume_score": 0.0,
                "indicator_score": 0.0,
                "target_1382": 0.0,
                "target_1618": 0.0,
                "signal": "資料不足",
                "trade_type": "觀望",
                "sakata_label": "觀望",
                "volume_label": "多空均衡",
            }

        x = IndicatorEngine.attach(hist.copy())
        last = x.iloc[-1]
        score = StrategyEngine.score(x)
        is_etf = self._is_etf(stock)

        close_ = float(last["close"])
        ma5 = float(last["ma5"]) if pd.notna(last["ma5"]) else close_
        ma10 = float(last["ma10"]) if pd.notna(last["ma10"]) else close_
        ma20 = float(last["ma20"]) if pd.notna(last["ma20"]) else close_
        ma60 = float(last["ma60"]) if pd.notna(last["ma60"]) else close_
        rsi = float(last["rsi14"]) if pd.notna(last["rsi14"]) else 50.0
        macd_hist = float(last["macd_hist"]) if pd.notna(last["macd_hist"]) else 0.0
        k = float(last["k"]) if pd.notna(last["k"]) else 50.0
        d = float(last["d"]) if pd.notna(last["d"]) else 50.0

        recent = x.tail(60)
        recent_high = float(recent["high"].max())
        recent_low = float(recent["low"].min())
        support = min(ma20, recent["low"].tail(20).min()) if pd.notna(ma20) else recent_low
        resistance = recent_high
        support = float(support) if pd.notna(support) else recent_low
        resistance = float(resistance) if pd.notna(resistance) else close_

        source_signal = str(score["signal"])
        signal = self._map_kline_signal(source_signal, close_, recent_high, ma5, ma10, ma20, ma60, macd_hist, rsi)
        wave_label = self._wave_position(x)
        sakata_label = self._sakata_label(signal, close_, ma5, ma10, ma20, recent_high)
        vol_ma20 = x["volume"].tail(20).mean()
        vol_ratio = float(last["volume"] / vol_ma20) if vol_ma20 and pd.notna(vol_ma20) else 1.0
        volume_label = self._volume_label(vol_ratio, close_, ma20)

        kline_score = float(V62_KLINE_SCORE.get(signal, 55))
        wave_score = float(V62_WAVE_SCORE.get(wave_label, 60))
        fib_score, fib1382, fib1618 = self._fib_score_and_targets(close_, support, resistance)
        sakata_score = float(V62_SAKATA_SCORE.get(sakata_label, 20))
        volume_score = float(V62_VOLUME_SCORE.get(volume_label, 60))
        indicator_score = float(self._indicator_score(rsi, macd_hist, k, d))

        model_score = round(
            kline_score * V62_WEIGHTS["kline"] +
            wave_score * V62_WEIGHTS["wave"] +
            fib_score * V62_WEIGHTS["fib"] +
            sakata_score * V62_WEIGHTS["sakata"] +
            volume_score * V62_WEIGHTS["volume"] +
            indicator_score * V62_WEIGHTS["indicator"], 2
        )

        breakout_type = signal in ("突破強勢", "強勢追蹤") and close_ >= resistance * 0.985
        if breakout_type:
            entry_low = resistance * 1.005
            entry_high = entry_low
            trade_type = "突破型"
        else:
            entry_low = support * 1.005
            entry_high = support * 1.015
            trade_type = "低接型"

        stop = support * 0.97
        target = fib1618 if breakout_type else fib1382
        risk = max(entry_high - stop, 0.01)
        reward = max(target - entry_high, 0.0)
        rr = round(reward / risk, 2)

        trend_ok = int(close_ > ma5 > ma10 > ma20)
        macd_ok = int(macd_hist > 0)
        kd_ok = int(k >= d)
        volume_ok = int(vol_ratio >= 1.0)

        win_grade, win_rate = WinRateEngine.estimate(hist)
        decision = self._decision(model_score, rr, rsi, wave_label, signal)

        if is_etf:
            bucket = "防守"
        elif decision == "BUY":
            bucket = "主攻"
        elif decision == "WEAK BUY":
            bucket = "次強"
        elif decision == "HOLD":
            bucket = "觀察"
        else:
            bucket = "排除"

        preferred_theme = any(key.lower() in str(stock.get("theme", "")).lower() for key in ThemeStrengthEngine.PREFERRED_KEYWORDS)
        selection_score = round(model_score * 0.7 + win_rate * 0.15 + min(rr, 3.0) * 5 + (8 if decision == "BUY" else 4 if decision == "WEAK BUY" else 0), 2)
        trade_score = round(model_score * 0.45 + score["ai_score"] * 0.15 + win_rate * 0.2 + min(rr, 3.0) * 6 + (8 if preferred_theme else 0), 2)

        reason = (
            f"{signal}｜{wave_label}｜{trade_type}｜{volume_label}｜"
            f"六模組 {model_score:.1f}｜RR {rr:.2f}｜RSI {rsi:.1f}"
        )

        ui_state = self._ui_trade_state(decision, close_, entry_low, entry_high, rr, win_rate)

        return {
            "stock_id": stock_id,
            "stock_name": stock["stock_name"],
            "industry": stock["industry"],
            "theme": stock["theme"],
            "market": stock["market"],
            "is_etf": 1 if is_etf else 0,
            "trade_action": decision,
            "ui_state": ui_state,
            "entry_low": round(entry_low, 2),
            "entry_high": round(entry_high, 2),
            "entry_zone": f"{self._round_price(entry_low)} ~ {self._round_price(entry_high)}",
            "stop_loss": self._round_price(stop),
            "target_price": self._round_price(target),
            "target_1382": fib1382,
            "target_1618": fib1618,
            "rr": rr,
            "win_grade": win_grade,
            "win_rate": win_rate,
            "selection_score": selection_score,
            "trade_score": trade_score,
            "bucket": bucket,
            "reason": reason,
            "wave": wave_label,
            "rsi": round(rsi, 2),
            "trend_ok": trend_ok,
            "kd_ok": kd_ok,
            "macd_ok": macd_ok,
            "volume_ok": volume_ok,
            "decision": decision,
            "signal": signal,
            "trade_type": trade_type,
            "support": round(support, 2),
            "resistance": round(resistance, 2),
            "kline_score": round(kline_score, 2),
            "wave_score": round(wave_score, 2),
            "fib_score": round(fib_score, 2),
            "sakata_score": round(sakata_score, 2),
            "volume_score": round(volume_score, 2),
            "indicator_score": round(indicator_score, 2),
            "model_score": model_score,
            "sakata_label": sakata_label,
            "volume_label": volume_label,
        }


class MasterTradingEngine:
    def __init__(self, db: DBManager):
        self.db = db
        self.market_engine = MarketRegimeEngine(db)
        self.plan_engine = TradingPlanEngine(db)

    def get_trade_pool(self, filtered_df: pd.DataFrame, progress_cb=None, log_cb=None, cancel_cb=None) -> dict:
        if filtered_df.empty:
            empty = pd.DataFrame()
            market = self.market_engine.get_market_regime()
            return {
                "market": market, "trade_top20": empty, "attack": empty, "watch": empty, "defense": empty,
                "today_buy": empty, "wait_pullback": empty, "theme_summary": empty
            }

        base = filtered_df.copy()
        hot_themes = ThemeStrengthEngine.get_hot_themes(base)

        plans = []
        sids = base["stock_id"].astype(str).tolist()
        total = len(sids)
        for idx2, sid in enumerate(sids, start=1):
            if cancel_cb and cancel_cb():
                raise OperationCancelled("使用者中斷 AI選股TOP20")
            plans.append(self.plan_engine.build_plan(sid))
            if progress_cb:
                progress_cb(idx2, total, sid)
            if log_cb and (idx2 % 100 == 0 or idx2 == total):
                log_cb(f"AI選股分析進度 {idx2}/{total}｜{sid}")
        plans_df = pd.DataFrame(plans)

        market = self.market_engine.get_market_regime()
        if plans_df.empty:
            empty = pd.DataFrame()
            return {
                "market": market, "trade_top20": empty, "attack": empty, "watch": empty, "defense": empty,
                "today_buy": empty, "wait_pullback": empty, "theme_summary": ThemeStrengthEngine.summarize(base)
            }

        preferred_mask = plans_df["theme"].isin(hot_themes) if hot_themes else pd.Series([True] * len(plans_df), index=plans_df.index)

        # 依 SOP 順序：先篩決策，再支撐>0，再壓力>支撐，最後按六模組總分排序
        tradable = plans_df[
            (plans_df["decision"].isin(["BUY", "WEAK BUY"])) &
            (plans_df["support"] > 0) &
            (plans_df["resistance"] > plans_df["support"])
        ].copy()

        if not tradable.empty:
            tradable["decision_rank"] = tradable["decision"].map({"BUY": 2, "WEAK BUY": 1}).fillna(0)
            tradable["preferred_rank"] = preferred_mask.reindex(tradable.index).fillna(False).astype(int)
            tradable = tradable.sort_values("model_score", ascending=False)

        trade_top20 = tradable.head(20).copy()  # 依你的要求，TOP20 保留

        attack = trade_top20[trade_top20["decision"] == "BUY"].copy()
        watch = trade_top20[trade_top20["decision"] == "WEAK BUY"].copy()

        defense = plans_df[
            (plans_df["is_etf"] == 1) &
            (plans_df["support"] > 0) &
            (plans_df["resistance"] > plans_df["support"])
        ].copy()
        if not defense.empty:
            defense = defense.sort_values(["model_score", "trade_score", "rr", "win_rate"], ascending=False).head(10)

        today_buy = tradable[
            (tradable["decision"] == "BUY") &
            (tradable["rr"] >= 1.2) &
            (tradable["win_rate"] >= max(55.0, market["min_win_rate"] - 10))
        ].sort_values("model_score", ascending=False)

        wait_pullback = tradable[
            (tradable["decision"] == "WEAK BUY") &
            (tradable["rr"] >= 1.0)
        ].sort_values("model_score", ascending=False)

        dynamic_n = max(1, min(10, market["max_positions"] + 2))

        return {
            "market": market,
            "trade_top20": trade_top20,
            "attack": attack.head(10),
            "watch": watch.head(10),
            "defense": defense.head(10),
            "today_buy": today_buy.head(dynamic_n),
            "wait_pullback": wait_pullback.head(dynamic_n),
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




class BacktestEngine:
    """
    v6.5 PRO：
    - 以近 240 根歷史資料做簡易策略回測
    - 輸出勝率、平均報酬、平均RR、樣本數
    """
    def __init__(self, db: DBManager):
        self.db = db

    def estimate_trade_quality(self, stock_id: str) -> dict:
        hist = self.db.get_price_history(stock_id)
        if hist is None or hist.empty or len(hist) < 140:
            return {"backtest_win_rate": 45.0, "avg_return": 0.0, "avg_rr": 1.0, "samples": 0}

        x = IndicatorEngine.attach(hist.copy()).tail(260).reset_index(drop=True)
        trades = []

        for i in range(70, len(x) - 6):
            row = x.iloc[i]
            entry = float(row["close"])
            support = float(x.iloc[max(0, i-20):i+1]["low"].min())
            resistance = float(x.iloc[max(0, i-60):i+1]["high"].max())
            if support <= 0 or resistance <= support:
                continue

            signal_like = (
                (pd.notna(row["ma20"]) and pd.notna(row["ma60"]) and row["close"] > row["ma20"] >= row["ma60"]) and
                (pd.notna(row["macd_hist"]) and row["macd_hist"] > 0)
            )
            if not signal_like:
                continue

            stop = support * 0.97
            risk = max(entry - stop, 0.01)
            target = support + (resistance - support) * 1.382
            rr = max((target - entry) / risk, 0.0)

            future = x.iloc[i+1:i+6]
            max_hi = float(future["high"].max())
            min_lo = float(future["low"].min())
            exit_close = float(future.iloc[-1]["close"])

            if max_hi >= target:
                ret = (target / entry) - 1
                win = 1
            elif min_lo <= stop:
                ret = (stop / entry) - 1
                win = 0
            else:
                ret = (exit_close / entry) - 1
                win = 1 if ret > 0 else 0

            trades.append({"win": win, "ret": ret, "rr": rr})

        if not trades:
            return {"backtest_win_rate": 45.0, "avg_return": 0.0, "avg_rr": 1.0, "samples": 0}

        t = pd.DataFrame(trades)
        return {
            "backtest_win_rate": round(float(t["win"].mean() * 100), 2),
            "avg_return": round(float(t["ret"].mean() * 100), 2),
            "avg_rr": round(float(t["rr"].mean()), 2),
            "samples": int(len(t)),
        }


class AppUI:
    def __init__(self, root, db: DBManager):
        self.root = root
        self.db = db
        self.data_engine = DataEngine(db)
        self.rank_engine = RankingEngine(db)
        self.master_trading_engine = MasterTradingEngine(db)
        self.backtest_engine = BacktestEngine(db)
        self.last_top20_df = pd.DataFrame()
        self.last_top5_df = pd.DataFrame()
        self.last_theme_summary_df = pd.DataFrame()
        self.last_attack_df = pd.DataFrame()
        self.last_watch_df = pd.DataFrame()
        self.last_defense_df = pd.DataFrame()
        self.last_order_list_df = pd.DataFrame()
        self.last_today_buy_df = pd.DataFrame()
        self.last_wait_df = pd.DataFrame()
        self.current_chart_path = None
        self.worker = None
        self.cancel_event = threading.Event()
        self.current_job = None
        self.history_batch_size = 25
        self.history_sleep_sec = 0.6
        self.last_job_summary = {}

        self.root.title(APP_NAME)
        self.root.geometry("1580x920")

        self.market_var = tk.StringVar(value="全部")
        self.industry_var = tk.StringVar(value="全部")
        self.theme_var = tk.StringVar(value="全部")
        self.search_var = tk.StringVar(value="")

        self._build_ui()
        self.refresh_filters()
        self.show_welcome_message()
        self.refresh_all_tables()
        self.set_status(f"PACKED={PACKED_DATA_DIR} | EXTERNAL={EXTERNAL_DATA_DIR} | CSV={MASTER_CSV}")

    def show_welcome_message(self):
        last_date = self.db.get_last_price_date() or "尚未建立"
        ranking_count = self.db.get_ranking_rows_count()
        price_rows = self.db.get_total_price_rows()
        lines = [
            "《GTC AI Trading System v6.5 PRO-TRADING-SYSTEM》",
            "",
            f"主檔狀態：{len(self.db.get_master())} 檔",
            f"歷史資料：{price_rows} 筆｜最後交易日：{last_date}",
            f"最新排行筆數：{ranking_count}",
            "",
            "建議操作順序：",
            "1. 初始化全市場（第一次或要重整主檔時）",
            "2. 建立完整歷史（第一次建庫）",
            "3. 每日增量更新",
            "4. 重建排行",
            "5. AI選股TOP20",
            "6. 採用 v6.5 PRO 六模組交易模型 / TOP5 / 回測勝率 / 預掛單 / 即時Log / 下單清單",
        ]
        self.detail.delete("1.0", tk.END)
        self.detail.insert("1.0", "\n".join(lines))

    def ensure_ranking_ready(self, auto_rebuild: bool = False) -> bool:
        ranking = self.db.get_latest_ranking()
        if ranking is not None and not ranking.empty:
            return True
        if auto_rebuild and self.db.get_total_price_rows() > 0:
            try:
                count = self.rank_engine.rebuild()
                return count > 0
            except Exception:
                return False
        return False

    def _build_ui(self):
        toolbar = ttk.Frame(self.root, padding=8)
        toolbar.pack(fill="x")

        row1 = ttk.Frame(toolbar)
        row1.pack(fill="x", pady=(0, 6))
        row2 = ttk.Frame(toolbar)
        row2.pack(fill="x")

        ttk.Label(row1, text="市場").pack(side="left")
        self.market_cb = ttk.Combobox(row1, textvariable=self.market_var, width=12, state="readonly")
        self.market_cb.pack(side="left", padx=4)

        ttk.Label(row1, text="產業").pack(side="left")
        self.industry_cb = ttk.Combobox(row1, textvariable=self.industry_var, width=16, state="readonly")
        self.industry_cb.pack(side="left", padx=4)

        ttk.Label(row1, text="題材").pack(side="left")
        self.theme_cb = ttk.Combobox(row1, textvariable=self.theme_var, width=18, state="readonly")
        self.theme_cb.pack(side="left", padx=4)

        ttk.Label(row1, text="搜尋").pack(side="left")
        ttk.Entry(row1, textvariable=self.search_var, width=16).pack(side="left", padx=4)

        self.btn_filter = ttk.Button(row1, text="套用篩選", command=self.refresh_all_tables)
        self.btn_filter.pack(side="left", padx=4)

        self.status_label = ttk.Label(row1, text="系統就緒")
        self.status_label.pack(side="right")

        ttk.Label(row2, text="功能").pack(side="left")
        self.action_var = tk.StringVar(value="AI選股TOP20")
        self.action_cb = ttk.Combobox(row2, textvariable=self.action_var, width=18, state="readonly")
        self.action_cb["values"] = [
            "AI選股TOP20",
            "AI選股TOP5",
            "初始化全市場",
            "建立完整歷史（一次）",
            "續跑建庫",
            "每日增量更新",
            "重建排行",
            "中斷作業",
            "匯出分析Excel",
            "開啟圖表",
        ]
        self.action_cb.pack(side="left", padx=4)
        self.btn_run_action = ttk.Button(row2, text="執行功能", command=self.execute_action)
        self.btn_run_action.pack(side="left", padx=(4, 12))

        ttk.Label(row2, text="下載").pack(side="left")
        self.download_target_var = tk.StringVar(value="TOP20")
        self.download_target_cb = ttk.Combobox(row2, textvariable=self.download_target_var, width=12, state="readonly")
        self.download_target_cb["values"] = ["TOP20", "TOP5", "今日可買", "等待拉回", "預掛單", "主攻", "次強", "防守", "下單清單", "排行", "類股", "題材"]
        self.download_target_cb.pack(side="left", padx=4)
        self.btn_export_data = ttk.Button(row2, text="下載資料", command=self.export_selected_data)
        self.btn_export_data.pack(side="left", padx=(4, 12))

        self.btn_export_excel = ttk.Button(row2, text="匯出分析Excel", command=self.export_analysis_excel)
        self.btn_export_excel.pack(side="left", padx=4)
        self.btn_open_chart = ttk.Button(row2, text="開啟圖表", command=self.open_current_chart)
        self.btn_open_chart.pack(side="left", padx=4)

        self.progress_var = tk.DoubleVar(value=0)
        self.progress = ttk.Progressbar(row2, variable=self.progress_var, maximum=100, length=180, mode="determinate")
        self.progress.pack(side="left", padx=(12, 6))
        self.progress_text_var = tk.StringVar(value="0% | 0/0 | 成功 0 | 失敗 0")
        self.progress_text_label = ttk.Label(row2, textvariable=self.progress_text_var, width=44)
        self.progress_text_label.pack(side="left", padx=4)

        main = ttk.Panedwindow(self.root, orient="horizontal")
        main.pack(fill="both", expand=True, padx=8, pady=8)

        self.left_notebook = ttk.Notebook(main)
        right = ttk.Frame(main, padding=8)
        main.add(self.left_notebook, weight=3)
        main.add(right, weight=2)

        self.tab_rank = ttk.Frame(self.left_notebook)
        self.tab_sector = ttk.Frame(self.left_notebook)
        self.tab_theme = ttk.Frame(self.left_notebook)
        self.tab_top20 = ttk.Frame(self.left_notebook)
        self.tab_top5 = ttk.Frame(self.left_notebook)
        self.tab_order = ttk.Frame(self.left_notebook)
        self.left_notebook.add(self.tab_rank, text="排行榜")
        self.left_notebook.add(self.tab_sector, text="類股熱度")
        self.left_notebook.add(self.tab_theme, text="題材輪動")
        self.left_notebook.add(self.tab_top20, text="AI交易TOP20")
        self.left_notebook.add(self.tab_top5, text="AI選股TOP5")
        self.left_notebook.add(self.tab_order, text="下單清單")

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


        self.top20_tree = self._make_tree(self.tab_top20, ("rank", "id", "name", "bucket", "ui_action", "entry", "stop", "target1382", "target1618", "rr", "win_rate"), {
            "rank": "排序", "id": "代號", "name": "名稱", "bucket": "分類", "ui_action": "狀態", "entry": "進場區", "stop": "停損", "target1382": "1.382", "target1618": "1.618", "rr": "RR", "win_rate": "勝率%"
        })
        self.top20_tree.bind("<<TreeviewSelect>>", self.on_select_top20)

        self.top5_tree = self._make_tree(self.tab_top5, ("rank", "id", "name", "state", "entry", "stop", "target1382", "rr", "win_rate", "backtest"), {
            "rank": "排序", "id": "代號", "name": "名稱", "state": "狀態", "entry": "進場區", "stop": "停損", "target1382": "1.382", "rr": "RR", "win_rate": "勝率%", "backtest": "回測勝率%"
        })
        self.top5_tree.bind("<<TreeviewSelect>>", self.on_select_top5)

        self.order_tree = self._make_tree(self.tab_order, ("priority", "id", "name", "bucket", "action", "entry", "stop", "target1382", "target1618", "rr", "win_rate", "qty", "risk_note"), {
            "priority": "優先級", "id": "代號", "name": "名稱", "bucket": "分類", "action": "狀態", "entry": "進場區", "stop": "停損", "target1382": "1.382", "target1618": "1.618", "rr": "RR", "win_rate": "勝率%", "qty": "建議張數", "risk_note": "風險備註"
        })
        self.order_tree.bind("<<TreeviewSelect>>", self.on_select_order)

        upper = ttk.LabelFrame(right, text="個股 / 系統說明", padding=6)
        upper.pack(fill="both", expand=True)
        upper_body = ttk.Frame(upper)
        upper_body.pack(fill="both", expand=True)
        self.detail = tk.Text(upper_body, wrap="none", font=("Consolas", 11), height=18)
        self.detail_vsb = ttk.Scrollbar(upper_body, orient="vertical", command=self.detail.yview)
        self.detail_hsb = ttk.Scrollbar(upper_body, orient="horizontal", command=self.detail.xview)
        self.detail.configure(yscrollcommand=self.detail_vsb.set, xscrollcommand=self.detail_hsb.set)
        self.detail.grid(row=0, column=0, sticky="nsew")
        self.detail_vsb.grid(row=0, column=1, sticky="ns")
        self.detail_hsb.grid(row=1, column=0, sticky="ew")
        upper_body.rowconfigure(0, weight=1)
        upper_body.columnconfigure(0, weight=1)

        lower = ttk.LabelFrame(right, text="即時 Log 視窗", padding=6)
        lower.pack(fill="both", expand=True, pady=(8, 0))
        lower_body = ttk.Frame(lower)
        lower_body.pack(fill="both", expand=True)
        self.log_text = tk.Text(lower_body, wrap="none", font=("Consolas", 10), height=14)
        self.log_vsb = ttk.Scrollbar(lower_body, orient="vertical", command=self.log_text.yview)
        self.log_hsb = ttk.Scrollbar(lower_body, orient="horizontal", command=self.log_text.xview)
        self.log_text.configure(yscrollcommand=self.log_vsb.set, xscrollcommand=self.log_hsb.set)
        self.log_text.grid(row=0, column=0, sticky="nsew")
        self.log_vsb.grid(row=0, column=1, sticky="ns")
        self.log_hsb.grid(row=1, column=0, sticky="ew")
        lower_body.rowconfigure(0, weight=1)
        lower_body.columnconfigure(0, weight=1)

    def _make_tree(self, parent, cols, headers):
        frame = ttk.Frame(parent)
        frame.pack(fill="both", expand=True)
        tree = ttk.Treeview(frame, columns=cols, show="headings", height=28)
        for c in cols:
            tree.heading(c, text=headers[c])
            tree.column(c, width=140 if c not in ("rank", "count", "avg_total", "avg_ai", "id", "total", "ai") else 90, anchor="center")
        vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)
        return tree

    def set_status(self, text):
        self.status_label.config(text=text)
        self.root.update_idletasks()

    def set_progress(self, current=0, total=100, success=0, failed=0, sid="", skipped=0, stage=""):
        total = max(int(total), 1)
        current = max(0, min(int(current), total))
        self.progress.configure(maximum=total)
        self.progress_var.set(current)
        pct = (current / total) * 100 if total else 0
        stage_part = f"[{stage}] " if stage else ""
        sid_part = f" | {sid}" if sid else ""
        skip_part = f" | 跳過 {skipped}" if skipped else ""
        self.progress_text_var.set(f"{stage_part}{pct:5.1f}% | {current}/{total} | 成功 {success} | 失敗 {failed}{skip_part}{sid_part}")
        self.root.update_idletasks()

    def reset_progress(self):
        self.progress.configure(maximum=100)
        self.progress_var.set(0)
        self.progress_text_var.set("0% | 0/0 | 成功 0 | 失敗 0")
        self.root.update_idletasks()

    def start_task(self, stage: str, total: int = 100):
        self.set_status(f"{stage} 開始...")
        self.set_progress(0, total, 0, 0, stage=stage)

    def update_task(self, stage: str, current: int, total: int, success: int = 0, failed: int = 0, skipped: int = 0, item: str = ""):
        self.set_progress(current, total, success, failed, item, skipped=skipped, stage=stage)

    def finish_task(self, stage: str, summary: str = ""):
        self.set_status(summary or f"{stage} 完成")

    def append_log(self, text):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{ts}] {text}\n")
        self.log_text.see(tk.END)
        self.root.update_idletasks()

    def clear_log(self):
        self.log_text.delete("1.0", tk.END)

    def save_history_state(self, state: dict):
        try:
            STATE_PATH.write_text(pd.Series(state).to_json(force_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def load_history_state(self) -> dict:
        try:
            if STATE_PATH.exists():
                return pd.read_json(STATE_PATH, typ="series").to_dict()
        except Exception:
            pass
        return {}

    def clear_history_state(self):
        try:
            if STATE_PATH.exists():
                STATE_PATH.unlink()
        except Exception:
            pass

    def cancel_current_job(self):
        if self.worker is None or not self.worker.is_alive():
            return messagebox.showinfo("提醒", "目前沒有執行中的背景作業。")
        self.cancel_event.set()
        self.append_log("已收到中斷要求，將於本批或本檔完成後停止。")
        self.set_status("已發出中斷要求，請稍候…")

    def set_busy(self, busy: bool):
        normal_buttons = [
            self.btn_filter, self.action_cb, self.btn_run_action,
            self.btn_export_data, self.download_target_cb,
            self.btn_export_excel, self.btn_open_chart
        ]
        for btn in normal_buttons:
            try:
                btn.config(state="disabled" if busy else "readonly" if btn in (self.action_cb, self.download_target_cb) else "normal")
            except Exception:
                pass
        if busy:
            if self.action_var.get() == "中斷作業":
                self.action_var.set("AI選股TOP20")
        self.root.update_idletasks()

    def execute_action(self):
        action = (self.action_var.get() or "").strip()
        mapping = {
            "初始化全市場": self.init_master_data,
            "建立完整歷史（一次）": self.build_full_history_once,
            "續跑建庫": self.resume_full_history,
            "每日增量更新": self.update_data,
            "重建排行": self.rebuild_ranking,
            "AI選股TOP20": self.show_top20,
            "AI選股TOP5": self.show_top5,
            "匯出分析Excel": self.export_analysis_excel,
            "開啟圖表": self.open_current_chart,
            "中斷作業": self.cancel_current_job,
        }
        func = mapping.get(action)
        if func is None:
            return messagebox.showwarning("提醒", "請先選擇功能。")
        func()

    def ui_call(self, func, *args, **kwargs):
        self.root.after(0, lambda: func(*args, **kwargs))

    def _run_in_thread(self, target, name="worker"):
        if self.worker is not None and self.worker.is_alive():
            messagebox.showwarning("提醒", "背景作業進行中，請稍候。")
            return

        def runner():
            self.cancel_event.clear()
            self.current_job = name
            self.ui_call(self.set_busy, True)
            self.ui_call(self.reset_progress)
            try:
                target()
            finally:
                self.current_job = None
                self.ui_call(self.set_busy, False)

        self.worker = threading.Thread(target=runner, name=name, daemon=True)
        self.worker.start()

    def open_current_chart(self):
        if self.current_chart_path is None or not Path(self.current_chart_path).exists():
            return messagebox.showwarning("提醒", "目前沒有可開啟的圖表，請先點選股票。")
        open_path(Path(self.current_chart_path))


    def export_selected_data(self):
        target = self.download_target_var.get().strip() or "TOP20"

        def worker():
            mapping = {
                "TOP20": getattr(self, "last_top20_df", pd.DataFrame()),
                "TOP5": getattr(self, "last_top5_df", pd.DataFrame()),
                "今日可買": getattr(self, "last_today_buy_df", pd.DataFrame()),
                "等待拉回": getattr(self, "last_wait_df", pd.DataFrame()),
                "預掛單": getattr(self, "last_wait_df", pd.DataFrame()),
                "主攻": self.last_attack_df,
                "次強": self.last_watch_df,
                "防守": self.last_defense_df,
                "下單清單": self.last_order_list_df,
                "排行": self._filtered_ranking(),
                "類股": pd.DataFrame([(self.sector_tree.item(i, "values")) for i in self.sector_tree.get_children()], columns=["產業", "檔數", "平均總分", "平均AI分", "代表股"]) if self.sector_tree.get_children() else pd.DataFrame(),
                "題材": pd.DataFrame([(self.theme_tree.item(i, "values")) for i in self.theme_tree.get_children()], columns=["題材", "檔數", "平均總分", "平均AI分", "代表股"]) if self.theme_tree.get_children() else pd.DataFrame(),
            }
            df = mapping.get(target, pd.DataFrame())
            if df is None:
                df = pd.DataFrame()
            if df.empty:
                empty_columns = {
                    "TOP20": ["stock_id", "stock_name", "bucket", "ui_state", "entry_zone", "stop_loss", "target_1382", "target_1618", "rr", "win_rate"],
                    "TOP5": ["stock_id", "stock_name", "ui_state", "entry_zone", "stop_loss", "target_1382", "rr", "win_rate", "backtest_win_rate"],
                    "今日可買": ["stock_id", "stock_name", "ui_state", "entry_zone", "stop_loss", "target_1382", "target_1618", "rr", "win_rate"],
                    "等待拉回": ["stock_id", "stock_name", "ui_state", "entry_zone", "stop_loss", "target_1382", "target_1618", "rr", "win_rate"],
                    "預掛單": ["stock_id", "stock_name", "ui_state", "entry_zone", "stop_loss", "target_1382", "target_1618", "rr", "win_rate"],
                    "下單清單": ["優先級", "代號", "名稱", "分類", "狀態", "進場區", "停損", "1.382", "1.618", "RR", "勝率", "建議張數", "風險備註"],
                }
                df = pd.DataFrame(columns=empty_columns.get(target, ["message"]))
                if df.empty and target not in empty_columns:
                    df = pd.DataFrame([{"message": f"目前沒有可下載的【{target}】資料"}])
            try:
                self.ui_call(self.start_task, f"下載{target}", 3)
                self.ui_call(self.update_task, f"下載{target}", 1, 3, item="準備資料")
                base = RUNTIME_DIR / f"{target}_Data_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                self.ui_call(self.update_task, f"下載{target}", 2, 3, item="輸出檔案")
                out_path, out_type = write_table_bundle(base, {target: df}, preferred="excel")
                display_name = Path(out_path).name if isinstance(out_path, Path) else str(out_path)
                self.ui_call(self.update_task, f"下載{target}", 3, 3, success=1, item=display_name)
                self.ui_call(self.finish_task, f"下載{target}", f"{target} 資料已輸出：{display_name}")
                self.ui_call(messagebox.showinfo, "完成", f"{target} 資料已輸出（{out_type}）：\n{out_path}")
            except Exception as e:
                self.ui_call(messagebox.showerror, "錯誤", str(e))

        self._run_in_thread(worker, f"export_{target}")


    def export_analysis_excel(self):
        ranking = self._filtered_ranking()
        if ranking is None or ranking.empty:
            return messagebox.showwarning("提醒", "目前沒有可匯出的分析資料。")

        def worker():
            try:
                sector = pd.DataFrame()
                theme = pd.DataFrame()
                self.ui_call(self.start_task, "匯出分析", 5)
                self.ui_call(self.update_task, "匯出分析", 1, 5, item="整理排行")
                if not ranking.empty:
                    sector = ranking.groupby("industry", as_index=False).agg(count=("stock_id", "count"), avg_total=("total_score", "mean"), avg_ai=("ai_score", "mean")).sort_values(["avg_total", "avg_ai"], ascending=False)
                    theme = ranking.groupby("theme", as_index=False).agg(count=("stock_id", "count"), avg_total=("total_score", "mean"), avg_ai=("ai_score", "mean")).sort_values(["avg_total", "avg_ai"], ascending=False)
                detail_text = self.detail.get("1.0", tk.END).strip()
                tables = {"Ranking": ranking}
                if not sector.empty:
                    tables["Sector"] = sector
                if not theme.empty:
                    tables["Theme"] = theme
                if self.last_top20_df is not None and not self.last_top20_df.empty:
                    tables["Trade_TOP20"] = self.last_top20_df
                if self.last_top5_df is not None and not self.last_top5_df.empty:
                    tables["Trade_TOP5"] = self.last_top5_df
                if getattr(self, "last_today_buy_df", pd.DataFrame()) is not None and not getattr(self, "last_today_buy_df", pd.DataFrame()).empty:
                    tables["Today_Buy"] = self.last_today_buy_df
                if getattr(self, "last_wait_df", pd.DataFrame()) is not None and not getattr(self, "last_wait_df", pd.DataFrame()).empty:
                    tables["Wait_Pullback"] = self.last_wait_df
                if self.last_attack_df is not None and not self.last_attack_df.empty:
                    tables["Attack"] = self.last_attack_df
                if self.last_watch_df is not None and not self.last_watch_df.empty:
                    tables["Watch"] = self.last_watch_df
                if self.last_defense_df is not None and not self.last_defense_df.empty:
                    tables["Defense"] = self.last_defense_df
                if self.last_order_list_df is not None and not self.last_order_list_df.empty:
                    tables["Order_List"] = self.last_order_list_df
                tables["Detail"] = pd.DataFrame({"detail": [detail_text]})
                self.ui_call(self.update_task, "匯出分析", 3, 5, item="寫入檔案")
                base = RUNTIME_DIR / f"Analysis_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                out_path, out_type = write_table_bundle(base, tables, preferred="excel")
                display_name = Path(out_path).name if isinstance(out_path, Path) else str(out_path)
                self.ui_call(self.update_task, "匯出分析", 5, 5, success=1, item=display_name)
                self.ui_call(self.finish_task, "匯出分析", f"分析報告已輸出：{display_name}")
                self.ui_call(messagebox.showinfo, "完成", f"分析報告已輸出（{out_type}）：\n{out_path}")
            except Exception as e:
                self.ui_call(messagebox.showerror, "錯誤", str(e))

        self._run_in_thread(worker, "export_analysis")

    def build_order_list(self, today_buy_df: pd.DataFrame, wait_df: pd.DataFrame | None = None) -> pd.DataFrame:
        cols = ["優先級", "代號", "名稱", "分類", "狀態", "進場區", "停損", "1.382", "1.618", "RR", "勝率", "建議張數", "風險備註"]
        rows = []
        if today_buy_df is not None and not today_buy_df.empty:
            for i, (_, r) in enumerate(today_buy_df.iterrows(), start=1):
                rr = float(r.get("rr", 0) or 0)
                win_rate = float(r.get("win_rate", 0) or 0)
                if rr >= 2.5 and win_rate >= 85:
                    qty = 2.0
                    risk_note = "強買：主升段且結構完整"
                elif rr >= 1.8:
                    qty = 1.0
                    risk_note = "一般買：標準可交易標的"
                else:
                    qty = 0.5
                    risk_note = "測試單：僅供追蹤"
                rows.append({
                    "優先級": i,
                    "代號": r.get("stock_id", ""),
                    "名稱": r.get("stock_name", ""),
                    "分類": "今日可買",
                    "狀態": "可買",
                    "進場區": r.get("entry_zone", "-"),
                    "停損": r.get("stop_loss", "-"),
                    "1.382": f"{float(r.get('target_1382', 0) or 0):.2f}" if pd.notna(r.get("target_1382", None)) else "-",
                    "1.618": f"{float(r.get('target_1618', 0) or 0):.2f}" if pd.notna(r.get("target_1618", None)) else "-",
                    "RR": rr,
                    "勝率": win_rate,
                    "建議張數": qty,
                    "風險備註": risk_note,
                })
        if wait_df is not None and not wait_df.empty:
            base_priority = len(rows)
            for j, (_, r) in enumerate(wait_df.iterrows(), start=1):
                rows.append({
                    "優先級": base_priority + j,
                    "代號": r.get("stock_id", ""),
                    "名稱": r.get("stock_name", ""),
                    "分類": "預掛單",
                    "狀態": "預掛單",
                    "進場區": r.get("entry_zone", "-"),
                    "停損": r.get("stop_loss", "-"),
                    "1.382": f"{float(r.get('target_1382', 0) or 0):.2f}" if pd.notna(r.get("target_1382", None)) else "-",
                    "1.618": f"{float(r.get('target_1618', 0) or 0):.2f}" if pd.notna(r.get("target_1618", None)) else "-",
                    "RR": float(r.get("rr", 0) or 0),
                    "勝率": float(r.get("win_rate", 0) or 0),
                    "建議張數": 0,
                    "風險備註": "等待拉回確認，到價後轉可買，不直接追價",
                })
        return pd.DataFrame(rows, columns=cols)


    def refresh_top20_and_order_views(self):
        for tree in (self.top20_tree, self.top5_tree, self.order_tree):
            for item in tree.get_children():
                tree.delete(item)

        if self.last_top20_df is not None and not self.last_top20_df.empty:
            for i, (_, r) in enumerate(self.last_top20_df.iterrows(), start=1):
                ui_action = str(r.get("ui_state", "不可買"))
                self.top20_tree.insert("", "end", values=(
                    i, r.get("stock_id", ""), r.get("stock_name", ""), r.get("bucket", ""), ui_action,
                    r.get("entry_zone", "-"), r.get("stop_loss", "-"),
                    f"{float(r.get('target_1382', 0) or 0):.2f}", f"{float(r.get('target_1618', 0) or 0):.2f}",
                    f"{float(r.get('rr', 0) or 0):.2f}", f"{float(r.get('win_rate', 0) or 0):.1f}"
                ))

        if self.last_top5_df is not None and not self.last_top5_df.empty:
            for i, (_, r) in enumerate(self.last_top5_df.iterrows(), start=1):
                self.top5_tree.insert("", "end", values=(
                    i, r.get("stock_id", ""), r.get("stock_name", ""), r.get("ui_state", "-"),
                    r.get("entry_zone", "-"), r.get("stop_loss", "-"),
                    f"{float(r.get('target_1382', 0) or 0):.2f}",
                    f"{float(r.get('rr', 0) or 0):.2f}",
                    f"{float(r.get('win_rate', 0) or 0):.1f}",
                    f"{float(r.get('backtest_win_rate', 0) or 0):.1f}"
                ))


        if self.last_order_list_df is not None and not self.last_order_list_df.empty:
            for _, r in self.last_order_list_df.iterrows():
                self.order_tree.insert("", "end", values=(
                    int(r.get("優先級", 0) or 0), r.get("代號", ""), r.get("名稱", ""), r.get("分類", ""),
                    r.get("狀態", ""), r.get("進場區", "-"), r.get("停損", "-"),
                    r.get("1.382", "-"), r.get("1.618", "-"),
                    f"{float(r.get('RR', 0) or 0):.2f}", f"{float(r.get('勝率', 0) or 0):.1f}",
                    (f"{float(r.get('建議張數', 0) or 0):.1f}".rstrip('0').rstrip('.') if pd.notna(r.get('建議張數', 0)) else "0"), r.get("風險備註", "")
                ))

    def on_select_top20(self, event=None):
        sel = self.top20_tree.selection()
        if not sel:
            return
        vals = self.top20_tree.item(sel[0], "values")
        stock_id = str(vals[1])
        stock = self.db.get_stock_row(stock_id)
        hist = self.db.get_price_history(stock_id)
        if stock is None or hist.empty:
            return
        hist = DataEngine.attach(hist)
        last = hist.iloc[-1]
        trade_plan = self.master_trading_engine.plan_engine.build_plan(stock_id)
        self.current_chart_path = self.export_chart(stock_id, hist)
        lines = [
            f"《AI交易TOP20 / v6.5 PRO》",
            f"股票：{stock['stock_name']} ({stock_id})",
            f"市場 / 產業 / 題材：{stock['market']} / {stock['industry']} / {stock['theme']}",
            f"最新收盤：{float(last['close']):.2f}",
            f"交易分類：{trade_plan['bucket']}｜決策：{trade_plan['trade_action']}｜狀態：{trade_plan.get('ui_state', '-')}",
            f"支撐 / 壓力：{float(trade_plan.get('support', 0) or 0):.2f} / {float(trade_plan.get('resistance', 0) or 0):.2f}",
            f"進場區：{trade_plan['entry_zone']}",
            f"停損：{trade_plan['stop_loss']}",
            f"1.382 / 1.618：{float(trade_plan.get('target_1382', 0) or 0):.2f} / {float(trade_plan.get('target_1618', 0) or 0):.2f}",
            f"RR：{float(trade_plan['rr']):.2f}｜勝率：{trade_plan['win_grade']} ({float(trade_plan['win_rate']):.1f}%)",
            f"六模組總分：{float(trade_plan.get('model_score', 0) or 0):.2f}",
            f"六模組：K {trade_plan.get('kline_score',0):.1f}｜波 {trade_plan.get('wave_score',0):.1f}｜費 {trade_plan.get('fib_score',0):.1f}｜阪 {trade_plan.get('sakata_score',0):.1f}｜量 {trade_plan.get('volume_score',0):.1f}｜指 {trade_plan.get('indicator_score',0):.1f}",
            f"理由：{trade_plan['reason']}",
            f"圖表：{self.current_chart_path}",
        ]
        self.detail.delete("1.0", tk.END)
        self.detail.insert("1.0", "\n".join(lines))
    def on_select_order(self, event=None):
        sel = self.order_tree.selection()
        if not sel:
            return
        vals = self.order_tree.item(sel[0], "values")
        stock_id = str(vals[1])
        stock = self.db.get_stock_row(stock_id)
        hist = self.db.get_price_history(stock_id)
        if stock is None or hist.empty:
            return
        hist = DataEngine.attach(hist)
        last = hist.iloc[-1]
        self.current_chart_path = self.export_chart(stock_id, hist)
        lines = [
            "《下單清單 / v6.5 PRO 可下單系統》",
            f"優先級：{vals[0]}",
            f"股票：{stock['stock_name']} ({stock_id})",
            f"分類 / 狀態：{vals[3]} / {vals[4]}",
            f"進場區：{vals[5]}",
            f"停損：{vals[6]}",
            f"1.382 / 1.618：{vals[7]} / {vals[8]}",
            f"RR：{vals[9]}｜勝率：{vals[10]}%｜建議張數：{vals[11]}",
            f"風險備註：{vals[12]}",
            f"最新收盤：{float(last['close']):.2f}",
            f"圖表：{self.current_chart_path}",
        ]
        self.detail.delete("1.0", tk.END)
        self.detail.insert("1.0", "\n".join(lines))


    def show_top5(self):
        if self.last_top5_df is None or self.last_top5_df.empty:
            self.show_top20()
            if self.last_top5_df is None or self.last_top5_df.empty:
                return messagebox.showwarning("提醒", "目前尚無可用 TOP5 資料，請先執行 AI選股TOP20。")
        self.refresh_top20_and_order_views()
        self.left_notebook.select(self.tab_top5)
        lines = ["《v6.5 PRO AI選股TOP5》", ""]
        for i, (_, r) in enumerate(self.last_top5_df.iterrows(), start=1):
            lines.append(
                f"{i}. {r['stock_id']} {r['stock_name']}｜{r.get('ui_state','-')}｜進場 {r.get('entry_zone','-')}｜RR {float(r.get('rr',0) or 0):.2f}｜勝率 {float(r.get('win_rate',0) or 0):.1f}%｜回測 {float(r.get('backtest_win_rate',0) or 0):.1f}%"
            )
        self.detail.delete("1.0", tk.END)
        self.detail.insert("1.0", "\n".join(lines))

    def on_select_top5(self, event=None):
        sel = self.top5_tree.selection()
        if not sel:
            return
        vals = self.top5_tree.item(sel[0], "values")
        stock_id = str(vals[1])
        stock = self.db.get_stock_row(stock_id)
        hist = self.db.get_price_history(stock_id)
        if stock is None or hist.empty:
            return
        hist = DataEngine.attach(hist)
        last = hist.iloc[-1]
        trade_plan = self.master_trading_engine.plan_engine.build_plan(stock_id)
        bt = self.backtest_engine.estimate_trade_quality(stock_id)
        self.current_chart_path = self.export_chart(stock_id, hist)
        lines = [
            f"《AI選股TOP5 / v6.5 PRO》",
            f"股票：{stock['stock_name']} ({stock_id})",
            f"市場 / 產業 / 題材：{stock['market']} / {stock['industry']} / {stock['theme']}",
            f"最新收盤：{float(last['close']):.2f}",
            f"狀態：{trade_plan.get('ui_state','-')}｜決策：{trade_plan.get('trade_action','-')}",
            f"進場區：{trade_plan.get('entry_zone','-')}",
            f"停損：{trade_plan.get('stop_loss','-')}",
            f"1.382 / 1.618：{float(trade_plan.get('target_1382',0) or 0):.2f} / {float(trade_plan.get('target_1618',0) or 0):.2f}",
            f"RR：{float(trade_plan.get('rr',0) or 0):.2f}｜模型勝率：{float(trade_plan.get('win_rate',0) or 0):.1f}%",
            f"回測勝率：{float(bt.get('backtest_win_rate',0) or 0):.1f}%｜平均報酬：{float(bt.get('avg_return',0) or 0):.2f}%｜樣本數：{int(bt.get('samples',0) or 0)}",
            f"六模組總分：{float(trade_plan.get('model_score',0) or 0):.2f}",
            f"圖表：{self.current_chart_path}",
        ]
        self.detail.delete("1.0", tk.END)
        self.detail.insert("1.0", "\n".join(lines))

    def build_full_history_once(self):
        self._start_build_history(resume=False)

    def resume_full_history(self):
        self._start_build_history(resume=True)

    def _start_build_history(self, resume: bool = False):
        master = self.db.get_master()
        if master.empty:
            return messagebox.showwarning("提醒", "請先初始化全市場。")

        counts = master["stock_id"].astype(str).apply(self.db.get_price_history_count)
        ready = int((counts >= 240).sum())
        total = len(master)
        state = self.load_history_state()

        if resume:
            if not state:
                self.append_log("未找到上次中斷狀態，改為一般補建模式。")
            ok = messagebox.askyesno("確認", f"將執行續跑建庫。\n目前完整檔數：{ready}/{total}\n系統會自動跳過已完成股票，是否開始？")
        elif ready >= int(total * 0.9):
            ok = messagebox.askyesno("確認", f"已有 {ready}/{total} 檔具備完整歷史資料。\n再次執行將只補缺漏資料，是否繼續？")
        else:
            ok = messagebox.askyesno("確認", f"將建立完整歷史資料。\n目前完整檔數：{ready}/{total}\n是否開始？")
        if not ok:
            return

        def worker():
            try:
                self.ui_call(self.clear_log)
                self.ui_call(self.append_log, f"開始完整建庫，模式={'續跑' if resume else '一般'}，主檔 {total} 檔")
                self.ui_call(self.set_status, "開始建立完整歷史資料（分批 / 可中斷 / 可續跑）...")
                self.ui_call(self.start_task, "建立完整歷史", total)
                self.ui_call(self.update_task, "建立完整歷史", 0, total, 0, 0, 0, "準備中")
                counters = {"ok": 0, "fail": 0}

                def progress(idx, total_count, sid, existing_count, flag):
                    if flag in ("fail", "error"):
                        counters["fail"] += 1
                    elif flag == "ok":
                        counters["ok"] += 1
                    self.ui_call(self.update_task, "建立完整歷史", idx, total_count, counters["ok"], counters["fail"], 0, sid)
                    self.save_history_state({
                        "mode": "build_history",
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "current_index": idx,
                        "total": total_count,
                        "stock_id": sid,
                        "completed_ready": int((master["stock_id"].astype(str).apply(self.db.get_price_history_count) >= 240).sum()),
                        "success": counters["ok"],
                        "failed": counters["fail"],
                        "existing_count": int(existing_count),
                    })
                    if idx % 10 == 0 or idx == total_count:
                        self.ui_call(self.set_status, f"建立歷史中 {idx}/{total_count}｜{sid}｜成功 {counters['ok']}｜失敗 {counters['fail']}")

                success, failed, rows = self.data_engine.build_full_history(
                    batch_size=self.history_batch_size,
                    sleep_sec=self.history_sleep_sec,
                    progress_cb=progress,
                    log_cb=lambda msg: self.ui_call(self.append_log, msg),
                    cancel_cb=lambda: self.cancel_event.is_set(),
                )
                self.clear_history_state()
                self.ui_call(self.update_task, "建立完整歷史", total, total, success, failed, 0, "完成")
                self.ui_call(self.set_status, f"完整歷史建立完成：成功 {success} 檔，失敗 {failed} 檔，寫入 {rows} 筆。")
                self.ui_call(self.append_log, f"完整建庫完成：成功 {success} 檔｜失敗 {failed} 檔｜寫入 {rows} 筆")
                self.ui_call(self.show_welcome_message)
                self.ui_call(messagebox.showinfo, "完成", f"完整歷史建立完成\n成功 {success} 檔\n失敗 {failed} 檔\n寫入 {rows} 筆\n\n已支援分批抓取 / 中斷續跑")
            except OperationCancelled:
                state2 = self.load_history_state()
                sid = state2.get("stock_id", "")
                idx = state2.get("current_index", 0)
                total_count = state2.get("total", total)
                self.ui_call(self.append_log, f"作業已中斷：停在 {idx}/{total_count}｜{sid}")
                self.ui_call(self.set_status, f"建庫已中斷：停在 {idx}/{total_count}｜{sid}，可按『續跑建庫』")
                self.ui_call(messagebox.showwarning, "已中斷", f"完整建庫已中斷\n目前停在 {idx}/{total_count}｜{sid}\n\n下次請按『續跑建庫』，系統會自動跳過已完成資料。")
            except Exception as e:
                traceback.print_exc()
                self.ui_call(self.append_log, f"完整建庫發生錯誤：{e}")
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
                self.ui_call(self.start_task, "初始化全市場", 4)
                self.ui_call(self.update_task, "初始化全市場", 1, 4, item="抓取主檔")
                universe = build_full_market_universe()
                if universe is None or universe.empty:
                    csv_path = resolve_master_csv()
                    self.db.import_master_csv(csv_path)
                    master2 = self.db.get_master()
                    self.ui_call(self.refresh_filters)
                    self.ui_call(self.refresh_all_tables)
                    self.ui_call(self.update_task, "初始化全市場", 4, 4, success=1, item="完成")
                    self.ui_call(self.set_status, f"已改用本地主檔，共 {len(master2)} 檔。")
                    self.ui_call(messagebox.showinfo, "完成", f"全市場抓取失敗，已改用本地主檔\n共 {len(master2)} 檔\n\n使用主檔：{csv_path}")
                    return
                self.db.import_master_df(universe)
                master2 = self.db.get_master()
                self.ui_call(self.refresh_filters)
                self.ui_call(self.refresh_all_tables)
                self.ui_call(self.show_welcome_message)
                self.ui_call(self.update_task, "初始化全市場", 4, 4, success=1, item="完成")
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

        if not self.ensure_ranking_ready(auto_rebuild=True):
            price_rows = self.db.get_total_price_rows()
            if price_rows > 0:
                self.set_status("已有歷史資料，但尚未形成有效排行；請先補足歷史或重建排行。")
            else:
                self.set_status("目前尚無排行資料，請先初始化、建立歷史，再重建排行。")
            self.show_welcome_message()
            return

        df = self._filtered_ranking()
        if df.empty:
            self.set_status("目前篩選條件下沒有資料。")
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
        if (self.last_top20_df is not None and not self.last_top20_df.empty) or (self.last_order_list_df is not None and not self.last_order_list_df.empty):
            self.refresh_top20_and_order_views()

    def update_data(self):
        last_date = self.db.get_last_price_date()
        today = datetime.now().strftime("%Y-%m-%d")
        if last_date == today:
            ok = messagebox.askyesno("確認", f"今日資料（{today}）可能已更新過。\n再次執行會覆蓋今日官方資料，是否繼續？")
            if not ok:
                return

        def worker():
            try:
                master = self.db.get_master()
                total = len(master) if not master.empty else 1
                counters = {"ok": 0, "fail": 0, "skip": 0}
                self.ui_call(self.clear_log)
                self.ui_call(self.start_task, "每日增量更新", total)

                def progress(idx, total_count, sid, row_count, flag):
                    if flag == "ok":
                        counters["ok"] += 1
                    elif flag in ("fail", "error"):
                        counters["fail"] += 1
                    else:
                        counters["skip"] += 1
                    self.ui_call(self.update_task, "每日增量更新", idx, total_count, counters["ok"], counters["fail"], counters["skip"], sid)

                success, failed, rows = self.data_engine.update_incremental(progress_cb=progress, log_cb=lambda msg: self.ui_call(self.append_log, msg), cancel_cb=lambda: self.cancel_event.is_set())
                self.ui_call(self.start_task, "重建排行", total)
                rank_skip = {"skip": 0}
                def rank_progress(idx, total_count, sid, ok_count, fail_count, skip_count, flag):
                    rank_skip["skip"] = skip_count
                    self.ui_call(self.update_task, "重建排行", idx, total_count, ok_count, fail_count, skip_count, sid)
                rank_count = self.rank_engine.rebuild(progress_cb=rank_progress, log_cb=lambda msg: self.ui_call(self.append_log, msg), cancel_cb=lambda: self.cancel_event.is_set())
                self.ui_call(self.refresh_filters)
                self.ui_call(self.refresh_all_tables)
                self.ui_call(self.show_welcome_message)
                self.ui_call(self.finish_task, "每日增量更新", f"完成：成功 {success} 檔，寫入 {rows} 筆，排行 {rank_count} 檔。")
                self.ui_call(messagebox.showinfo, "完成", f"每日增量更新完成\n成功 {success} 檔\n寫入 {rows} 筆\n排行 {rank_count} 檔\n（TWSE/TPEX 官方優先，只更新今日）")
            except OperationCancelled:
                self.ui_call(self.append_log, "每日更新/重排行已中斷")
                self.ui_call(self.finish_task, "每日增量更新", "作業已中斷")
            except Exception as e:
                traceback.print_exc()
                self.ui_call(messagebox.showerror, "錯誤", str(e))

        self._run_in_thread(worker, "update_daily")

    def rebuild_ranking(self):
        def worker():
            try:
                master = self.db.get_master()
                total = len(master) if not master.empty else 1
                self.ui_call(self.clear_log)
                self.ui_call(self.start_task, "重建排行", total)
                def progress(idx, total_count, sid, ok_count, fail_count, skip_count, flag):
                    self.ui_call(self.update_task, "重建排行", idx, total_count, ok_count, fail_count, skip_count, sid)
                count = self.rank_engine.rebuild(progress_cb=progress, log_cb=lambda msg: self.ui_call(self.append_log, msg), cancel_cb=lambda: self.cancel_event.is_set())
                self.ui_call(self.refresh_filters)
                self.ui_call(self.refresh_all_tables)
                self.ui_call(self.show_welcome_message)
                self.ui_call(self.finish_task, "重建排行", f"排行已完成，共 {count} 檔")
                if count <= 0:
                    self.ui_call(messagebox.showwarning, "提醒", "排行重建完成，但目前可計算檔數為 0。\n請先建立至少 70 根以上歷史K線資料。")
                else:
                    self.ui_call(messagebox.showinfo, "完成", f"排行已完成，共 {count} 檔")
            except OperationCancelled:
                self.ui_call(self.finish_task, "重建排行", "重建排行已中斷")
            except Exception as e:
                traceback.print_exc()
                self.ui_call(messagebox.showerror, "錯誤", str(e))

        self._run_in_thread(worker, "rebuild_rank")

    def show_top20(self):
        if not self.ensure_ranking_ready(auto_rebuild=True):
            return messagebox.showwarning("提醒", "目前尚無可用排行資料，請先建立歷史資料後重建排行。")
        df = self._filtered_ranking()
        if df.empty:
            return messagebox.showwarning("提醒", "目前篩選條件下沒有可用資料")

        def worker():
            try:
                total = len(df)
                self.ui_call(self.clear_log)
                self.ui_call(self.start_task, "AI選股TOP20", total)

                def progress(idx, total_count, sid):
                    self.ui_call(self.update_task, "AI選股TOP20", idx, total_count, idx, 0, 0, sid)

                trade = self.master_trading_engine.get_trade_pool(
                    df,
                    progress_cb=progress,
                    log_cb=lambda msg: self.ui_call(self.append_log, msg),
                    cancel_cb=lambda: self.cancel_event.is_set(),
                )
                market = trade["market"]
                trade_top20 = trade["trade_top20"]
                attack = trade["attack"]
                watch = trade["watch"]
                defense = trade["defense"]
                today_buy = trade["today_buy"]
                wait_pullback = trade["wait_pullback"]
                theme_summary = trade["theme_summary"]

                self.last_top20_df = trade_top20.copy()
                top5 = trade_top20.head(5).copy()
                if not top5.empty:
                    bt_rows = []
                    for _, rr in top5.iterrows():
                        bt = self.backtest_engine.estimate_trade_quality(str(rr["stock_id"]))
                        bt_rows.append(bt)
                    bt_df = pd.DataFrame(bt_rows)
                    top5 = pd.concat([top5.reset_index(drop=True), bt_df.reset_index(drop=True)], axis=1)
                self.last_top5_df = top5.copy()
                self.last_attack_df = attack.copy()
                self.last_watch_df = watch.copy()
                self.last_defense_df = defense.copy()
                self.last_theme_summary_df = theme_summary.copy()
                self.last_today_buy_df = today_buy.copy()
                self.last_wait_df = wait_pullback.copy()
                self.last_order_list_df = self.build_order_list(today_buy, wait_pullback)

                self.ui_call(self.refresh_top20_and_order_views)
                self.ui_call(self.left_notebook.select, self.tab_top20)

                defend_cnt = int(trade_top20["bucket"].eq("防守").sum()) if not trade_top20.empty else 0
                lines = [
                    "《v6.5 PRO 可下單系統》",
                    f"市場判斷：{market['regime']}（{market['score']:.2f}）｜市場廣度 {market['breadth']:.1f}",
                    f"市場說明：{market['memo']}",
                    f"TOP20 觀察池：{len(trade_top20)} 檔｜今日可買：{len(today_buy)}｜預掛單：{len(wait_pullback)}｜防守：{defend_cnt}",
                    f"交易門檻：決策 BUY / WEAK BUY｜支撐 > 0｜壓力 > 支撐｜再依六模組總分排序",
                    "",
                    "【TOP20 觀察池 前5檔】",
                ]
                if trade_top20.empty:
                    lines.append("目前無符合條件標的")
                else:
                    for i, (_, r) in enumerate(trade_top20.head(5).iterrows(), start=1):
                        lines.append(f"{i}. {r['stock_id']} {r['stock_name']}｜{r['bucket']}｜{r['trade_action']}｜RR {r['rr']:.2f}｜勝率 {r['win_rate']:.1f}%")

                lines.extend(["", "【今日可買】"])
                if today_buy.empty:
                    lines.append("今日無符合 SOP 的可買名單（允許空白，不為湊數放寬）。")
                else:
                    for i, (_, r) in enumerate(today_buy.iterrows(), start=1):
                        lines.append(f"{i}. {r['stock_id']} {r['stock_name']}｜{r['trade_action']}｜RR {r['rr']:.2f}｜勝率 {r['win_rate']:.1f}%｜{r['entry_zone']}")

                lines.extend(["", "【預掛單】"])
                if wait_pullback.empty:
                    lines.append("無預掛單")
                else:
                    for i, (_, r) in enumerate(wait_pullback.iterrows(), start=1):
                        lines.append(f"{i}. {r['stock_id']} {r['stock_name']}｜預掛單｜RR {r['rr']:.2f}｜勝率 {r['win_rate']:.1f}%｜{r['entry_zone']}")

                self.ui_call(self.detail.delete, "1.0", tk.END)
                self.ui_call(self.detail.insert, "1.0", "\n".join(lines))
                self.ui_call(self.finish_task, "AI選股TOP20", f"AI選股完成：TOP20 {len(trade_top20)}｜今日可買 {len(today_buy)}｜等待 {len(wait_pullback)}")
            except OperationCancelled:
                self.ui_call(self.finish_task, "AI選股TOP20", "AI選股TOP20 已中斷")
            except Exception as e:
                traceback.print_exc()
                self.ui_call(messagebox.showerror, "錯誤", str(e))

        self._run_in_thread(worker, "show_top20")

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
        hist = DataEngine.attach(hist)
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
            f"六模組：K {trade_plan.get('kline_score',0):.1f}｜波 {trade_plan.get('wave_score',0):.1f}｜費 {trade_plan.get('fib_score',0):.1f}｜阪 {trade_plan.get('sakata_score',0):.1f}｜量 {trade_plan.get('volume_score',0):.1f}｜指 {trade_plan.get('indicator_score',0):.1f}",
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

        if db.get_ranking_rows_count() == 0 and db.get_total_price_rows() > 0:
            rank_count = RankingEngine(db).rebuild()
            if rank_count > 0:
                init_message += f"｜已自動重建排行 {rank_count} 檔"
            else:
                init_message += "｜已有歷史資料，但目前不足以形成排行"
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
