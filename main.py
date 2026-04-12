
# -*- coding: utf-8 -*-
"""
GTC AI Trading System v9.2 FINAL-RELEASE

功能：
- 股票主檔分類（市場 / 產業 / 題材 / 子題材）
- 本地 SQLite 歷史資料庫
- TWSE/TPEX 官方資料 + Yahoo Finance 備援更新
- 核心 StrategyEngineV91（訊號 → 評分 → 倉位 → 交易計畫）
- 波浪 + 費波交易模型化
- Kelly + ATR 資金管理
- 真回測系統（勝率 / 平均報酬 / CAGR / MDD / Sharpe）
- 回測視覺化（Equity Curve）
- 分類 + 產業輪動分析
- TOP20 / TOP5 / 下單清單 / 機構交易計畫
- 專業交易 UI（儀表板 / 輪動 / 排行 / TOP / 計畫 / 回測）
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
import warnings
import logging
import json
import hashlib
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
from matplotlib import font_manager
from matplotlib.patches import Rectangle
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg


warnings.filterwarnings("ignore", message=r"Glyph .* missing from font")
warnings.filterwarnings("ignore", message=r"Matplotlib is currently using agg")

PREFERRED_CJK_FONTS = [
    "Microsoft JhengHei", "Microsoft YaHei", "PingFang TC", "PingFang SC",
    "Noto Sans CJK TC", "Noto Sans CJK SC", "Noto Sans CJK JP",
    "Source Han Sans TW", "Source Han Sans CN", "SimHei", "Arial Unicode MS",
]

def load_font_from_runtime(font_path: Path) -> Optional[str]:
    try:
        if font_path and Path(font_path).exists():
            font_manager.fontManager.addfont(str(font_path))
            return font_manager.FontProperties(fname=str(font_path)).get_name()
    except Exception:
        return None
    return None

def resolve_cjk_font_path() -> Optional[Path]:
    search_dirs = [
        (RUNTIME_DIR / "fonts") if "RUNTIME_DIR" in globals() else None,
        (BASE_DIR / "fonts") if "BASE_DIR" in globals() else None,
        Path(__file__).resolve().parent / "fonts",
    ]
    candidates = [
        "NotoSansCJK-Regular.ttc",
        "NotoSansCJKtc-Regular.otf",
        "NotoSansTC-Regular.ttf",
        "msjh.ttc",
        "msyh.ttc",
        "simhei.ttf",
    ]
    for folder in search_dirs:
        if folder is None:
            continue
        for name in candidates:
            p = folder / name
            if p.exists():
                return p
    return None

def configure_matplotlib_cjk_font() -> str:
    chosen = None
    font_path = resolve_cjk_font_path()
    if font_path is not None:
        chosen = load_font_from_runtime(font_path)
    if chosen is None:
        available = {f.name for f in font_manager.fontManager.ttflist}
        for name in PREFERRED_CJK_FONTS:
            if name in available:
                chosen = name
                break
    if chosen is None:
        chosen = "DejaVu Sans"
    plt.rcParams["font.sans-serif"] = [chosen, "DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False
    return chosen

def safe_plot_text(value, fallback: str = "-") -> str:
    if value is None:
        return fallback
    s = str(value).replace("\n", " ").replace("\r", " ").strip()
    if not s:
        return fallback
    replacements = {
        "｜": " | ", "【": "[", "】": "]", "（": "(", "）": ")",
        "：": ": ", "，": ", ", "／": "/", "～": "~",
    }
    for old, new in replacements.items():
        s = s.replace(old, new)
    s = re.sub(r"\s+", " ", s).strip()
    return s or fallback

SELECTED_PLOT_FONT = None


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
APP_NAME = "GTC AI Trading System v9.2 FINAL-RELEASE V3.5 OPERATION"
STATE_PATH = RUNTIME_DIR / "build_history_state_v9_2_final_release.json"

LOG_DIR = RUNTIME_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_DIR / f"gtc_ai_trading_{datetime.now().strftime('%Y%m%d')}.log"

def configure_app_logger() -> logging.Logger:
    logger = logging.getLogger("gtc_ai_trading")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(threadName)s | %(message)s")
    file_handler = logging.FileHandler(LOG_PATH, encoding="utf-8")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger

APP_LOGGER = configure_app_logger()

def log_info(message: str):
    try:
        APP_LOGGER.info(str(message))
    except Exception:
        pass

def log_warning(message: str):
    try:
        APP_LOGGER.warning(str(message))
    except Exception:
        pass

def log_error(message: str):
    try:
        APP_LOGGER.error(str(message))
    except Exception:
        pass

def log_exception(message: str, exc: Exception | None = None):
    try:
        if exc is None:
            APP_LOGGER.exception(str(message))
        else:
            APP_LOGGER.exception(f"{message} | {exc}")
    except Exception:
        pass

SELECTED_PLOT_FONT = configure_matplotlib_cjk_font()


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


CLASSIFICATION_DOWNLOAD_URL = "https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv"
CLASSIFICATION_DOWNLOAD_URL_TWSE = CLASSIFICATION_DOWNLOAD_URL
CLASSIFICATION_DOWNLOAD_URL_TPEX = "https://mopsfin.twse.com.tw/opendata/t187ap03_O.csv"
CLASSIFICATION_DOWNLOAD_SOURCES = {
    "上市": {
        "url": CLASSIFICATION_DOWNLOAD_URL_TWSE,
        "cache_path": CLASSIFICATION_CACHE_CSV_TWSE if "CLASSIFICATION_CACHE_CSV_TWSE" in globals() else None,
        "source": "MOPS-CSV-TWSE",
    },
    "上櫃": {
        "url": CLASSIFICATION_DOWNLOAD_URL_TPEX,
        "cache_path": CLASSIFICATION_CACHE_CSV_TPEX if "CLASSIFICATION_CACHE_CSV_TPEX" in globals() else None,
        "source": "MOPS-CSV-TPEX",
    },
}
CLASSIFICATION_LEGACY_DOWNLOAD_URL = "https://www.twse.com.tw/docs1/data01/market/public_html/960803-0960203558-2.xls"
CLASSIFICATION_CACHE_DIR = EXTERNAL_DATA_DIR / "classification"
CLASSIFICATION_CACHE_DIR.mkdir(parents=True, exist_ok=True)
CLASSIFICATION_CACHE_CSV_TWSE = CLASSIFICATION_CACHE_DIR / "台股官方產業分類_上市.csv"
CLASSIFICATION_CACHE_CSV_TPEX = CLASSIFICATION_CACHE_DIR / "台股官方產業分類_上櫃.csv"
CLASSIFICATION_CACHE_CSV = CLASSIFICATION_CACHE_CSV_TWSE  # backward compatibility
CLASSIFICATION_DOWNLOAD_SOURCES["上市"]["cache_path"] = CLASSIFICATION_CACHE_CSV_TWSE
CLASSIFICATION_DOWNLOAD_SOURCES["上櫃"]["cache_path"] = CLASSIFICATION_CACHE_CSV_TPEX
CLASSIFICATION_CACHE_XLS = CLASSIFICATION_CACHE_DIR / "台股類股分類.xls"
CLASSIFICATION_CACHE_XLSX = CLASSIFICATION_CACHE_DIR / "台股類股分類.xlsx"
CLASSIFICATION_META_PATH = CLASSIFICATION_CACHE_DIR / "classification_meta.json"
CLASSIFICATION_CACHE_PICKLE = CLASSIFICATION_CACHE_DIR / "classification_cache.pkl"
CLASSIFICATION_MAX_AGE_DAYS = 7
CLASSIFICATION_DOWNLOAD_TIMEOUT = (10, 45)
CLASSIFICATION_DOWNLOAD_RETRIES = 3
CLASSIFICATION_MEMORY_CACHE = {"df": None, "path": None, "mtime": None, "meta": None, "market": "ALL"}
LAST_CLASSIFICATION_LOAD_INFO = {"loaded": False, "rows": 0, "path": "", "note": "尚未載入"}
CLASSIFICATION_V2_SUMMARY_PATH = CLASSIFICATION_CACHE_DIR / "classification_v2_summary.json"
CLASSIFICATION_V2_UNCLASSIFIED_PATH = CLASSIFICATION_CACHE_DIR / "未匹配分類清單.xlsx"
CLASSIFICATION_V2_LAST_SUMMARY = {}

CLASSIFICATION_LOG_CALLBACK = None

def set_classification_log_callback(cb):
    global CLASSIFICATION_LOG_CALLBACK
    CLASSIFICATION_LOG_CALLBACK = cb

def classification_debug_log(message: str, level: str = "INFO"):
    msg = f"[分類載入] {message}"
    try:
        level_upper = str(level or "INFO").upper()
        if level_upper == "ERROR":
            log_error(msg)
        elif level_upper == "WARNING":
            log_warning(msg)
        else:
            log_info(msg)
    except Exception:
        pass
    try:
        cb = CLASSIFICATION_LOG_CALLBACK
        if cb is not None:
            cb(msg, level)
    except Exception:
        pass


CLASSIFICATION_BOOK_CANDIDATES = [
    RUNTIME_DIR / "台股官方產業分類_上市.csv",
    EXTERNAL_DATA_DIR / "台股官方產業分類_上市.csv",
    CLASSIFICATION_CACHE_CSV_TWSE,
    BASE_DIR / "台股官方產業分類_上市.csv",
    RUNTIME_DIR / "台股官方產業分類_上櫃.csv",
    EXTERNAL_DATA_DIR / "台股官方產業分類_上櫃.csv",
    CLASSIFICATION_CACHE_CSV_TPEX,
    BASE_DIR / "台股官方產業分類_上櫃.csv",
    RUNTIME_DIR / "台股類股分類.csv",
    EXTERNAL_DATA_DIR / "台股類股分類.csv",
    BASE_DIR / "台股類股分類.csv",
    RUNTIME_DIR / "台股類股分類.xlsx",
    RUNTIME_DIR / "台股類股分類.xls",
    RUNTIME_DIR / "股票類別對照表.xlsx",
    RUNTIME_DIR / "股票類別對照表.xls",
    EXTERNAL_DATA_DIR / "台股類股分類.xlsx",
    EXTERNAL_DATA_DIR / "台股類股分類.xls",
    EXTERNAL_DATA_DIR / "股票類別對照表.xlsx",
    EXTERNAL_DATA_DIR / "股票類別對照表.xls",
    CLASSIFICATION_CACHE_XLSX,
    CLASSIFICATION_CACHE_XLS,
    BASE_DIR / "台股類股分類.xlsx",
    BASE_DIR / "台股類股分類.xls",
    BASE_DIR / "股票類別對照表.xlsx",
    BASE_DIR / "股票類別對照表.xls",
]


def _safe_file_sha256(path: Path) -> str:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return ""

def _read_classification_meta() -> dict:
    try:
        if CLASSIFICATION_META_PATH.exists():
            return json.loads(CLASSIFICATION_META_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def _write_classification_meta(meta: dict):
    try:
        CLASSIFICATION_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        CLASSIFICATION_META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

def _build_classification_meta(path: Path, source: str = "TWSE", note: str = "") -> dict:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    stat = path.stat() if path.exists() else None
    return {
        "last_update": now,
        "source": source,
        "file": path.name if path else "",
        "path": str(path) if path else "",
        "hash": _safe_file_sha256(path) if path and path.exists() else "",
        "size": int(stat.st_size) if stat else 0,
        "mtime": float(stat.st_mtime) if stat else 0.0,
        "status": "ok" if path and path.exists() else "missing",
        "note": note or "",
    }

def _mark_classification_meta_status(status: str, note: str = "", path: Path | None = None):
    meta = _read_classification_meta()
    if path and Path(path).exists():
        meta.update(_build_classification_meta(Path(path), source=meta.get("source", "TWSE"), note=note))
    meta["status"] = status
    if note:
        meta["note"] = note
    if "last_update" not in meta:
        meta["last_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _write_classification_meta(meta)

def _classification_meta_is_stale(meta: dict, max_age_days: int = CLASSIFICATION_MAX_AGE_DAYS) -> bool:
    try:
        ts = float(meta.get("mtime", 0) or 0)
        if ts <= 0:
            return True
        age_days = (time.time() - ts) / 86400.0
        return age_days > max_age_days
    except Exception:
        return True

def _set_classification_load_info(loaded: bool, rows: int = 0, path: Path | None = None, note: str = ""):
    global LAST_CLASSIFICATION_LOAD_INFO
    LAST_CLASSIFICATION_LOAD_INFO = {
        "loaded": bool(loaded),
        "rows": int(rows or 0),
        "path": str(path) if path else "",
        "note": str(note or "")
    }

def get_classification_load_info() -> dict:
    try:
        return dict(LAST_CLASSIFICATION_LOAD_INFO)
    except Exception:
        return {"loaded": False, "rows": 0, "path": "", "note": "未知"}

def get_classification_status() -> dict:
    path = resolve_classification_book()
    meta = _read_classification_meta()
    out = dict(meta) if isinstance(meta, dict) else {}
    load_info = get_classification_load_info()
    out["loaded"] = bool(load_info.get("loaded", False))
    out["loaded_rows"] = int(load_info.get("rows", 0) or 0)
    out["load_note"] = str(load_info.get("note", "") or "")
    out["load_path"] = str(load_info.get("path", "") or "")
    if path and Path(path).exists():
        p = Path(path)
        out["file"] = p.name
        out["path"] = str(p)
        out["hash"] = _safe_file_sha256(p)
        out["size"] = int(p.stat().st_size)
        out["mtime"] = float(p.stat().st_mtime)
        out["exists"] = True
        out["is_stale"] = _classification_meta_is_stale(out)
    else:
        out["exists"] = False
        out["is_stale"] = True
        out.setdefault("status", "missing")
    if out.get("loaded") and out.get("loaded_rows", 0) > 0:
        out["status"] = "ok"
    elif out.get("exists"):
        out["status"] = out.get("status", "fallback")
    else:
        out["status"] = out.get("status", "missing")
    return out

def _normalize_stock_name_for_match(v) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    replacements = {
        "（": "(", "）": ")", "　": "", " ": "", "-": "", "_": "",
        "股份有限公司": "", "有限公司": "", "公司": "", "控股": "", "控": "",
        "DR": "", "dr": "", "ETF": "ETF",
    }
    for old, new in replacements.items():
        s = s.replace(old, new)
    s = re.sub(r"[\.\,\/\\]+", "", s)
    return s.upper().strip()



def normalize_stock_id(v) -> str:
    s = str(v).strip()
    if s in ("", "nan", "None"):
        return ""
    m = re.search(r"(\d{4,5})", s)
    if not m:
        return ""
    code = m.group(1)
    return code.zfill(4) if len(code) == 4 else code



def safe_read_csv_auto(path: Path) -> pd.DataFrame:
    path = Path(path)
    last_error = None
    for enc in ("utf-8-sig", "utf-8", "big5", "cp950"):
        try:
            return pd.read_csv(path, encoding=enc, dtype=str).fillna("")
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"CSV讀取失敗：{path}｜{last_error}")


def _extract_official_csv_rows(df: pd.DataFrame, market: str = "上市") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["stock_id", "stock_name_official", "stock_name_norm_official", "market_official", "industry_official"])

    x = df.copy().fillna("")
    x.columns = _coerce_unique_columns(x.columns)

    code_col = None
    name_col = None
    industry_name_col = None
    industry_code_col = None

    for c in x.columns:
        base = str(c).split("__dup")[0].strip()
        if base in ("公司代號", "股票代號", "證券代號", "公司代碼"):
            code_col = c if code_col is None else code_col
        elif base in ("公司名稱", "公司簡稱", "證券名稱", "股票名稱"):
            name_col = c if name_col is None else name_col
        elif base in ("新產業類別", "新產業別", "產業名稱", "industry_name"):
            industry_name_col = c if industry_name_col is None else industry_name_col
        elif base in ("產業別", "產業類別", "產業代碼", "產業類別代號", "industry_code"):
            industry_code_col = c if industry_code_col is None else industry_code_col

    industry_col = industry_name_col or industry_code_col
    if code_col is None or industry_col is None:
        return pd.DataFrame(columns=["stock_id", "stock_name_official", "stock_name_norm_official", "market_official", "industry_official"])

    out = pd.DataFrame({
        "stock_id": x[code_col].map(normalize_stock_id),
        "stock_name_official": x[name_col].astype(str).str.strip() if name_col in x.columns else "",
        "industry_official": x[industry_col].astype(str).str.strip(),
    })
    out["industry_official"] = out["industry_official"].map(normalize_official_industry_name)
    out["stock_name_norm_official"] = out["stock_name_official"].map(_normalize_stock_name_for_match)
    out["market_official"] = market
    out = out[(out["stock_id"] != "") & (out["industry_official"] != "")].copy()
    if out.empty:
        return out
    return out[["stock_id", "stock_name_official", "stock_name_norm_official", "market_official", "industry_official"]].drop_duplicates(subset=["stock_id"], keep="first")

def _try_convert_xls_to_xlsx(src: Path, dst: Path) -> Optional[Path]:
    try:
        if not src.exists():
            return None
        import win32com.client  # type: ignore
        excel = win32com.client.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        wb = excel.Workbooks.Open(str(src.resolve()))
        try:
            wb.SaveAs(str(dst.resolve()), FileFormat=51)
        finally:
            wb.Close(False)
            excel.Quit()
        if dst.exists() and dst.stat().st_size > 0:
            return dst
    except Exception:
        pass
    return None

def _try_convert_xls_to_xlsx_soffice(src: Path, dst: Path) -> Optional[Path]:
    try:
        src = Path(src)
        dst = Path(dst)
        if not src.exists():
            return None
        out_dir = dst.parent
        out_dir.mkdir(parents=True, exist_ok=True)
        for candidate in ("soffice", "libreoffice"):
            try:
                proc = subprocess.run(
                    [candidate, "--headless", "--convert-to", "xlsx", "--outdir", str(out_dir), str(src)],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=60,
                )
                if proc.returncode == 0:
                    break
            except Exception:
                continue
        if dst.exists() and dst.stat().st_size > 0:
            return dst
        alt = out_dir / f"{src.stem}.xlsx"
        if alt.exists() and alt.stat().st_size > 0:
            if alt != dst:
                try:
                    if dst.exists():
                        dst.unlink()
                except Exception:
                    pass
                alt.replace(dst)
            return dst
    except Exception:
        pass
    return None


def safe_read_excel(path: Path):
    path = Path(path)
    last_error = None

    if path.suffix.lower() == ".xlsx":
        try:
            return pd.read_excel(path, engine="openpyxl")
        except Exception as exc:
            last_error = exc

    if path.suffix.lower() == ".xls":
        converted = convert_xls_to_xlsx_force(path, CLASSIFICATION_CACHE_XLSX)
        if converted is not None and converted.exists():
            try:
                return pd.read_excel(converted, engine="openpyxl")
            except Exception as exc:
                last_error = exc
        raise RuntimeError(f"Excel讀取失敗：{path}｜無法將 xls 轉成 xlsx，且目前環境不可直接讀取 xls。原始錯誤：{last_error}")

    try:
        return pd.read_excel(path, engine="openpyxl")
    except Exception as exc:
        last_error = exc
    raise RuntimeError(f"Excel讀取失敗：{path}｜{last_error}")


def convert_xls_to_xlsx_force(src: Path, dst: Path, log_cb=None) -> Optional[Path]:
    src = Path(src)
    dst = Path(dst)

    converted = _try_convert_xls_to_xlsx(src, dst)
    if converted is not None and converted.exists() and converted.stat().st_size > 0:
        if log_cb:
            log_cb(f"分類檔已轉成 xlsx（Excel COM）：{converted}")
        return converted

    converted = _try_convert_xls_to_xlsx_soffice(src, dst)
    if converted is not None and converted.exists() and converted.stat().st_size > 0:
        if log_cb:
            log_cb(f"分類檔已轉成 xlsx（LibreOffice）：{converted}")
        return converted

    try:
        import importlib.util
        if importlib.util.find_spec("xlrd") is not None:
            xl = pd.ExcelFile(src, engine="xlrd")
            with pd.ExcelWriter(dst, engine="openpyxl") as writer:
                for sheet in xl.sheet_names:
                    try:
                        df = xl.parse(sheet)
                        df.to_excel(writer, sheet_name=safe_sheet_name(sheet), index=False)
                    except Exception:
                        continue
            if dst.exists() and dst.stat().st_size > 0:
                if log_cb:
                    log_cb(f"分類檔已轉成 xlsx（pandas/xlrd fallback）：{dst}")
                return dst
    except Exception:
        pass

    return None


def _download_single_classification_csv(market: str, force_refresh: bool = False, log_cb=None) -> Optional[Path]:
    market = str(market or "").strip()
    cfg = CLASSIFICATION_DOWNLOAD_SOURCES.get(market)
    if not cfg:
        return None
    cache_path = Path(cfg["cache_path"])
    if cache_path.exists() and not force_refresh:
        return cache_path

    last_error = None
    for attempt in range(1, CLASSIFICATION_DOWNLOAD_RETRIES + 1):
        try:
            if log_cb:
                log_cb(f"{market} 分類來源下載開始（第 {attempt}/{CLASSIFICATION_DOWNLOAD_RETRIES} 次）：{cfg['url']}")
            resp = requests.get(
                cfg["url"],
                timeout=CLASSIFICATION_DOWNLOAD_TIMEOUT,
                headers={"User-Agent": "Mozilla/5.0", "Referer": "https://mopsfin.twse.com.tw/"}
            )
            resp.raise_for_status()
            content = resp.content or b""
            if len(content) < 128:
                raise ValueError("下載內容過小，疑似失敗或非有效檔案")
            cache_path.write_bytes(content)
            probe = safe_read_csv_auto(cache_path)
            parsed = _extract_official_csv_rows(probe, market=market)
            if parsed is None or parsed.empty:
                raise RuntimeError(f"{market} CSV可讀取，但無可用官方產業資料")
            if log_cb:
                log_cb(f"{market} 官方分類CSV已下載到快取：{cache_path}｜rows={len(parsed)}")
            return cache_path
        except Exception as exc:
            last_error = exc
            log_warning(f"{market} 分類來源下載失敗（第 {attempt} 次）：{exc}")
            if log_cb:
                log_cb(f"{market} 分類來源下載失敗（第 {attempt} 次）：{exc}")
            if attempt < CLASSIFICATION_DOWNLOAD_RETRIES:
                time.sleep(min(2 * attempt, 5))
    raise RuntimeError(f"{market} 分類來源下載失敗：{last_error}")


def download_classification_book(force_refresh: bool = False, log_cb=None) -> Optional[Path]:
    if not force_refresh:
        existing = resolve_classification_book_by_market("上市")
        existing_tpex = resolve_classification_book_by_market("上櫃")
        if existing is not None and existing_tpex is not None:
            return existing

    CLASSIFICATION_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    downloaded = []
    errors = []
    for market in ("上市", "上櫃"):
        try:
            p = _download_single_classification_csv(market, force_refresh=force_refresh, log_cb=log_cb)
            if p is not None:
                downloaded.append((market, p))
        except Exception as exc:
            errors.append(f"{market}: {exc}")

    if downloaded:
        meta_paths = [str(p) for _, p in downloaded]
        meta = {
            "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source": "MOPS-CSV-TWSE+TPEX",
            "file": ", ".join(Path(p).name for _, p in downloaded),
            "path": " | ".join(meta_paths),
            "status": "ok" if len(downloaded) == 2 else "partial",
            "note": f"downloaded markets={','.join(m for m,_ in downloaded)}" + (f" | errors={' ; '.join(errors)}" if errors else ""),
        }
        _write_classification_meta(meta)
        CLASSIFICATION_MEMORY_CACHE["df"] = None
        CLASSIFICATION_MEMORY_CACHE["path"] = None
        CLASSIFICATION_MEMORY_CACHE["mtime"] = None
        CLASSIFICATION_MEMORY_CACHE["meta"] = meta
        first_path = downloaded[0][1]
        note = "已下載官方分類CSV（上市+上櫃）" if len(downloaded) == 2 else f"部分下載成功：{','.join(m for m,_ in downloaded)}"
        _set_classification_load_info(False, 0, first_path, note)
        return first_path

    fallback = next(iter(_iter_classification_candidates_prefer_xlsx()), None)
    if fallback is not None:
        _mark_classification_meta_status("fallback", note=f"download failed: {' ; '.join(errors)}", path=fallback)
        _set_classification_load_info(False, 0, fallback, f"下載失敗，使用既有檔：{' ; '.join(errors)}")
        return fallback
    _mark_classification_meta_status("download_failed", note=' ; '.join(errors) or "unknown")
    _set_classification_load_info(False, 0, None, ' ; '.join(errors) or "unknown")
    return None


def _iter_classification_candidates_prefer_xlsx():
    csv_first = []
    xlsx_second = []
    xls_after = []
    others = []
    for p in CLASSIFICATION_BOOK_CANDIDATES:
        pp = Path(p)
        if not pp.exists():
            continue
        suffix = pp.suffix.lower()
        if suffix == ".csv":
            csv_first.append(pp)
        elif suffix == ".xlsx":
            xlsx_second.append(pp)
        elif suffix == ".xls":
            xls_after.append(pp)
        else:
            others.append(pp)
    seen = set()
    ordered = []
    for pp in csv_first + xlsx_second + xls_after + others:
        key = str(pp.resolve()) if pp.exists() else str(pp)
        if key in seen:
            continue
        seen.add(key)
        ordered.append(pp)
    return ordered


def ensure_classification_book(force_refresh: bool = False, log_cb=None) -> Optional[Path]:
    if not force_refresh:
        for p in _iter_classification_candidates_prefer_xlsx():
            meta = _read_classification_meta()
            if not meta or Path(meta.get("path", "")) != p:
                _write_classification_meta(_build_classification_meta(p, source="LOCAL", note="discovered existing file"))
            return p
    return download_classification_book(force_refresh=force_refresh, log_cb=log_cb)

def resolve_classification_book() -> Optional[Path]:
    return ensure_classification_book(force_refresh=False)


def _coerce_unique_columns(columns) -> list[str]:
    seen = {}
    out = []
    for c in list(columns):
        name = str(c).strip() if c is not None else ""
        if not name:
            name = "Unnamed"
        count = seen.get(name, 0)
        out_name = name if count == 0 else f"{name}__dup{count}"
        seen[name] = count + 1
        out.append(out_name)
    return out

def _extract_official_sheet_rows(df: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["stock_id", "stock_name_official", "stock_name_norm_official", "market_official", "industry_official"])

    x = df.copy().fillna("")
    x.columns = _coerce_unique_columns(x.columns)
    market = "上市" if "上市" in str(sheet_name) else ("上櫃" if "上櫃" in str(sheet_name) else ("興櫃" if "興櫃" in str(sheet_name) else ""))

    code_candidates = []
    name_col = None
    industry_name_col = None
    industry_code_col = None

    for c in x.columns:
        s = str(c).strip()
        base = s.split("__dup")[0].strip()
        if base in ("公司名稱", "公司簡稱", "股票名稱", "證券名稱"):
            name_col = c if name_col is None else name_col
        elif base in ("新產業類別", "新產業別", "產業名稱", "industry_name"):
            industry_name_col = c if industry_name_col is None else industry_name_col
        elif base in ("產業別", "產業類別", "產業代碼", "產業類別代號", "industry_code"):
            industry_code_col = c if industry_code_col is None else industry_code_col
        elif base in ("股票代號", "證券代號", "公司代號"):
            code_candidates.append(c)
        elif base == "代號":
            code_candidates.append(c)

    industry_col = industry_name_col or industry_code_col
    if industry_col is None or not code_candidates:
        return pd.DataFrame(columns=["stock_id", "stock_name_official", "stock_name_norm_official", "market_official", "industry_official"])

    preferred = []
    for c in code_candidates:
        base = str(c).split("__dup")[0]
        if base in ("股票代號", "證券代號", "公司代號"):
            preferred.append(c)
    code_candidates = preferred + [c for c in code_candidates if c not in preferred]

    stock_id_series = pd.Series([""] * len(x), index=x.index, dtype="object")
    for c in code_candidates:
        vals = x[c].map(normalize_stock_id)
        fill_mask = stock_id_series.eq("") & vals.ne("")
        if fill_mask.any():
            stock_id_series.loc[fill_mask] = vals.loc[fill_mask]

    out = pd.DataFrame({
        "stock_id": stock_id_series.astype(str),
        "stock_name_official": x[name_col].astype(str).str.strip() if name_col in x.columns else "",
        "industry_official": x[industry_col].astype(str).str.strip(),
    })
    out["industry_official"] = out["industry_official"].map(normalize_official_industry_name)
    out["stock_name_norm_official"] = out["stock_name_official"].map(_normalize_stock_name_for_match)
    out["market_official"] = market
    out = out[(out["stock_id"] != "") & (out["industry_official"] != "")].copy()
    if out.empty:
        return out
    return out[["stock_id", "stock_name_official", "stock_name_norm_official", "market_official", "industry_official"]].drop_duplicates(subset=["stock_id"], keep="first")



def _classification_csv_candidates_by_market(market: str = "ALL") -> list[Path]:
    market = str(market or "ALL").strip()
    if market == "上市":
        return [
            RUNTIME_DIR / "台股官方產業分類_上市.csv",
            EXTERNAL_DATA_DIR / "台股官方產業分類_上市.csv",
            CLASSIFICATION_CACHE_CSV_TWSE,
            BASE_DIR / "台股官方產業分類_上市.csv",
        ]
    if market == "上櫃":
        return [
            RUNTIME_DIR / "台股官方產業分類_上櫃.csv",
            EXTERNAL_DATA_DIR / "台股官方產業分類_上櫃.csv",
            CLASSIFICATION_CACHE_CSV_TPEX,
            BASE_DIR / "台股官方產業分類_上櫃.csv",
        ]
    return _classification_csv_candidates_by_market("上市") + _classification_csv_candidates_by_market("上櫃")


def resolve_classification_book_by_market(market: str = "ALL") -> Optional[Path]:
    for p in _classification_csv_candidates_by_market(market):
        if Path(p).exists():
            return Path(p)
    if market in ("上市", "上櫃"):
        try:
            downloaded = _download_single_classification_csv(market, force_refresh=False, log_cb=classification_debug_log)
            if downloaded is not None and Path(downloaded).exists():
                return Path(downloaded)
        except Exception as exc:
            classification_debug_log(f"自動補抓官方分類失敗（{market}）：{exc}", "WARNING")
    return None


def load_official_classification_book(market: str = "ALL") -> pd.DataFrame:
    """
    官方分類來源支援：上市 / 上櫃 / ALL。
    ALL 會合併上市 + 上櫃後再回傳。
    """
    market = str(market or "ALL").strip() or "ALL"
    empty_df = pd.DataFrame(columns=["stock_id", "stock_name_official", "stock_name_norm_official", "market_official", "industry_official"])

    if market == "ALL":
        twse = load_official_classification_book("上市")
        tpex = load_official_classification_book("上櫃")
        parts = [df for df in [twse, tpex] if df is not None and not df.empty]
        if not parts:
            note = "找不到任何官方分類來源（ALL）"
            classification_debug_log(note, "WARNING")
            return empty_df
        out = pd.concat(parts, ignore_index=True).fillna("")
        out["stock_id"] = out["stock_id"].astype(str).map(normalize_stock_id)
        out["industry_official"] = out["industry_official"].astype(str).map(normalize_official_industry_name)
        out = out[(out["stock_id"] != "") & (out["industry_official"] != "")].copy()
        out = out.sort_values(["stock_id", "market_official", "industry_official"]).drop_duplicates(subset=["stock_id"], keep="first")
        classification_debug_log(f"補充分類載入成功：ALL｜rows={len(out)}")
        _set_classification_load_info(True, len(out), None, f"補充分類載入成功：ALL｜rows={len(out)}")
        return out

    path = resolve_classification_book_by_market(market)
    if path is None:
        note = f"找不到官方分類來源：{market}"
        classification_debug_log(note, "WARNING")
        return empty_df

    classification_debug_log(f"實際讀到的分類來源（{market}）：{path}")
    try:
        raw = safe_read_csv_auto(path)
        out = _extract_official_csv_rows(raw, market=market)
        if out is None or out.empty:
            raise RuntimeError(f"{market} 官方分類可讀取，但沒有有效資料")
        out["stock_id"] = out["stock_id"].astype(str).map(normalize_stock_id)
        out["industry_official"] = out["industry_official"].astype(str).map(normalize_official_industry_name)
        out = out[(out["stock_id"] != "") & (out["industry_official"] != "")].copy()
        out = out.drop_duplicates(subset=["stock_id"], keep="first")
        classification_debug_log(f"補充分類載入成功：{market}｜rows={len(out)}")
        return out
    except Exception as exc:
        classification_debug_log(f"分類來源解析失敗（{market}）：{exc}", "ERROR")
        return empty_df

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
    x["stock_name_norm_manual"] = x["stock_name"].map(_normalize_stock_name_for_match)
    x = x.drop_duplicates(subset=["stock_id"], keep="first")
    return x.rename(columns={
        "stock_name": "stock_name_manual",
        "stock_name_norm_manual": "stock_name_norm_manual",
        "market": "market_manual",
        "industry": "industry_manual",
        "theme": "theme_manual",
        "sub_theme": "sub_theme_manual",
        "is_etf": "is_etf_manual",
    })



def _ensure_object_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    x = df.copy()
    for col in columns:
        if col not in x.columns:
            x[col] = ""
        try:
            x[col] = x[col].astype("object")
        except Exception:
            x[col] = pd.Series(list(x[col]), index=x.index, dtype="object")
    return x

def _safe_text_series(series: pd.Series, default: str = "") -> pd.Series:
    if series is None:
        return pd.Series(dtype="object")
    s = pd.Series(series, copy=True)
    try:
        s = s.astype("object")
    except Exception:
        s = pd.Series(list(s), index=s.index, dtype="object")
    s = s.where(pd.notna(s), default)
    def _clean(v):
        if v is None:
            return default
        sv = str(v).strip()
        if sv in ("<NA>", "nan", "None", "NaN", "NULL", "null"):
            return default
        return sv
    s = s.map(_clean)
    try:
        s = s.fillna(default)
    except Exception:
        pass
    return s.astype("object")

def _safe_numeric_flag_series(series: pd.Series, default: int = 0) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").fillna(default)
    return s.astype(int)

def _assign_object_values(df: pd.DataFrame, mask: pd.Series, col: str, values) -> pd.DataFrame:
    x = df
    if col not in x.columns:
        x[col] = ""
    try:
        x[col] = x[col].astype("object")
    except Exception:
        x[col] = pd.Series(list(x[col]), index=x.index, dtype="object")
    vals = _safe_text_series(pd.Series(list(values), index=x.index[mask]), "")
    x.loc[mask, col] = vals.astype("object").values
    return x


def _safe_int(v, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return int(default)


def _is_placeholder_text(v) -> bool:
    return str(v or "").strip() in ("", "未分類", "未匹配", "全市場", "系統掃描", "其他", "nan", "None", "<NA>", "N/A", "NaN", "null", "NULL")


def _is_missing_classification_value(v) -> bool:
    s = str(v or "").strip()
    return s in ("", "未分類", "未匹配", "全市場", "系統掃描", "其他", "nan", "None", "<NA>", "N/A", "NaN", "null", "NULL")


def _choose_text(*values, default: str = "") -> str:
    for v in values:
        s = str(v or "").strip()
        if s and not _is_placeholder_text(s):
            return s
    return str(default or "")

def normalize_official_industry_name(v: str) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    s = s.replace(".0", "") if re.fullmatch(r"\d+\.0", s) else s
    if re.fullmatch(r"\d{1,2}", s):
        s = s.zfill(2)
    if s in INDUSTRY_CODE_MAP:
        s = INDUSTRY_CODE_MAP.get(s, s)
    return OFFICIAL_INDUSTRY_ALIAS_MAP.get(s, s)


def _write_json_safe(path: Path, payload: dict):
    try:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _write_unclassified_report(df: pd.DataFrame):
    try:
        if df is None:
            return
        CLASSIFICATION_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        engine = available_excel_engine()
        if engine:
            with pd.ExcelWriter(CLASSIFICATION_V2_UNCLASSIFIED_PATH, engine=engine) as writer:
                df.to_excel(writer, sheet_name="Unclassified", index=False)
        else:
            df.to_csv(CLASSIFICATION_V2_UNCLASSIFIED_PATH.with_suffix('.csv'), index=False, encoding='utf-8-sig')
    except Exception:
        pass


def get_classification_v2_summary() -> dict:
    global CLASSIFICATION_V2_LAST_SUMMARY
    file_data = {}
    try:
        if CLASSIFICATION_V2_SUMMARY_PATH.exists():
            raw = json.loads(CLASSIFICATION_V2_SUMMARY_PATH.read_text(encoding='utf-8'))
            if isinstance(raw, dict):
                file_data = dict(raw)
    except Exception:
        file_data = {}
    if file_data:
        mem = CLASSIFICATION_V2_LAST_SUMMARY if isinstance(CLASSIFICATION_V2_LAST_SUMMARY, dict) else {}
        mem_time = str(mem.get("report_time", "") or "")
        file_time = str(file_data.get("report_time", "") or "")
        if not mem or (file_time and file_time >= mem_time):
            CLASSIFICATION_V2_LAST_SUMMARY = dict(file_data)
            return dict(file_data)
    if isinstance(CLASSIFICATION_V2_LAST_SUMMARY, dict) and CLASSIFICATION_V2_LAST_SUMMARY:
        return dict(CLASSIFICATION_V2_LAST_SUMMARY)
    return {}



def infer_ai_classification(stock_name: str, industry_hint: str = "", market_hint: str = "", is_etf: int = 0) -> tuple[str, str, str, str, int, str]:
    name = str(stock_name or "").strip()
    industry_hint = normalize_official_industry_name(str(industry_hint or "").strip())
    market_hint = str(market_hint or "").strip()
    if int(is_etf or 0) == 1 or re.search(r"ETF|台灣50|高股息|中型100|科技優息|精選高息", name, flags=re.I):
        return ("ETF", "ETF", "ETF", "ai_infer", 98, "ETF keyword")

    ai_rules = [
        (r"華星光|上詮|聯鈞|波若威|光聖|聯亞|眾達|前鼎|立碁|環宇|光環|聯光通|眾達-KY", ("光通訊", "CPO/光模組", "高速光通訊", 94, "光通訊/CPO keyword")),
        (r"智邦|智易|中磊|啟碁|正文|建漢|神準|明泰|友訊|合勤控|振曜", ("網通", "資料中心交換器", "高階網通", 90, "網通 keyword")),
        (r"台積電|創意|世芯|世芯-KY|晶心科|聯發科|聯詠|祥碩|M31|力旺|智原|信驊", ("半導體", "AI/晶圓代工", "半導體", 92, "半導體 keyword")),
        (r"奇鋐|雙鴻|建準|高力|力致|超眾|健策", ("散熱", "AI散熱", "液冷", 90, "散熱 keyword")),
        (r"鴻海|廣達|緯創|緯穎|仁寶|英業達|和碩|神達|技嘉|華碩|微星|宏碁", ("電子代工", "AI伺服器", "伺服器", 88, "伺服器/代工 keyword")),
        (r"台達電|光寶科|康舒|群電|全漢|偉訓|順達|AES|新盛力|加百裕|系統電|飛宏|茂達", ("電源/電機", "電源/HVDC", "電源", 88, "電源 keyword")),
        (r"長榮|萬海|陽明|裕民|慧洋|中航|四維航", ("航運業", "運輸", "航運", 85, "航運 keyword")),
        (r"富邦金|國泰金|中信金|兆豐金|玉山金|元大金|第一金|華南金|永豐金|台新金", ("金融保險", "金融", "金融", 86, "金融 keyword")),
        (r"統一|大成|卜蜂|味全|愛之味|黑松|佳格", ("食品工業", "民生消費", "食品", 84, "食品 keyword")),
    ]
    for pattern, bundle in ai_rules:
        if re.search(pattern, name):
            industry, theme, sub_theme, conf, note = bundle
            return industry, theme, sub_theme, "ai_infer", conf, note

    if industry_hint in INDUSTRY_THEME_MAP:
        industry, theme, sub_theme = INDUSTRY_THEME_MAP[industry_hint]
        return industry, theme, sub_theme, "rule_engine", 76, f"industry map: {industry_hint}"

    fallback_industry, fallback_theme, fallback_sub = infer_theme_bundle(name, industry_hint, int(is_etf or 0))
    if _is_placeholder_text(fallback_theme) and _is_placeholder_text(fallback_sub):
        if market_hint == "ETF":
            return ("ETF", "ETF", "ETF", "ai_infer", 90, "market hint ETF")
        return (_choose_text(industry_hint, fallback_industry, default="未分類"), _choose_text(fallback_theme, default="全市場"), _choose_text(fallback_sub, default="系統掃描"), "ai_infer", 55, "weak fallback")
    return (_choose_text(industry_hint, fallback_industry, default="未分類"), _choose_text(fallback_theme, default="全市場"), _choose_text(fallback_sub, default="系統掃描"), "rule_engine", 68, "generic keyword fallback")


def build_classification_quality_report(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    global CLASSIFICATION_V2_LAST_SUMMARY
    if df is None or df.empty:
        summary = {
            "total": 0, "official": 0, "manual": 0, "rule_engine": 0, "ai_infer": 0,
            "unclassified": 0, "covered": 0, "coverage_pct": 0.0,
            "report_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        CLASSIFICATION_V2_LAST_SUMMARY = dict(summary)
        _write_json_safe(CLASSIFICATION_V2_SUMMARY_PATH, summary)
        _write_unclassified_report(pd.DataFrame(columns=["stock_id", "stock_name", "market", "industry_final", "theme_final", "sub_theme_final", "classification_source", "classification_confidence", "classification_note"]))
        return pd.DataFrame(), summary

    x = df.copy()
    for col in ["stock_id", "stock_name", "market", "industry_final", "theme_final", "sub_theme_final", "classification_source", "classification_confidence", "classification_note"]:
        if col not in x.columns:
            x[col] = ""
    for col in ["stock_id", "stock_name", "market", "industry_final", "theme_final", "sub_theme_final", "classification_source", "classification_note"]:
        x[col] = _safe_text_series(x[col], "")
    x["classification_confidence"] = pd.to_numeric(x["classification_confidence"], errors="coerce").fillna(0)

    unclassified_mask = (
        x["industry_final"].map(_is_missing_classification_value) |
        x["theme_final"].map(_is_missing_classification_value) |
        x["sub_theme_final"].map(_is_missing_classification_value)
    )
    unclassified = x.loc[unclassified_mask, ["stock_id", "stock_name", "market", "industry_final", "theme_final", "sub_theme_final", "classification_source", "classification_confidence", "classification_note"]].copy().sort_values(["classification_source", "stock_id"])

    source_counts = x["classification_source"].astype(str).value_counts().to_dict()
    total = int(len(x))
    unclassified_count = int(unclassified_mask.sum())
    covered = max(total - unclassified_count, 0)
    summary = {
        "total": total,
        "official": int(source_counts.get("official", 0)),
        "manual": int(source_counts.get("manual", 0)),
        "rule_engine": int(source_counts.get("rule_engine", 0)),
        "ai_infer": int(source_counts.get("ai_infer", 0)),
        "unclassified": unclassified_count,
        "covered": covered,
        "coverage_pct": round((covered / total * 100.0), 2) if total else 0.0,
        "report_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "unclassified_report": str(CLASSIFICATION_V2_UNCLASSIFIED_PATH),
    }
    CLASSIFICATION_V2_LAST_SUMMARY = dict(summary)
    _write_json_safe(CLASSIFICATION_V2_SUMMARY_PATH, summary)
    _write_unclassified_report(unclassified)
    coverage = 1.0 - float(x["industry_final"].isin(["未分類", "未匹配"]).mean()) if len(x) else 0.0
    if coverage < 0.95:
        log_warning(f"分類覆蓋率不足：{coverage:.2%}")
    return unclassified, summary





THEME_RULES = [
    (r"台積電|創意|世芯|世芯-KY|晶心|聯發科|聯詠|矽力|祥碩|M31|力旺|智原|信驊|創惟|威鋒電子", ("半導體", "AI/晶圓代工", "半導體")),
    (r"華星光|聯亞|光聖|波若威|上詮|聯鈞|眾達|環宇|前鼎|立碁|光環|聯光通|聯合再生|聯亞光|IET-KY", ("光通訊", "CPO/光模組", "高速光通訊")),
    (r"智邦|智易|中磊|啟碁|正文|建漢|神準|明泰|友訊|合勤控|振曜|康全電訊|仲琦", ("網通", "資料中心交換器", "高階網通")),
    (r"台達電|光寶科|康舒|群電|全漢|偉訓|順達|AES|新盛力|加百裕|系統電|飛宏|茂達|群光電能", ("電源/電機", "電源/HVDC", "電源")),
    (r"奇鋐|雙鴻|建準|超眾|力致|高力|健策|一詮|散熱", ("散熱", "AI散熱", "液冷")),
    (r"鴻海|廣達|緯創|緯穎|仁寶|英業達|和碩|技嘉|華碩|微星|神達|宏碁|研華", ("電子代工", "AI伺服器", "伺服器")),
    (r"南亞科|華邦電|旺宏|群聯|威剛|十銓|宇瞻|創見", ("半導體", "記憶體", "DRAM/NAND")),
    (r"日月光|京元電子|矽格|精材|頎邦|力成", ("半導體", "先進封裝", "封測")),
    (r"長榮|萬海|陽明|裕民|慧洋|中航|四維航", ("航運業", "運輸", "航運")),
    (r"富邦金|國泰金|中信金|兆豐金|玉山金|元大金|第一金|華南金|永豐金|台新金", ("金融保險", "金融", "金融")),
]

INDUSTRY_CODE_MAP = {
    "01": "水泥工業",
    "02": "食品工業",
    "03": "塑膠工業",
    "04": "紡織纖維",
    "05": "電機機械",
    "06": "電器電纜",
    "08": "玻璃陶瓷",
    "09": "造紙工業",
    "10": "鋼鐵工業",
    "11": "橡膠工業",
    "12": "汽車工業",
    "14": "建材營造",
    "15": "航運業",
    "16": "觀光餐旅",
    "17": "金融保險",
    "18": "貿易百貨",
    "20": "其他",
    "21": "化學工業",
    "22": "生技醫療",
    "23": "油電燃氣",
    "24": "半導體業",
    "25": "電腦及週邊設備業",
    "26": "光電業",
    "27": "通信網路業",
    "28": "電子零組件業",
    "29": "電子通路業",
    "30": "資訊服務業",
    "31": "其他電子業",
    "32": "文化創意業",
    "33": "農業科技業",
    "34": "電子商務",
    "35": "綠能環保",
    "36": "數位雲端",
    "37": "運動休閒",
    "38": "居家生活",
}

OFFICIAL_INDUSTRY_ALIAS_MAP = {
    "半導體業": "半導體",
    "半導體": "半導體",
    "電腦及週邊設備業": "電子代工",
    "電子工業": "電子工業",
    "電子零組件業": "電子零組件",
    "通信網路業": "網通",
    "光電業": "光通訊",
    "資訊服務業": "資訊服務",
    "其他電子業": "其他電子",
    "電機機械": "電源/電機",
    "電器電纜": "電源/電機",
    "生技醫療": "生技醫療",
    "油電燃氣": "油電燃氣",
    "其他": "其他",
    "文化創意業": "文化創意",
    "農業科技業": "農業科技",
    "電子商務": "電子商務",
    "綠能環保": "綠能環保",
    "數位雲端": "資訊服務",
    "運動休閒": "運動休閒",
    "居家生活": "居家生活",
}

INDUSTRY_THEME_MAP = {
    "食品工業": ("食品工業", "民生消費", "食品"),
    "塑膠工業": ("塑膠工業", "基礎原物料", "塑膠"),
    "紡織纖維": ("紡織纖維", "傳產", "紡織"),
    "電源/電機": ("電源/電機", "電源/HVDC", "電源"),
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
    "半導體": ("半導體", "AI/晶圓代工", "半導體"),
    "記憶體": ("半導體", "記憶體", "DRAM/NAND"),
    "先進封裝": ("半導體", "先進封裝", "封測"),
    "電腦及週邊設備業": ("電子代工", "AI伺服器", "伺服器"),
    "電子代工": ("電子代工", "AI伺服器", "伺服器"),
    "光電業": ("光電", "光電", "面板/光學"),
    "光通訊": ("光通訊", "CPO/光模組", "高速光通訊"),
    "通信網路業": ("網通", "網通/光通訊", "網通"),
    "網通": ("網通", "資料中心交換器", "高階網通"),
    "電子零組件業": ("電子零組件", "電子零組件", "零組件"),
    "電子零組件": ("電子零組件", "電子零組件", "零組件"),
    "電子通路業": ("電子通路", "電子通路", "通路"),
    "資訊服務業": ("資訊服務", "軟體/資訊服務", "資訊服務"),
    "資訊服務": ("資訊服務", "軟體/雲端", "雲端"),
    "其他電子業": ("其他電子", "電子", "其他電子"),
    "散熱": ("散熱", "AI散熱", "液冷"),
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
    "ETF": ("ETF", "ETF", "ETF"),
}


def infer_theme_bundle(stock_name: str, industry: str, is_etf: int) -> Tuple[str, str, str]:
    name = str(stock_name or "")
    industry = normalize_official_industry_name(str(industry or "").strip())
    if int(is_etf or 0) == 1 or re.search(r"ETF|台灣50|高股息|中型100|科技優息|精選高息", name):
        return "ETF", "ETF", "ETF"
    for pattern, bundle in THEME_RULES:
        if re.search(pattern, name):
            return bundle
    if industry in INDUSTRY_THEME_MAP:
        return INDUSTRY_THEME_MAP[industry]
    if re.search(r"光|通|網|訊|CPO|矽光", name):
        return (industry or "光通訊", "CPO/光模組", "高速光通訊")
    if re.search(r"交換器|路由|寬頻|通訊|5G", name):
        return (industry or "網通", "資料中心交換器", "高階網通")
    if re.search(r"電|控|達|機|電源|電池|能源", name):
        return (industry or "電源/電機", "電源/HVDC", "電源")
    if re.search(r"積電|半導體|晶|芯|封裝|測試|DRAM|記憶體", name):
        return (industry or "半導體", "半導體", "半導體")
    if re.search(r"伺服器|AI|雲端|運算|主機板", name):
        return (industry or "電子代工", "AI伺服器", "伺服器")
    return (industry or "未分類", "全市場", "系統掃描")



def apply_classification_layers(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy().fillna("")
    x = _ensure_object_columns(x, [
        "stock_id", "stock_name", "market", "industry", "theme", "sub_theme",
        "classification_source", "classification_note"
    ])

    official_twse = load_official_classification_book("上市")
    official_tpex = load_official_classification_book("上櫃")
    official_parts = [df for df in [official_twse, official_tpex] if df is not None and not df.empty]
    official = pd.concat(official_parts, ignore_index=True).drop_duplicates(subset=["stock_id"], keep="first") if official_parts else pd.DataFrame(columns=["stock_id", "stock_name_official", "stock_name_norm_official", "market_official", "industry_official"])
    manual = load_manual_theme_mapping()

    x["stock_id"] = _safe_text_series(x["stock_id"], "").map(normalize_stock_id)
    x = x[x["stock_id"] != ""].copy().reset_index(drop=True)
    if x.empty:
        return pd.DataFrame(columns=["stock_id", "stock_name", "market", "industry", "theme", "sub_theme", "is_etf", "is_active", "update_date"])

    x["stock_name"] = _safe_text_series(x["stock_name"], "")
    x["market"] = _safe_text_series(x["market"], "")
    x["industry"] = _safe_text_series(x["industry"], "")
    x["theme"] = _safe_text_series(x["theme"], "")
    x["sub_theme"] = _safe_text_series(x["sub_theme"], "")
    x["classification_source"] = _safe_text_series(x["classification_source"], "")
    x["classification_note"] = _safe_text_series(x["classification_note"], "")
    x["stock_name_norm"] = x.get("stock_name", "").map(_normalize_stock_name_for_match)

    if not official.empty:
        official = official.copy()
        official = _ensure_object_columns(official, [
            "stock_id", "stock_name_official", "stock_name_norm_official",
            "market_official", "industry_official"
        ])
        for col in ["stock_id", "stock_name_official", "stock_name_norm_official", "market_official", "industry_official"]:
            official[col] = _safe_text_series(official[col], "")
        official["industry_official"] = official["industry_official"].map(normalize_official_industry_name)
        x = x.merge(official, on="stock_id", how="left")
        x = _ensure_object_columns(x, ["stock_name_official", "stock_name_norm_official", "market_official", "industry_official"])
        for col in ["stock_name_official", "stock_name_norm_official", "market_official", "industry_official"]:
            x[col] = _safe_text_series(x[col], "")
        missing_mask = x["industry_official"].eq("")
        if missing_mask.any() and "stock_name_norm_official" in official.columns:
            official_name_map = official[official["stock_name_norm_official"].astype(str) != ""].drop_duplicates("stock_name_norm_official")
            if not official_name_map.empty:
                miss = x.loc[missing_mask, ["stock_name_norm"]].merge(
                    official_name_map[["stock_name_norm_official", "stock_name_official", "market_official", "industry_official"]],
                    left_on="stock_name_norm", right_on="stock_name_norm_official", how="left"
                )
                for col in ["stock_name_official", "market_official", "industry_official"]:
                    vals = _safe_text_series(miss[col], "")
                    orig = _safe_text_series(x.loc[missing_mask, col], "")
                    merged_vals = [v if v else o for v, o in zip(vals.tolist(), orig.tolist())]
                    x = _assign_object_values(x, missing_mask, col, merged_vals)
    else:
        for col in ["stock_name_official", "stock_name_norm_official", "market_official", "industry_official"]:
            x[col] = ""

    if not manual.empty:
        manual = manual.copy()
        manual = _ensure_object_columns(manual, [
            "stock_id", "stock_name_manual", "stock_name_norm_manual", "market_manual",
            "industry_manual", "theme_manual", "sub_theme_manual", "is_etf_manual"
        ])
        for col in ["stock_id", "stock_name_manual", "stock_name_norm_manual", "market_manual", "industry_manual", "theme_manual", "sub_theme_manual", "is_etf_manual"]:
            manual[col] = _safe_text_series(manual[col], "")
        manual["industry_manual"] = manual["industry_manual"].map(normalize_official_industry_name)
        x = x.merge(manual, on="stock_id", how="left")
        x = _ensure_object_columns(x, ["stock_name_manual", "stock_name_norm_manual", "market_manual", "industry_manual", "theme_manual", "sub_theme_manual", "is_etf_manual"])
        for col in ["stock_name_manual", "stock_name_norm_manual", "market_manual", "industry_manual", "theme_manual", "sub_theme_manual", "is_etf_manual"]:
            x[col] = _safe_text_series(x[col], "")
        missing_manual = x["industry_manual"].eq("")
        if missing_manual.any() and "stock_name_norm_manual" in manual.columns:
            manual_name_map = manual[manual["stock_name_norm_manual"].astype(str) != ""].drop_duplicates("stock_name_norm_manual")
            if not manual_name_map.empty:
                miss = x.loc[missing_manual, ["stock_name_norm"]].merge(
                    manual_name_map[["stock_name_norm_manual", "stock_name_manual", "market_manual", "industry_manual", "theme_manual", "sub_theme_manual", "is_etf_manual"]],
                    left_on="stock_name_norm", right_on="stock_name_norm_manual", how="left"
                )
                for col in ["stock_name_manual", "market_manual", "industry_manual", "theme_manual", "sub_theme_manual", "is_etf_manual"]:
                    vals = _safe_text_series(miss[col], "")
                    orig = _safe_text_series(x.loc[missing_manual, col], "")
                    merged_vals = [v if v else o for v, o in zip(vals.tolist(), orig.tolist())]
                    x = _assign_object_values(x, missing_manual, col, merged_vals)
    else:
        for col in ["stock_name_manual", "stock_name_norm_manual", "market_manual", "industry_manual", "theme_manual", "sub_theme_manual", "is_etf_manual"]:
            x[col] = ""

    x["stock_name"] = _safe_text_series(x["stock_name"], "")
    x["stock_name_official"] = _safe_text_series(x.get("stock_name_official", ""), "")
    x["stock_name_manual"] = _safe_text_series(x.get("stock_name_manual", ""), "")
    x["stock_name"] = _safe_text_series(
        x["stock_name"].replace("", pd.NA).fillna(x["stock_name_official"].replace("", pd.NA)).fillna(x["stock_name_manual"].replace("", pd.NA)).fillna(x["stock_id"]),
        ""
    )

    etf_mask = x["stock_id"].astype(str).str.startswith("00") | x["stock_name"].astype(str).str.contains("ETF|台灣50|高股息|中型100|科技優息|精選高息|DR", regex=True)
    if "is_etf_manual" in x.columns:
        etf_mask = etf_mask | _safe_numeric_flag_series(x["is_etf_manual"], 0).eq(1)
    x["is_etf"] = etf_mask.astype(int)

    x["market"] = _safe_text_series(x["market"], "")
    if "market_official" in x.columns:
        x["market"] = _safe_text_series(x["market"].replace("", pd.NA).fillna(_safe_text_series(x["market_official"], "").replace("", pd.NA)), "")
    if "market_manual" in x.columns:
        x["market"] = _safe_text_series(x["market"].replace("", pd.NA).fillna(_safe_text_series(x["market_manual"], "").replace("", pd.NA)), "")
    x["market"] = _safe_text_series(x["market"].replace("", pd.NA).fillna("上市"), "上市")
    x.loc[x["is_etf"].eq(1), "market"] = "ETF"

    if "industry_official" in x.columns:
        x["industry_official"] = _safe_text_series(x["industry_official"], "").map(normalize_official_industry_name)
    if "industry_manual" in x.columns:
        x["industry_manual"] = _safe_text_series(x["industry_manual"], "").map(normalize_official_industry_name)

    x["industry_seed"] = _safe_text_series(x["industry"], "").map(normalize_official_industry_name).replace("", pd.NA)
    if "industry_official" in x.columns:
        x["industry_seed"] = x["industry_seed"].fillna(_safe_text_series(x["industry_official"], "").replace("", pd.NA))
    if "industry_manual" in x.columns:
        x["industry_seed"] = x["industry_seed"].fillna(_safe_text_series(x["industry_manual"], "").replace("", pd.NA))
    x["industry_seed"] = _safe_text_series(x["industry_seed"].fillna("未匹配"), "未匹配")

    x["theme_seed"] = _safe_text_series(x["theme"] if "theme" in x.columns else "", "")
    x["sub_theme_seed"] = _safe_text_series(x["sub_theme"] if "sub_theme" in x.columns else "", "")

    def _clean_classification_seed(series: pd.Series) -> pd.Series:
        return _safe_text_series(series, "")

    if "theme_manual" in x.columns:
        manual_theme = _clean_classification_seed(x["theme_manual"])
        x["theme_seed"] = _safe_text_series(x["theme_seed"].replace("", pd.NA).fillna(manual_theme.replace("", pd.NA)), "")
    if "sub_theme_manual" in x.columns:
        manual_sub_theme = _clean_classification_seed(x["sub_theme_manual"])
        x["sub_theme_seed"] = _safe_text_series(x["sub_theme_seed"].replace("", pd.NA).fillna(manual_sub_theme.replace("", pd.NA)), "")

    x["classification_source"] = _safe_text_series(x.get("classification_source", ""), "")
    official_mask = _safe_text_series(x.get("industry_official", ""), "").ne("")
    x.loc[official_mask, "classification_source"] = "official"
    manual_mask = x["classification_source"].eq("") & _safe_text_series(x.get("industry_manual", ""), "").ne("")
    x.loc[manual_mask, "classification_source"] = "manual"

    x["classification_confidence"] = 0
    x.loc[x["classification_source"].eq("official"), "classification_confidence"] = 100
    x.loc[x["classification_source"].eq("manual"), "classification_confidence"] = 92

    x["classification_note"] = _safe_text_series(x.get("classification_note", ""), "")
    x.loc[x["classification_source"].eq("official"), "classification_note"] = "official workbook matched"
    x.loc[x["classification_source"].eq("manual"), "classification_note"] = "manual mapping matched"

    ai_rows = x.apply(lambda r: infer_ai_classification(r.get("stock_name", ""), r.get("industry_seed", ""), r.get("market", ""), _safe_int(r.get("is_etf", 0))), axis=1, result_type="expand")
    ai_rows.columns = ["industry_ai", "theme_ai", "sub_theme_ai", "source_ai", "confidence_ai", "note_ai"]
    x = pd.concat([x, ai_rows], axis=1)
    x = _ensure_object_columns(x, ["industry_ai", "theme_ai", "sub_theme_ai", "source_ai", "note_ai"])
    for col in ["industry_ai", "theme_ai", "sub_theme_ai", "source_ai", "note_ai"]:
        x[col] = _safe_text_series(x[col], "")
    x["confidence_ai"] = pd.to_numeric(x["confidence_ai"], errors="coerce").fillna(0).astype(int)

    x["industry_final"] = _safe_text_series(x["industry_seed"], "未匹配").map(normalize_official_industry_name)
    x["theme_final"] = _safe_text_series(x["theme_seed"], "")
    x["sub_theme_final"] = _safe_text_series(x["sub_theme_seed"], "")

    missing_industry = x["industry_final"].map(_is_missing_classification_value)
    missing_theme = x["theme_final"].map(_is_missing_classification_value)
    missing_sub = x["sub_theme_final"].map(_is_missing_classification_value)

    x.loc[missing_industry, "industry_final"] = _safe_text_series(x.loc[missing_industry, "industry_ai"], "").map(normalize_official_industry_name).values
    x.loc[missing_theme, "theme_final"] = _safe_text_series(x.loc[missing_theme, "theme_ai"], "").values
    x.loc[missing_sub, "sub_theme_final"] = _safe_text_series(x.loc[missing_sub, "sub_theme_ai"], "").values

    industry_map_rows = x["industry_final"].map(lambda s: INDUSTRY_THEME_MAP.get(normalize_official_industry_name(s), ("", "", "")))
    industry_map_df = pd.DataFrame(industry_map_rows.tolist(), columns=["industry_from_map", "theme_from_map", "sub_from_map"], index=x.index)
    x = pd.concat([x, industry_map_df], axis=1)
    x = _ensure_object_columns(x, ["industry_from_map", "theme_from_map", "sub_from_map"])
    for col in ["industry_from_map", "theme_from_map", "sub_from_map"]:
        x[col] = _safe_text_series(x[col], "")
    missing_theme = x["theme_final"].map(_is_missing_classification_value)
    missing_sub = x["sub_theme_final"].map(_is_missing_classification_value)
    x.loc[missing_theme, "theme_final"] = _safe_text_series(x.loc[missing_theme, "theme_from_map"], "").values
    x.loc[missing_sub, "sub_theme_final"] = _safe_text_series(x.loc[missing_sub, "sub_from_map"], "").values

    rule_used_mask = x["classification_source"].eq("") & x["source_ai"].isin(["rule_engine", "ai_infer"])
    x.loc[rule_used_mask, "classification_source"] = _safe_text_series(x.loc[rule_used_mask, "source_ai"], "").values
    x.loc[rule_used_mask, "classification_confidence"] = pd.to_numeric(x.loc[rule_used_mask, "confidence_ai"], errors="coerce").fillna(0).astype(int).values
    x.loc[rule_used_mask, "classification_note"] = _safe_text_series(x.loc[rule_used_mask, "note_ai"], "").values

    supplement_mask = ~x["classification_source"].eq("") & (x["theme_final"].map(_is_missing_classification_value) | x["sub_theme_final"].map(_is_missing_classification_value))
    x.loc[supplement_mask, "theme_final"] = _safe_text_series(x.loc[supplement_mask, "theme_ai"], "").values
    x.loc[supplement_mask, "sub_theme_final"] = _safe_text_series(x.loc[supplement_mask, "sub_theme_ai"], "").values
    note_mask = supplement_mask & x["classification_note"].eq("")
    x.loc[note_mask, "classification_note"] = _safe_text_series(x.loc[note_mask, "note_ai"], "").values

    for col in [
        "industry", "theme", "sub_theme",
        "industry_final", "theme_final", "sub_theme_final",
        "classification_source", "classification_note"
    ]:
        if col not in x.columns:
            x[col] = ""
        x[col] = _safe_text_series(x[col], "")

    x["industry_final"] = _safe_text_series(x["industry_final"], "未匹配").map(normalize_official_industry_name)
    x["industry_final"] = _safe_text_series(x["industry_final"], "未匹配").replace("", "未匹配")
    x["theme_final"] = _safe_text_series(x["theme_final"], "全市場").replace("", "全市場")
    x["sub_theme_final"] = _safe_text_series(x["sub_theme_final"], "系統掃描").replace("", "系統掃描")
    x.loc[x["is_etf"].eq(1), ["industry_final", "theme_final", "sub_theme_final", "classification_source", "classification_confidence", "classification_note"]] = ["ETF", "ETF", "ETF", "manual", 98, "ETF normalized"]

    x["industry"] = _safe_text_series(x["industry_final"], "未匹配")
    x["theme"] = _safe_text_series(x["theme_final"], "全市場")
    x["sub_theme"] = _safe_text_series(x["sub_theme_final"], "系統掃描")
    x["is_active"] = 1
    x["update_date"] = datetime.now().strftime("%Y-%m-%d")

    unmatched_df, summary = build_classification_quality_report(x)
    try:
        classification_debug_log(f"V2 覆蓋率 {summary.get('coverage_pct', 0):.2f}%｜官方 {summary.get('official', 0)}｜手動 {summary.get('manual', 0)}｜規則 {summary.get('rule_engine', 0)}｜AI {summary.get('ai_infer', 0)}｜未匹配 {summary.get('unclassified', 0)}")
        if unmatched_df is not None and not unmatched_df.empty:
            classification_debug_log(f"未匹配分類清單已輸出：{CLASSIFICATION_V2_UNCLASSIFIED_PATH}")
    except Exception:
        pass

    keep = ["stock_id", "stock_name", "market", "industry", "theme", "sub_theme", "is_etf", "is_active", "update_date"]
    for c in keep:
        if c not in x.columns:
            x[c] = ""
    for c in ["stock_id", "stock_name", "market", "industry", "theme", "sub_theme", "update_date"]:
        x[c] = _safe_text_series(x[c], "")
    x["is_etf"] = _safe_numeric_flag_series(x["is_etf"], 0)
    x["is_active"] = _safe_numeric_flag_series(x["is_active"], 1)
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
            x = pd.read_csv(csv_path, dtype=str).fillna("")
            return _normalize_master_df(x, "上市")
        return pd.DataFrame(columns=["stock_id", "stock_name", "market", "industry", "theme", "sub_theme", "is_etf", "is_active", "update_date"])

    try:
        csv_path = resolve_master_csv()
        if csv_path.exists():
            x = pd.read_csv(csv_path, dtype=str).fillna("")
            x = _normalize_master_df(x, "上市")
            if x is not None and not x.empty and "stock_id" in x.columns:
                x["stock_id"] = x["stock_id"].astype(str).str.strip()
                x = x[x["stock_id"].str.fullmatch(r"\d{4,5}", na=False)].copy()
                keep_cols = ["stock_id", "stock_name", "market", "industry", "theme", "sub_theme", "is_etf", "is_active", "update_date"]
                for c in keep_cols:
                    if c not in x.columns:
                        x[c] = ""
                x = x[keep_cols].drop_duplicates(subset=["stock_id"]).set_index("stock_id")
                all_df = all_df.drop_duplicates(subset=["stock_id"]).set_index("stock_id")

                invalid_text = {"", "未分類", "全市場", "系統掃描"}
                for col in ["stock_name", "market", "industry", "theme", "sub_theme"]:
                    if col in x.columns and col in all_df.columns:
                        valid_mask = ~x[col].astype(str).isin(invalid_text)
                        valid_ids = x.index[valid_mask & x.index.isin(all_df.index)]
                        if len(valid_ids) > 0:
                            all_df.loc[valid_ids, col] = x.loc[valid_ids, col]

                for col in ["is_etf", "is_active", "update_date"]:
                    if col in x.columns and col in all_df.columns:
                        valid_mask = x[col].astype(str).str.strip().ne("")
                        valid_ids = x.index[valid_mask & x.index.isin(all_df.index)]
                        if len(valid_ids) > 0:
                            all_df.loc[valid_ids, col] = x.loc[valid_ids, col]

                all_df = all_df.reset_index()
    except Exception as exc:
        log_warning(f"主檔覆蓋既有 CSV 時略過：{exc}")

    if "stock_id" not in all_df.columns:
        return pd.DataFrame(columns=["stock_id", "stock_name", "market", "industry", "theme", "sub_theme", "is_etf", "is_active", "update_date"])

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

    def get_realtime_quote(self, stock_id: str, market: str, hist: pd.DataFrame | None = None) -> dict:
        fallback = {}
        try:
            x = hist if hist is not None and not hist.empty else self.db.get_price_history(stock_id)
            if x is not None and not x.empty:
                x = x.copy()
                last = x.iloc[-1]
                fallback = {
                    'close': float(last.get('close', 0) or 0),
                    'prev_close': float(x.iloc[-2].get('close', last.get('close', 0)) or 0) if len(x) >= 2 else float(last.get('close', 0) or 0),
                    'open': float(last.get('open', last.get('close', 0)) or 0),
                    'high': float(last.get('high', last.get('close', 0)) or 0),
                    'low': float(last.get('low', last.get('close', 0)) or 0),
                }
        except Exception:
            fallback = {}
        return RealtimeQuoteEngine.build_realtime_snapshot(stock_id, market, fallback=fallback)


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


class RealtimeQuoteEngine:
    """Phase 1 tactical layer: unify TWSE MIS and Yahoo near-live quote."""

    @staticmethod
    def _safe_float(v, default=None):
        try:
            if v in (None, "", "-", "--"):
                return default
            return float(v)
        except Exception:
            return default

    @staticmethod
    def _safe_int(v, default=None):
        try:
            if v in (None, "", "-", "--"):
                return default
            return int(float(v))
        except Exception:
            return default

    @staticmethod
    def _round_price(v):
        try:
            return round(float(v), 2)
        except Exception:
            return 0.0

    @staticmethod
    def _split_prices(text):
        vals = []
        for x in str(text or '').split('_'):
            v = RealtimeQuoteEngine._safe_float(x)
            if v is not None and v > 0:
                vals.append(RealtimeQuoteEngine._round_price(v))
        return vals

    @staticmethod
    def _split_ints(text):
        vals = []
        for x in str(text or '').split('_'):
            v = RealtimeQuoteEngine._safe_int(x)
            if v is not None and v >= 0:
                vals.append(v)
        return vals

    @staticmethod
    def compute_orderbook_bias(bid_vols, ask_vols):
        buy_qty = int(sum((bid_vols or [])[:5])) if bid_vols else 0
        sell_qty = int(sum((ask_vols or [])[:5])) if ask_vols else 0
        if buy_qty == 0 and sell_qty == 0:
            return {"buy_qty": 0, "sell_qty": 0, "ratio": "-", "bias": "無有效五檔"}
        if sell_qty == 0:
            return {"buy_qty": buy_qty, "sell_qty": sell_qty, "ratio": "∞", "bias": "買盤明顯偏強"}
        ratio = buy_qty / max(sell_qty, 1)
        if ratio >= 1.5:
            bias = "買盤偏強"
        elif ratio <= 0.67:
            bias = "賣盤偏強"
        else:
            bias = "多空均衡"
        return {"buy_qty": buy_qty, "sell_qty": sell_qty, "ratio": f"{ratio:.2f}", "bias": bias}

    @staticmethod
    def fetch_twse_mis_quote(stock_id: str, market: str) -> dict | None:
        market = str(market or '')
        if market not in ('上市', '上櫃', 'ETF'):
            return None
        ex_prefix = 'otc' if market == '上櫃' else 'tse'
        ex_ch = f"{ex_prefix}_{stock_id}.tw"
        url = 'https://mis.twse.com.tw/stock/api/getStockInfo.jsp'
        params = {"ex_ch": ex_ch, "json": "1", "delay": "0", "_": str(int(time.time() * 1000))}
        headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://mis.twse.com.tw/stock/index.jsp"}
        try:
            r = requests.get(url, params=params, headers=headers, timeout=8)
            r.raise_for_status()
            data = r.json()
            msg_array = data.get('msgArray', [])
            if not msg_array:
                return None
            item = msg_array[0]
            last_trade = RealtimeQuoteEngine._safe_float(item.get('z'))
            open_price = RealtimeQuoteEngine._safe_float(item.get('o'))
            high_price = RealtimeQuoteEngine._safe_float(item.get('h'))
            low_price = RealtimeQuoteEngine._safe_float(item.get('l'))
            prev_close = RealtimeQuoteEngine._safe_float(item.get('y'))
            ask_prices = RealtimeQuoteEngine._split_prices(item.get('a'))
            bid_prices = RealtimeQuoteEngine._split_prices(item.get('b'))
            ask_vols = RealtimeQuoteEngine._split_ints(item.get('f'))
            bid_vols = RealtimeQuoteEngine._split_ints(item.get('g'))
            indicative_price = None
            if bid_prices and ask_prices:
                indicative_price = RealtimeQuoteEngine._round_price((bid_prices[0] + ask_prices[0]) / 2)
            elif bid_prices:
                indicative_price = bid_prices[0]
            elif ask_prices:
                indicative_price = ask_prices[0]
            display_price = last_trade if last_trade is not None else indicative_price
            if display_price is None and prev_close is not None:
                display_price = prev_close
            if display_price is None:
                return None
            ob = RealtimeQuoteEngine.compute_orderbook_bias(bid_vols, ask_vols)
            return {
                'close': RealtimeQuoteEngine._round_price(display_price),
                'prev_close': RealtimeQuoteEngine._round_price(prev_close if prev_close is not None else display_price),
                'open': RealtimeQuoteEngine._round_price(open_price if open_price is not None else display_price),
                'high': RealtimeQuoteEngine._round_price(high_price if high_price is not None else display_price),
                'low': RealtimeQuoteEngine._round_price(low_price if low_price is not None else display_price),
                'last_trade': RealtimeQuoteEngine._round_price(last_trade) if last_trade is not None else None,
                'indicative_price': RealtimeQuoteEngine._round_price(indicative_price) if indicative_price is not None else None,
                'bid_prices': bid_prices,
                'ask_prices': ask_prices,
                'bid_vols': bid_vols,
                'ask_vols': ask_vols,
                'buy_qty': ob['buy_qty'],
                'sell_qty': ob['sell_qty'],
                'orderbook_ratio': ob['ratio'],
                'orderbook_bias': ob['bias'],
                'quote_time': item.get('t') or item.get('tt') or '',
                'source': 'TWSE MIS 即時',
            }
        except Exception:
            return None

    @staticmethod
    def fetch_yahoo_live_quote(stock_id: str, market: str, fallback: dict | None = None) -> dict:
        fallback = dict(fallback or {})
        live_price = float(fallback.get('close', 0) or 0)
        prev_close = float(fallback.get('prev_close', live_price) or live_price)
        open_price = float(fallback.get('open', live_price) or live_price)
        high_price = float(fallback.get('high', live_price) or live_price)
        low_price = float(fallback.get('low', live_price) or live_price)
        symbol = DataEngine.yahoo_symbol(stock_id, market)
        if yf is not None:
            try:
                ticker = yf.Ticker(symbol)
                try:
                    fi = ticker.fast_info
                    if fi:
                        live_price = float(fi.get('lastPrice') or live_price)
                        prev_close = float(fi.get('previousClose') or prev_close)
                        open_price = float(fi.get('open') or open_price)
                        high_price = float(fi.get('dayHigh') or high_price)
                        low_price = float(fi.get('dayLow') or low_price)
                except Exception:
                    pass
                try:
                    info = ticker.info or {}
                    live_price = float(info.get('regularMarketPrice') or live_price)
                    prev_close = float(info.get('regularMarketPreviousClose') or prev_close)
                    open_price = float(info.get('regularMarketOpen') or open_price)
                    high_price = float(info.get('regularMarketDayHigh') or high_price)
                    low_price = float(info.get('regularMarketDayLow') or low_price)
                except Exception:
                    pass
            except Exception:
                pass
        return {
            'close': RealtimeQuoteEngine._round_price(live_price),
            'prev_close': RealtimeQuoteEngine._round_price(prev_close),
            'open': RealtimeQuoteEngine._round_price(open_price),
            'high': RealtimeQuoteEngine._round_price(high_price),
            'low': RealtimeQuoteEngine._round_price(low_price),
            'last_trade': RealtimeQuoteEngine._round_price(live_price),
            'indicative_price': RealtimeQuoteEngine._round_price(live_price),
            'bid_prices': [], 'ask_prices': [], 'bid_vols': [], 'ask_vols': [],
            'buy_qty': 0, 'sell_qty': 0, 'orderbook_ratio': '-', 'orderbook_bias': '不適用',
            'quote_time': '', 'source': 'Yahoo Finance',
        }

    @staticmethod
    def build_realtime_snapshot(stock_id: str, market: str, fallback: dict | None = None) -> dict:
        fallback = dict(fallback or {})
        quote = None
        if market in ('上市', '上櫃', 'ETF'):
            quote = RealtimeQuoteEngine.fetch_twse_mis_quote(stock_id, market)
        if quote is None:
            quote = RealtimeQuoteEngine.fetch_yahoo_live_quote(stock_id, market, fallback=fallback)
        if not quote:
            quote = {
                'close': float(fallback.get('close', 0) or 0),
                'prev_close': float(fallback.get('prev_close', fallback.get('close', 0)) or 0),
                'open': float(fallback.get('open', fallback.get('close', 0)) or 0),
                'high': float(fallback.get('high', fallback.get('close', 0)) or 0),
                'low': float(fallback.get('low', fallback.get('close', 0)) or 0),
                'bid_prices': [], 'ask_prices': [], 'bid_vols': [], 'ask_vols': [],
                'buy_qty': 0, 'sell_qty': 0, 'orderbook_ratio': '-', 'orderbook_bias': '無有效五檔',
                'quote_time': '', 'source': 'fallback', 'last_trade': None, 'indicative_price': None,
            }
        quote['stock_id'] = str(stock_id)
        quote['market'] = str(market)
        return quote


class TacticalScoreEngine:
    @staticmethod
    def calc_intraday_tactical_score(close, prev_close, open_price, high_price, low_price, support, resistance, orderbook_bias, change_pct):
        score = 50
        if change_pct >= 3:
            score += 20
        elif change_pct >= 1:
            score += 10
        elif change_pct <= -9:
            score -= 35
        elif change_pct <= -5:
            score -= 20
        elif change_pct < 0:
            score -= 8
        if close >= open_price:
            score += 8
        else:
            score -= 8
        if close >= prev_close:
            score += 8
        else:
            score -= 8
        day_range = max(high_price - low_price, 0.01)
        pos = (close - low_price) / day_range
        if pos >= 0.8:
            score += 12
        elif pos <= 0.2:
            score -= 12
        if resistance and close > resistance:
            score += 18
        elif resistance and close >= resistance * 0.995:
            score += 6
        elif support and close < support:
            score -= 18
        if change_pct >= 1.5 and close >= open_price and close >= prev_close:
            score += 10
        if orderbook_bias == '買盤明顯偏強':
            score += 12
        elif orderbook_bias == '買盤偏強':
            score += 7
        elif orderbook_bias == '賣盤偏強':
            score -= 8
        return max(0, min(100, int(score)))

    @staticmethod
    def evaluate_intraday_trade_state(close, prev_close, open_price, support, resistance, change_pct, trend_score, intraday_score, score, orderbook_bias, ma20=0, ma60=0, rsi=50):
        near_resistance = close >= resistance * 0.988 if resistance else False
        at_breakout = close >= resistance * 0.998 if resistance else False
        above_open = close >= open_price
        above_prev = close >= prev_close
        bullish_orderbook = orderbook_bias in ('買盤偏強', '買盤明顯偏強')
        structure_bullish = (close >= ma20 and close >= ma60 and ma20 >= ma60) if ma20 and ma60 else False
        if change_pct <= -9.0 or intraday_score <= 15:
            return '急跌風險', '觀望為主', 'weak'
        if support and (close < support * 0.997 or (close < support and intraday_score < 42)):
            return '跌破支撐', '減碼/防守', 'weak'
        if score >= 95 and trend_score >= 90 and intraday_score >= 85:
            if at_breakout and bullish_orderbook:
                return '突破強勢', '突破可追', 'strong'
            return '強勢追蹤', '拉回加碼', 'strong'
        if resistance and close > resistance and trend_score >= 82 and intraday_score >= 78 and score >= 86 and change_pct >= 1.8 and above_open and above_prev and bullish_orderbook:
            return '突破強勢', '突破可追', 'strong'
        if trend_score >= 82 and intraday_score >= 70 and score >= 82 and change_pct >= 0.8 and above_open and above_prev and bullish_orderbook:
            return '強勢追蹤', '拉回加碼', 'strong'
        if trend_score >= 80 and intraday_score >= 70 and score >= 75 and structure_bullish:
            return '整理偏多', '低接布局', 'bullish'
        if trend_score >= 72 and intraday_score >= 58 and score >= 70 and change_pct >= 0.3 and (above_open or structure_bullish):
            return '偏多觀察', '低接布局', 'bullish'
        if support and resistance and score >= 45 and support <= close <= resistance:
            return '區間整理', '區間操作', 'range'
        if score >= 30:
            return '轉弱警戒', '減碼/防守', 'weak'
        return '轉弱警戒', '減碼/防守', 'weak'

    @staticmethod
    def classify_leader_candidate(score, trend_score, intraday_score, close, ma20, ma60, resistance, rsi, signal, orderbook_bias):
        if (score >= 90 and trend_score >= 85 and intraday_score >= 80 and 50 <= rsi <= 70 and
            close > ma20 > ma60 and signal in ('強勢追蹤', '突破強勢', '偏多觀察') and
            orderbook_bias in ('買盤偏強', '買盤明顯偏強', '多空均衡') and
            (not resistance or close <= resistance * 1.01)):
            return '是'
        if (score >= 85 and trend_score >= 80 and intraday_score >= 70 and close > ma20 >= ma60 and
            48 <= rsi <= 72 and signal in ('強勢追蹤', '突破強勢', '偏多觀察') and orderbook_bias != '賣盤偏強' and
            (not resistance or close <= resistance * 1.003)):
            return '觀察'
        return '-'


class SingleStockNarrativeEngine:
    @staticmethod
    def build_tactical_risk_note(close, support, resistance, rsi, score, change_pct=None):
        notes = []
        if change_pct is not None and change_pct <= -7:
            notes.append('當日跌幅偏大，短線波動風險升高')
        if change_pct is not None and change_pct <= -9:
            notes.append('接近跌停級別，避免把急跌誤判為強勢買點')
        if support and close <= support * 1.01:
            notes.append('接近支撐，觀察是否守穩')
        if support and close < support:
            notes.append('已跌破支撐，需提高風險控管')
        if resistance and close >= resistance * 0.99:
            notes.append('逼近壓力，留意獲利了結賣壓')
        if resistance and close > resistance:
            notes.append('已突破壓力，觀察是否假突破')
        if rsi >= 70:
            notes.append('RSI 偏高，短線過熱風險上升')
        if rsi <= 30:
            notes.append('RSI 偏低，可能進入超跌區')
        if score < 30:
            notes.append('綜合評分偏弱，不宜積極追價')
        return '；'.join(notes) if notes else '目前技術面無明顯異常，但仍須控管部位'

    @staticmethod
    def build_ai_summary_text(ctx: dict) -> str:
        close = float(ctx.get('close', 0) or 0)
        ma20 = float(ctx.get('ma20', close) or close)
        ma60 = float(ctx.get('ma60', close) or close)
        rsi = float(ctx.get('rsi', 50) or 50)
        score = float(ctx.get('score', 0) or 0)
        trend_score = float(ctx.get('trend_score', score) or score)
        intraday_score = float(ctx.get('intraday_score', score) or score)
        support = float(ctx.get('support', 0) or 0)
        resistance = float(ctx.get('resistance', 0) or 0)
        signal = str(ctx.get('signal', '-'))
        advice = str(ctx.get('advice', '-'))
        orderbook_bias = str(ctx.get('orderbook_bias', '無'))
        orderbook_ratio = str(ctx.get('orderbook_ratio', '-'))
        change_pct = float(ctx.get('change_pct', 0) or 0)
        if close >= ma20 and close >= ma60:
            trend_text = '目前股價位於20日線與60日線之上，中期趨勢偏強。'
        elif close >= ma20 and close < ma60:
            trend_text = '目前股價站上20日線，但仍在60日線下方，屬短強中性結構。'
        elif close < ma20 and close >= ma60:
            trend_text = '目前股價跌破20日線但仍守住60日線，短線轉弱、中期待觀察。'
        else:
            trend_text = '目前股價位於20日線與60日線下方，技術面偏弱。'
        pos_text = f'目前股價位於支撐 {support:.2f} 與壓力 {resistance:.2f} 之間，仍屬區間內。' if support and resistance and support <= close <= resistance else (f'目前股價 {close:.2f} 已跌破支撐 {support:.2f}。' if support and close < support else f'目前股價 {close:.2f} 已突破壓力 {resistance:.2f}。' if resistance and close > resistance else f'目前股價 {close:.2f} 尚待確認位置。')
        rsi_text = f'RSI為 {rsi:.1f}。'
        ob_text = f'五檔力道為「{orderbook_bias}」，委買/委賣比 {orderbook_ratio}。'
        score_text = f'波段分={trend_score:.1f} / 盤中分={intraday_score:.1f} / 綜合分={score:.1f}'
        final_text = f'AI綜合判斷：訊號「{signal}」，建議「{advice}」。'
        if change_pct <= -9:
            final_text += ' 今日屬急跌型態，避免誤判強勢。'
        return '\n'.join([
            '【AI個股分析】',
            f'1. 趨勢判讀：{trend_text}',
            f'2. 位置判讀：{pos_text}',
            f'3. 動能狀態：{rsi_text}',
            f'4. 五檔力道：{ob_text}',
            f'5. 分數解讀：{score_text}',
            f'6. AI結論：{final_text}',
        ])

    @staticmethod
    def build_wave_summary_text(ctx: dict) -> str:
        return '\n'.join([
            '【波浪理論分析】',
            f"1. 波浪結構：{ctx.get('wave_structure', '-')}",
            f"2. 可能位置：{ctx.get('wave_position', '-')}",
            f"3. 交易訊號：{ctx.get('signal', '-')}｜交易型態：{ctx.get('trade_type', '-')}",
        ])

    @staticmethod
    def build_fibo_summary_text(ctx: dict) -> str:
        return '\n'.join([
            '【費波南西目標位】',
            f"1. 支撐位 / 壓力位：{float(ctx.get('support',0) or 0):.2f} / {float(ctx.get('resistance',0) or 0):.2f}",
            f"2. Fib 1.382：{float(ctx.get('target_1382',0) or 0):.2f}",
            f"3. Fib 1.618：{float(ctx.get('target_1618',0) or 0):.2f}",
            f"4. 進場區 / 停損 / RR：{ctx.get('entry_zone','-')} / {ctx.get('stop_loss','-')} / {float(ctx.get('rr',0) or 0):.2f}",
        ])

    @staticmethod
    def build_bull_bear_path_text(ctx: dict) -> str:
        support = float(ctx.get('support',0) or 0)
        resistance = float(ctx.get('resistance',0) or 0)
        target = float(ctx.get('target_1382', resistance) or resistance)
        return '\n'.join([
            '【多空路徑圖示】',
            f'◎ 多方：守住 {support:.2f} → 挑戰 {resistance:.2f} → 續強看 {target:.2f}',
            f'◎ 空方：跌破 {support:.2f} → 結構轉弱 → 反彈無法站回 {resistance:.2f} 則弱勢延續',
        ])

    @staticmethod
    def build_trade_scripts_text(ctx: dict) -> dict:
        support = float(ctx.get('support',0) or 0)
        resistance = float(ctx.get('resistance',0) or 0)
        target = float(ctx.get('target_1382', resistance) or resistance)
        bucket = str(ctx.get('state_bucket', 'range'))
        if bucket == 'strong':
            return {
                'script_a': f'劇本A（強勢突破）: 站穩 {resistance:.2f} 之上且量能續強，可順勢追蹤，下一目標看 {target:.2f}',
                'script_b': f'劇本B（拉回承接）: 回測 {support:.2f} 附近不破，可分批承接；失守則降級。',
                'script_c': f'劇本C（壓力震盪）: 接近 {resistance:.2f} 但量能不足，先等整理後再攻。',
            }
        if bucket == 'bullish':
            return {
                'script_a': f'劇本A（偏多延續）: 守住 {support:.2f} 可維持偏多，等待再挑戰 {resistance:.2f}',
                'script_b': f'劇本B（回測確認）: 回測 {support:.2f} 但止穩，可偏向低接；跌破則觀望。',
                'script_c': f'劇本C（轉強升級）: 有效突破 {resistance:.2f} 並量價配合，可升級為強勢追蹤。',
            }
        if bucket == 'weak':
            return {
                'script_a': f'劇本A（弱勢反彈）: 反彈至 {resistance:.2f} 下方仍無法突破，先視為弱勢反彈。',
                'script_b': f'劇本B（跌破續弱）: 若失守 {support:.2f}，優先控管部位。',
                'script_c': f'劇本C（止穩觀察）: 只有重新站回 {support:.2f} 並伴隨量價轉強，才考慮恢復偏多。',
            }
        return {
            'script_a': f'劇本A（區間低接）: 靠近 {support:.2f} 觀察承接力道。',
            'script_b': f'劇本B（跌破下緣）: 跌破 {support:.2f} 先轉為保守觀察。',
            'script_c': f'劇本C（突破上緣）: 有效突破 {resistance:.2f} 並量能配合，可由整理升級為偏多追蹤。',
        }

    @staticmethod
    def build_summary_block(ctx: dict) -> str:
        return f"{ctx.get('light','⚪')} {ctx.get('orderbook_bias','無')}｜盤中分 {float(ctx.get('intraday_score',0) or 0):.1f}｜{ctx.get('state_bucket','-')}｜主升候選 {ctx.get('leader_candidate','-')}"


class UITacticalPresenter:
    @staticmethod
    def derive_signal_light(signal, score=0, change_pct=0, intraday_score=0):
        if signal == '急跌風險' or change_pct <= -9.0:
            return '🔴'
        if signal in ('跌破支撐', '轉弱警戒'):
            return '🟠'
        if signal == '突破強勢':
            return '🔵'
        if signal in ('偏多觀察', '強勢追蹤', '整理偏多'):
            return '🟢'
        if signal == '區間整理':
            return '🟡'
        if score >= 45 or intraday_score >= 45:
            return '🟡'
        return '⚪'


class TacticalPlanAdapter:
    @staticmethod
    def calc_intraday_trade_plan(ctx: dict) -> dict:
        support = float(ctx.get('support', 0) or 0)
        resistance = float(ctx.get('resistance', 0) or 0)
        state = str(ctx.get('state_bucket', 'range'))
        target_1382 = float(ctx.get('target_1382', resistance) or resistance)
        if support <= 0:
            return {'entry_zone': '-', 'stop_loss': '-', 'intraday_target': '-', 'intraday_rr': None}
        if state == 'strong':
            entry_low = support * 1.002; entry_high = min(support * 1.012, resistance * 0.995) if resistance > 0 else support * 1.012; stop = support * 0.982
        elif state == 'bullish':
            entry_low = support * 1.000; entry_high = min(support * 1.010, resistance * 0.992) if resistance > 0 else support * 1.010; stop = support * 0.978
        elif state == 'range':
            entry_low = support * 0.998; entry_high = min(support * 1.006, resistance * 0.988) if resistance > 0 else support * 1.006; stop = support * 0.972
        else:
            return {'entry_zone': '-', 'stop_loss': f"{support * 0.968:.2f}", 'intraday_target': '-', 'intraday_rr': None}
        risk = entry_high - stop
        reward = max(target_1382, resistance) - entry_high
        rr = round(reward / risk, 2) if risk > 0 and reward > 0 else None
        return {'entry_zone': f"{entry_low:.2f} ~ {entry_high:.2f}", 'stop_loss': f"{stop:.2f}", 'intraday_target': f"{max(target_1382, resistance):.2f}", 'intraday_rr': rr}


class IndicatorEngine:
    """相容層：舊版仍呼叫 IndicatorEngine.attach(...)，統一導向 DataEngine.attach(...)。"""

    @staticmethod
    def attach(df: pd.DataFrame) -> pd.DataFrame:
        return DataEngine.attach(df)


class LegacyStrategyEngine:
    """相容層（未被主流程使用）：僅保留舊版評分參考，不再作為排行或交易核心。"""
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
        momentum = LegacyStrategyEngine._clamp(50 + ret20 * 2)

        trend_raw = 0
        trend_raw += 1 if pd.notna(last["ma5"]) and last["close"] > last["ma5"] else 0
        trend_raw += 1 if pd.notna(last["ma10"]) and last["ma5"] > last["ma10"] else 0
        trend_raw += 1 if pd.notna(last["ma20"]) and last["ma10"] > last["ma20"] else 0
        trend_raw += 1 if pd.notna(last["ma60"]) and last["ma20"] > last["ma60"] else 0
        trend = trend_raw * 25

        rsi = float(last["rsi14"]) if pd.notna(last["rsi14"]) else 50
        macd_hist = float(last["macd_hist"]) if pd.notna(last["macd_hist"]) else 0
        reversal = LegacyStrategyEngine._clamp((100 - abs(rsi - 55) * 1.4) * 0.6 + (50 + macd_hist * 150) * 0.4)

        vol_ma20 = df["volume"].tail(20).mean()
        vol_ratio = (float(last["volume"]) / vol_ma20) if vol_ma20 and not np.isnan(vol_ma20) else 1.0
        volume = LegacyStrategyEngine._clamp(vol_ratio * 50)

        vol20 = df["close"].pct_change().tail(20).std()
        vol20 = 0.02 if pd.isna(vol20) else float(vol20)
        risk = LegacyStrategyEngine._clamp(100 - vol20 * 1500)

        ai = LegacyStrategyEngine._clamp(momentum * 0.2 + trend * 0.25 + reversal * 0.15 + volume * 0.15 + risk * 0.25)
        total = LegacyStrategyEngine._clamp(momentum * 0.22 + trend * 0.28 + reversal * 0.15 + volume * 0.15 + risk * 0.10 + ai * 0.10)

        signal, action = LegacyStrategyEngine.signal_action(last, total)
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
            score = StrategyEngineV91.score(hist)
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




V80_KLINE_SCORE = {
    "突破強勢": 100, "強勢追蹤": 90, "整理偏多": 75, "偏多觀察": 68,
    "區間整理": 55, "轉弱警戒": 25, "急跌風險": 10,
}
V80_WAVE_SCORE = {
    "第3浪": 100, "推動浪": 92, "修正浪": 70, "整理浪": 60, "第5浪": 35,
}
V80_SAKATA_SCORE = {
    "拉回承接": 95, "偏多低接": 88, "整理偏多": 75, "區間低接": 70,
    "突破追價": 52, "觀望": 20,
}
V80_VOLUME_SCORE = {
    "買盤明顯偏強": 100, "買盤偏強": 88, "多空均衡": 60, "賣盤偏強": 28,
}
V80_WEIGHTS = {
    "kline": 0.18, "wave": 0.22, "fib": 0.14, "sakata": 0.14, "volume": 0.16, "indicator": 0.16,
}





class WaveEngine:
    @staticmethod
    def detect_wave_label(x: pd.DataFrame) -> str:
        recent = x.tail(89).copy()
        if recent.empty or len(recent) < 30:
            return "整理浪"
        close_ = float(recent.iloc[-1]["close"])
        recent_high = float(recent["high"].max())
        recent_low = float(recent["low"].min())
        ma20 = float(recent.iloc[-1]["ma20"]) if pd.notna(recent.iloc[-1]["ma20"]) else close_
        ma60 = float(recent.iloc[-1]["ma60"]) if pd.notna(recent.iloc[-1]["ma60"]) else close_
        rsi = float(recent.iloc[-1]["rsi14"]) if pd.notna(recent.iloc[-1]["rsi14"]) else 50.0
        macd_hist = float(recent.iloc[-1]["macd_hist"]) if pd.notna(recent.iloc[-1]["macd_hist"]) else 0.0

        width = max(recent_high - recent_low, 1e-6)
        pos = (close_ - recent_low) / width
        breakout = close_ >= recent_high * 0.995

        if breakout and ma20 > ma60 and macd_hist > 0 and 50 <= rsi <= 64 and 0.55 <= pos <= 0.85:
            return "第3浪"
        if breakout and (rsi > 72 or pos > 0.90):
            return "第5浪"
        if close_ > ma20 > ma60 and macd_hist > 0:
            return "推動浪"
        if close_ < ma20 and macd_hist < 0:
            return "修正浪"
        return "整理浪"


class FibEngine:
    @staticmethod
    def score_and_targets(close_: float, support: float, resistance: float) -> tuple[float, float, float]:
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
        if pos < 0:
            base = 35.0
        elif pos > 1.05:
            base = 38.0
        return round(base, 2), round(support + width * 1.382, 2), round(support + width * 1.618, 2)


class SakataEngine:
    @staticmethod
    def detect(signal: str, close_: float, ma5: float, ma10: float, ma20: float, recent_high: float) -> str:
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


class IndustryRotationEngine:
    @staticmethod
    def summarize(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=["industry", "count", "avg_total", "avg_ai", "trend_count", "hot_score", "rotation"])
        x = (
            df.groupby("industry", as_index=False)
            .agg(
                count=("stock_id", "count"),
                avg_total=("total_score", "mean"),
                avg_ai=("ai_score", "mean"),
                trend_count=("signal", lambda s: int(pd.Series(s).isin(["強勢追蹤", "整理偏多"]).sum()))
            )
        )
        x["hot_score"] = x["avg_total"] * 0.45 + x["avg_ai"] * 0.25 + x["trend_count"] * 6 + x["count"] * 2
        x["rotation"] = np.where(
            x["hot_score"] >= 75, "主升輪動",
            np.where(x["hot_score"] >= 60, "偏多輪動", np.where(x["hot_score"] >= 45, "中性輪動", "轉弱輪動"))
        )
        return x.sort_values(["hot_score", "avg_total"], ascending=False).reset_index(drop=True)



class StrategyEngineV91:
    """
    v9.2 FINAL-RELEASE 核心策略引擎：
    訊號 → 評分 → 倉位 → 交易計畫
    """
    @staticmethod
    def calc_atr(x: pd.DataFrame, n: int = 14) -> pd.Series:
        prev_close = x["close"].shift(1)
        tr = pd.concat([
            (x["high"] - x["low"]).abs(),
            (x["high"] - prev_close).abs(),
            (x["low"] - prev_close).abs()
        ], axis=1).max(axis=1)
        return tr.rolling(n).mean()

    @staticmethod
    def wave_fib_trade_model(x: pd.DataFrame) -> dict:
        last = x.iloc[-1]
        close_ = float(last["close"])
        ma20 = float(last["ma20"]) if pd.notna(last["ma20"]) else close_
        ma60 = float(last["ma60"]) if pd.notna(last["ma60"]) else close_
        rsi = float(last["rsi14"]) if pd.notna(last["rsi14"]) else 50.0
        macd_hist = float(last["macd_hist"]) if pd.notna(last["macd_hist"]) else 0.0

        recent = x.tail(89)
        recent_high = float(recent["high"].max())
        recent_low = float(recent["low"].min())
        width = max(recent_high - recent_low, 1e-6)
        pos = (close_ - recent_low) / width

        wave = WaveEngine.detect_wave_label(x)
        fib_score, fib1382, fib1618 = FibEngine.score_and_targets(close_, max(ma20, recent_low), recent_high)

        # 交易模型化
        if wave == "第3浪":
            entry_low = max(ma20, recent_low) * 1.003
            entry_high = max(ma20, recent_low) * 1.015
            primary_target = fib1382
            structure_bonus = 12
        elif wave == "推動浪":
            entry_low = max(ma20, recent_low) * 1.002
            entry_high = max(ma20, recent_low) * 1.012
            primary_target = fib1382
            structure_bonus = 8
        elif wave == "第5浪":
            entry_low = ma20 * 0.998
            entry_high = ma20 * 1.006
            primary_target = fib1618
            structure_bonus = -5
        elif wave == "修正浪":
            entry_low = recent_low * 1.002
            entry_high = recent_low * 1.010
            primary_target = fib1382
            structure_bonus = -8
        else:
            entry_low = ma20 * 0.998
            entry_high = ma20 * 1.008
            primary_target = fib1382
            structure_bonus = 0

        regime_bias = 0
        if close_ > ma20 > ma60 and macd_hist > 0:
            regime_bias += 8
        if 48 <= rsi <= 68:
            regime_bias += 6
        elif rsi > 75:
            regime_bias -= 10
        elif rsi < 35:
            regime_bias -= 8

        model_trade_score = float(fib_score) + structure_bonus + regime_bias
        return {
            "wave_trade_score": round(model_trade_score, 2),
            "entry_low_v91": round(entry_low, 2),
            "entry_high_v91": round(entry_high, 2),
            "primary_target_v91": round(primary_target, 2),
            "fib1382_v91": round(fib1382, 2),
            "fib1618_v91": round(fib1618, 2),
            "wave_pos_v91": round(pos, 3),
        }

    @staticmethod
    def decide_signal(model_score: float, trade_score: float, rr: float, rsi: float, wave: str) -> tuple[str, str]:
        if model_score >= 82 and trade_score >= 82 and rr >= 1.5 and rsi <= 72 and wave in ("第3浪", "推動浪"):
            return "BUY", "可買"
        if model_score >= 72 and trade_score >= 72 and rr >= 1.15:
            return "WEAK BUY", "預掛單"
        if model_score >= 60 and rr >= 1.0:
            return "HOLD", "觀察"
        return "AVOID", "不可買"

    @staticmethod
    def score(df: pd.DataFrame) -> Dict[str, float]:
        """
        統一核心評分輸出：
        - 供 RankingEngine / TradingPlanEngine 共用
        - 回傳欄位格式維持與 ranking_result 相容
        """
        if df is None or df.empty or len(df) < 60:
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

        x = IndicatorEngine.attach(df.copy())
        last = x.iloc[-1]

        close_ = float(last["close"])
        ma5 = float(last["ma5"]) if pd.notna(last["ma5"]) else close_
        ma10 = float(last["ma10"]) if pd.notna(last["ma10"]) else close_
        ma20 = float(last["ma20"]) if pd.notna(last["ma20"]) else close_
        ma60 = float(last["ma60"]) if pd.notna(last["ma60"]) else close_
        rsi = float(last["rsi14"]) if pd.notna(last["rsi14"]) else 50.0
        macd_hist = float(last["macd_hist"]) if pd.notna(last["macd_hist"]) else 0.0

        recent20 = x.tail(20)
        recent60 = x.tail(60)

        ret20 = (close_ / float(x.iloc[-21]["close"]) - 1) * 100 if len(x) >= 21 else 0.0
        momentum = max(0.0, min(100.0, 50 + ret20 * 2))

        trend_raw = 0
        trend_raw += 1 if close_ > ma5 else 0
        trend_raw += 1 if ma5 > ma10 else 0
        trend_raw += 1 if ma10 > ma20 else 0
        trend_raw += 1 if ma20 > ma60 else 0
        trend = float(trend_raw * 25)

        if 45 <= rsi <= 65:
            reversal = 90.0
        elif 40 <= rsi < 45 or 65 < rsi <= 70:
            reversal = 72.0
        elif 35 <= rsi < 40 or 70 < rsi <= 75:
            reversal = 50.0
        else:
            reversal = 22.0
        if macd_hist > 0:
            reversal = min(100.0, reversal + 8)

        vol_ma20 = float(recent20["volume"].mean()) if not recent20.empty else 0.0
        vol_ratio = (float(last["volume"]) / vol_ma20) if vol_ma20 > 0 else 1.0
        if vol_ratio >= 1.4:
            volume = 100.0
        elif vol_ratio >= 1.05:
            volume = 82.0
        elif vol_ratio >= 0.8:
            volume = 60.0
        else:
            volume = 28.0

        vol20 = float(x["close"].pct_change().tail(20).std()) if len(x) >= 20 else 0.02
        vol20 = 0.02 if pd.isna(vol20) else vol20
        risk = max(0.0, min(100.0, 100 - vol20 * 1500))

        recent_high = float(recent60["high"].max()) if not recent60.empty else close_
        recent_low = float(recent60["low"].min()) if not recent60.empty else close_
        mapped_signal = "區間整理"
        breakout = recent_high > 0 and close_ >= recent_high * 0.995
        strong_trend = close_ > ma5 > ma10 > ma20
        mild_trend = close_ >= ma20 and ma20 >= ma60
        if breakout and strong_trend and macd_hist > 0 and 48 <= rsi <= 62:
            mapped_signal = "突破強勢"
        elif breakout and rsi > 72:
            mapped_signal = "區間整理"
        elif strong_trend and 50 <= rsi <= 70 and macd_hist > 0:
            mapped_signal = "強勢追蹤"
        elif mild_trend and 45 <= rsi <= 68:
            mapped_signal = "整理偏多"
        elif close_ > ma20 > ma60 and macd_hist > 0 and 45 <= rsi <= 68:
            mapped_signal = "強勢追蹤"
        elif close_ >= ma20 and macd_hist >= -0.02 and 40 <= rsi <= 65:
            mapped_signal = "偏多觀察"
        elif close_ < ma20 and (rsi < 40 or macd_hist < 0):
            mapped_signal = "轉弱警戒"
        elif close_ < ma60 and rsi < 32:
            mapped_signal = "急跌風險"

        ai = max(0.0, min(100.0, momentum * 0.18 + trend * 0.22 + reversal * 0.15 + volume * 0.15 + risk * 0.12 + (8 if macd_hist > 0 else 0)))
        total = max(0.0, min(100.0, momentum * 0.20 + trend * 0.24 + reversal * 0.14 + volume * 0.14 + risk * 0.10 + ai * 0.18))

        if mapped_signal in ("突破強勢", "強勢追蹤") and total >= 78:
            action = "拉回加碼"
        elif mapped_signal in ("整理偏多", "偏多觀察") and total >= 60:
            action = "低接布局"
        elif mapped_signal == "區間整理":
            action = "區間操作"
        elif mapped_signal == "轉弱警戒":
            action = "減碼/防守"
        elif mapped_signal == "急跌風險":
            action = "觀望為主"
        else:
            action = "等待訊號"

        return {
            "momentum_score": round(momentum, 2),
            "trend_score": round(trend, 2),
            "reversal_score": round(reversal, 2),
            "volume_score": round(volume, 2),
            "risk_score": round(risk, 2),
            "ai_score": round(ai, 2),
            "total_score": round(total, 2),
            "signal": mapped_signal,
            "action": action,
        }

    @staticmethod
    def kelly_position(win_rate_pct: float, rr: float, atr_pct: float, total_capital: float, regime: str) -> dict:
        p = max(0.01, min(float(win_rate_pct) / 100.0, 0.95))
        b = max(float(rr), 0.05)
        q = 1 - p
        raw_kelly = (b * p - q) / b
        raw_kelly = max(0.0, raw_kelly)

        # 分數保守化
        regime_factor = {"多頭": 0.60, "震盪": 0.35, "空頭": 0.18}.get(regime, 0.30)
        atr_penalty = 1.0
        if atr_pct >= 8:
            atr_penalty = 0.45
        elif atr_pct >= 5:
            atr_penalty = 0.65
        elif atr_pct >= 3:
            atr_penalty = 0.82

        final_pct = min(raw_kelly * 0.5, regime_factor) * atr_penalty
        final_pct = max(0.0, min(final_pct, 0.12))
        amount = round(total_capital * final_pct, 2)

        if final_pct >= 0.08:
            tier = "核心"
        elif final_pct >= 0.04:
            tier = "標準"
        elif final_pct > 0:
            tier = "試單"
        else:
            tier = "觀察"

        return {
            "kelly_raw": round(raw_kelly * 100, 2),
            "position_pct": round(final_pct * 100, 2),
            "suggest_amount": amount,
            "position_tier_v91": tier,
        }



class IntradayLiquidityEngine:
    """
    v9.2 升級：盤中資金活性淘汰規則（以日K/近端價量做代理版）
    - Phase A：分時趨勢分 / 攻擊量分 / 區間股淘汰
    - Phase B：VWAP缺資料時用近端均價代理 + 相對市場 / 相對產業強弱
    - Phase C：主動買盤 / 大單掃盤暫以量價攻擊代理分數占位
    """
    def __init__(self, db: DBManager):
        self.db = db

    @staticmethod
    def _safe_float(v, default: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return float(default)

    @staticmethod
    def _clamp(v: float, low: float = 0.0, high: float = 100.0) -> float:
        return max(low, min(high, float(v)))

    def _benchmark_strength(self) -> float:
        for sid in ("0050", "2330"):
            hist = self.db.get_price_history(sid)
            if hist is not None and not hist.empty and len(hist) >= 25:
                x = IndicatorEngine.attach(hist.copy())
                last = x.iloc[-1]
                close_ = self._safe_float(last.get("close", 0), 0)
                close_5 = self._safe_float(x.iloc[-6]["close"], close_) if len(x) >= 6 else close_
                close_20 = self._safe_float(x.iloc[-21]["close"], close_) if len(x) >= 21 else close_
                ret5 = (close_ / max(close_5, 1e-6) - 1.0) * 100
                ret20 = (close_ / max(close_20, 1e-6) - 1.0) * 100
                return ret5 * 0.55 + ret20 * 0.45
        return 0.0

    def _industry_relative_score(self, stock_row: pd.Series, own_ret20: float) -> tuple[float, float]:
        try:
            industry = str(stock_row.get("industry", "") or "").strip()
        except Exception:
            industry = ""
        if not industry:
            return 50.0, 0.0
        master = self.db.get_master()
        if master is None or master.empty or "industry" not in master.columns:
            return 50.0, 0.0
        peers = master[master["industry"].astype(str) == industry]["stock_id"].astype(str).tolist()[:60]
        peer_scores = []
        for sid in peers:
            hist = self.db.get_price_history(sid)
            if hist is None or hist.empty or len(hist) < 25:
                continue
            try:
                c = float(hist.iloc[-1]["close"])
                c20 = float(hist.iloc[-21]["close"]) if len(hist) >= 21 else c
                peer_scores.append((c / max(c20, 1e-6) - 1.0) * 100)
            except Exception:
                continue
        if not peer_scores:
            return 50.0, 0.0
        arr = pd.Series(peer_scores, dtype=float)
        pct = float((arr <= own_ret20).mean() * 100.0)
        return self._clamp(pct), float(arr.mean())

    def evaluate(self, stock_row: pd.Series, hist: pd.DataFrame, theme_hot: bool = False) -> dict:
        out = {
            "intraday_trend_score": 0.0,
            "attack_volume_score": 0.0,
            "range_breakout_score": 0.0,
            "vwap_distance_pct": 0.0,
            "relative_strength_market": 0.0,
            "relative_strength_industry": 0.0,
            "leader_follow_score": 0.0,
            "active_buy_score": 0.0,
            "orderflow_aggression_score": 0.0,
            "large_order_scan_score": 0.0,
            "institutional_participation_score": 0.0,
            "liquidity_status": "WATCH",
            "elimination_reason": "",
            "is_mainstream_funding": 0,
            "liquidity_score": 0.0,
        }
        if hist is None or hist.empty or len(hist) < 30:
            out["liquidity_status"] = "ELIMINATE"
            out["elimination_reason"] = "歷史資料不足"
            return out

        x = IndicatorEngine.attach(hist.copy())
        last = x.iloc[-1]
        close_ = self._safe_float(last.get("close", 0), 0)
        open_ = self._safe_float(last.get("open", close_), close_)
        high_ = self._safe_float(last.get("high", close_), close_)
        low_ = self._safe_float(last.get("low", close_), close_)
        ma5 = self._safe_float(last.get("ma5", close_), close_)
        ma10 = self._safe_float(last.get("ma10", close_), close_)
        ma20 = self._safe_float(last.get("ma20", close_), close_)
        ma60 = self._safe_float(last.get("ma60", close_), close_)

        recent20 = x.tail(20)
        recent60 = x.tail(60)
        recent10 = x.tail(10)

        close5 = self._safe_float(x.iloc[-6]["close"], close_) if len(x) >= 6 else close_
        close20 = self._safe_float(x.iloc[-21]["close"], close_) if len(x) >= 21 else close_
        ret5 = (close_ / max(close5, 1e-6) - 1.0) * 100
        ret20 = (close_ / max(close20, 1e-6) - 1.0) * 100

        range20_high = self._safe_float(recent20["high"].max(), close_)
        range20_low = self._safe_float(recent20["low"].min(), close_)
        range60_high = self._safe_float(recent60["high"].max(), close_)
        range60_low = self._safe_float(recent60["low"].min(), close_)
        width20 = max(range20_high - range20_low, 1e-6)
        width60 = max(range60_high - range60_low, 1e-6)
        close_loc = (close_ - low_) / max(high_ - low_, 1e-6)
        range_pos20 = (close_ - range20_low) / width20
        range_pos60 = (close_ - range60_low) / width60

        # 用近端均價代理 VWAP / 均價線
        proxy_vwap = self._safe_float((recent10["turnover"].sum() / max(recent10["volume"].sum(), 1e-6)) if "turnover" in recent10.columns else recent10["close"].mean(), recent10["close"].mean())
        vwap_distance_pct = ((close_ / max(proxy_vwap, 1e-6)) - 1.0) * 100

        trend_score = 0.0
        if close_ > proxy_vwap:
            trend_score += 24
        if close_ > ma5 > ma10:
            trend_score += 18
        if ma10 >= ma20:
            trend_score += 12
        if recent10["close"].tail(3).is_monotonic_increasing:
            trend_score += 12
        if ret5 > 0:
            trend_score += 12
        if range_pos20 >= 0.62:
            trend_score += 12
        if close_loc >= 0.62:
            trend_score += 10
        out["intraday_trend_score"] = round(self._clamp(trend_score), 2)
        out["vwap_distance_pct"] = round(vwap_distance_pct, 2)

        vol_today = self._safe_float(last.get("volume", 0), 0)
        vol5 = self._safe_float(recent20["volume"].tail(5).mean(), vol_today)
        vol10 = self._safe_float(recent20["volume"].tail(10).mean(), vol_today)
        vol20 = self._safe_float(recent20["volume"].mean(), vol_today)
        vr5 = vol_today / max(vol5, 1e-6)
        vr10 = vol_today / max(vol10, 1e-6)
        vr20 = vol_today / max(vol20, 1e-6)
        attack_volume_score = 0.0
        if vr5 >= 1.35:
            attack_volume_score += 30
        elif vr5 >= 1.10:
            attack_volume_score += 20
        elif vr5 >= 0.90:
            attack_volume_score += 10
        if vr10 >= 1.20:
            attack_volume_score += 20
        elif vr10 >= 1.00:
            attack_volume_score += 12
        if ret5 > 0 and vr20 >= 1.00:
            attack_volume_score += 20
        if close_ > open_ and close_loc >= 0.60:
            attack_volume_score += 15
        if close_ >= range20_high * 0.995:
            attack_volume_score += 15
        out["attack_volume_score"] = round(self._clamp(attack_volume_score), 2)

        range_breakout_score = 0.0
        is_range_stock = (width60 / max(close_, 1e-6)) < 0.12 and abs(ret20) < 8
        if close_ >= range20_high * 0.995:
            range_breakout_score += 45
        elif range_pos20 >= 0.75:
            range_breakout_score += 25
        if close_ > ma20 > ma60:
            range_breakout_score += 20
        if not is_range_stock:
            range_breakout_score += 20
        out["range_breakout_score"] = round(self._clamp(range_breakout_score), 2)

        benchmark = self._benchmark_strength()
        rel_market = ret20 - benchmark
        out["relative_strength_market"] = round(rel_market, 2)

        ind_pct, ind_mean = self._industry_relative_score(stock_row, ret20)
        out["relative_strength_industry"] = round(ind_pct, 2)

        leader_follow_score = 0.0
        if rel_market > 3:
            leader_follow_score += 35
        elif rel_market > 0:
            leader_follow_score += 22
        if ind_pct >= 70:
            leader_follow_score += 30
        elif ind_pct >= 55:
            leader_follow_score += 18
        if theme_hot:
            leader_follow_score += 20
        if close_ > proxy_vwap and vr5 >= 1.0:
            leader_follow_score += 15
        out["leader_follow_score"] = round(self._clamp(leader_follow_score), 2)
        out["is_mainstream_funding"] = int(theme_hot and rel_market > 0 and ind_pct >= 50)

        # 無五檔/主動買盤資料時，使用量價攻擊代理分數占位
        candle_body = max(close_ - open_, 0.0)
        candle_range = max(high_ - low_, 1e-6)
        body_ratio = candle_body / candle_range
        active_buy_score = 0.0
        if close_ > open_:
            active_buy_score += 25
        if body_ratio >= 0.55:
            active_buy_score += 20
        if close_loc >= 0.70:
            active_buy_score += 20
        if vr5 >= 1.10 and ret5 > 0:
            active_buy_score += 20
        if rel_market > 0:
            active_buy_score += 15
        out["active_buy_score"] = round(self._clamp(active_buy_score), 2)
        out["orderflow_aggression_score"] = round(self._clamp(active_buy_score * 0.9 + attack_volume_score * 0.1), 2)
        out["large_order_scan_score"] = round(self._clamp((body_ratio * 45) + (max(vr5 - 1, 0) * 55)), 2)
        out["institutional_participation_score"] = round(self._clamp((leader_follow_score * 0.55) + (active_buy_score * 0.45)), 2)

        fail_reasons = []
        hard_fail = False
        if out["intraday_trend_score"] < 42:
            fail_reasons.append("無方向")
        if out["attack_volume_score"] < 35:
            fail_reasons.append("無攻擊量")
        if is_range_stock and out["range_breakout_score"] < 40:
            fail_reasons.append("區間股")
        if out["leader_follow_score"] < 35:
            fail_reasons.append("非主流/相對弱")
        if out["orderflow_aggression_score"] < 35:
            fail_reasons.append("主動買盤弱")

        if out["intraday_trend_score"] < 35 and out["attack_volume_score"] < 30:
            hard_fail = True
        if is_range_stock and out["orderflow_aggression_score"] < 38:
            hard_fail = True

        liquidity_score = (
            out["intraday_trend_score"] * 0.28 +
            out["attack_volume_score"] * 0.24 +
            out["range_breakout_score"] * 0.14 +
            self._clamp(out["relative_strength_market"] * 6 + 50) * 0.10 +
            out["relative_strength_industry"] * 0.10 +
            out["leader_follow_score"] * 0.08 +
            out["orderflow_aggression_score"] * 0.06
        )
        out["liquidity_score"] = round(self._clamp(liquidity_score), 2)

        if hard_fail:
            out["liquidity_status"] = "ELIMINATE"
        elif out["liquidity_score"] >= 68 and out["intraday_trend_score"] >= 55 and out["attack_volume_score"] >= 50:
            out["liquidity_status"] = "PASS"
        else:
            out["liquidity_status"] = "WATCH"

        if out["liquidity_status"] == "ELIMINATE" and not fail_reasons:
            fail_reasons = ["量縮盤整"]
        out["elimination_reason"] = " / ".join(fail_reasons[:4]) if fail_reasons else "盤中結構可接受"
        return out


class TradingPlanEngine:
    def __init__(self, db: DBManager):
        self.db = db
        self.market_engine = MarketRegimeEngine(db)
        self.intraday_engine = IntradayLiquidityEngine(db)

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
    def _ui_trade_state(decision: str, close_: float, entry_low: float, entry_high: float, rr: float, win_rate: float, liquidity_status: str = "WATCH") -> str:
        decision = str(decision or "").strip().upper()
        liquidity_status = str(liquidity_status or "WATCH").strip().upper()
        in_entry_zone = TradingPlanEngine._in_entry_zone(close_, entry_low, entry_high)

        if liquidity_status == "ELIMINATE":
            return "淘汰"
        if decision == "BUY":
            return "可買" if in_entry_zone else "準備買"
        if decision == "WEAK BUY":
            return "預掛單" if liquidity_status == "PASS" else "觀察"
        if decision == "HOLD":
            return "觀察"
        if decision == "AVOID":
            return "不可買"
        return "觀察"

    @staticmethod
    def _clamp(v: float, low: float = 0.0, high: float = 100.0) -> float:
        return max(low, min(high, float(v)))

    def _map_kline_signal(self, source_signal: str, close_: float, recent_high: float, ma5: float, ma10: float, ma20: float, ma60: float, macd_hist: float, rsi: float) -> str:
        signal = str(source_signal or "").strip()
        breakout = recent_high > 0 and close_ >= recent_high * 0.995
        strong_trend = close_ > ma5 > ma10 > ma20
        mild_trend = close_ >= ma20 and ma20 >= ma60

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

    # legacy helper removed in v9.2 FINAL-RELEASE: _wave_position is no longer used

    # legacy helper removed in v9.2 FINAL-RELEASE: _fib_score_and_targets is no longer used

    # legacy helper removed in v9.2 FINAL-RELEASE: _sakata_label is no longer used

    def _volume_label(self, vol_ratio: float, close_: float, ma20: float) -> str:
        if vol_ratio >= 1.4 and close_ >= ma20:
            return "買盤明顯偏強"
        if vol_ratio >= 1.05 and close_ >= ma20:
            return "買盤偏強"
        if vol_ratio >= 0.8:
            return "多空均衡"
        return "賣盤偏強"

    def _indicator_score(self, rsi: float, macd_hist: float, k: float, d: float) -> float:
        # RSI 分段強化（v9.2 FINAL-RELEASE）：極端值明確扣分，避免過熱/過弱仍拿高分
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

    def _calc_position_score(self, close_: float, recent_high: float, recent_low: float, wave_label: str, vol_ratio: float, range_pos60: float) -> tuple[float, str]:
        recent_high = float(recent_high or close_)
        recent_low = float(recent_low or close_)
        width = max(recent_high - recent_low, 1e-6)
        dist_to_high_pct = (recent_high - close_) / max(recent_high, 1e-6) * 100.0
        if close_ >= recent_high * 0.995 and vol_ratio >= 1.2 and wave_label in ("第3浪", "推動浪"):
            return 100.0, "突破"
        if 0.45 <= range_pos60 <= 0.78 and vol_ratio >= 0.9 and wave_label in ("整理浪", "第3浪", "推動浪"):
            return 80.0, "平台整理"
        if wave_label == "修正浪" and range_pos60 <= 0.45:
            return 30.0, "反彈"
        if wave_label == "第5浪" or dist_to_high_pct <= 1.2 or range_pos60 >= 0.88:
            return -50.0, "高檔"
        return 55.0, "中位"

    def _theme_bonus_score(self, theme: str) -> tuple[float, str]:
        t = str(theme or "")
        if any(k in t for k in ["AI", "CPO", "網通"]):
            return 15.0, "主流題材"
        if any(k in t for k in ["伺服器", "半導體", "光通訊", "散熱", "電源"]):
            return 8.0, "次主流題材"
        return 0.0, "一般題材"

    def _module_pass_flags(self, signal: str, wave_label: str, fib_score: float, sakata_label: str, volume_label: str, rsi: float, macd_hist: float, k: float, d: float) -> dict:
        kline_pass = int(str(signal) in ("突破強勢", "強勢追蹤", "整理偏多", "偏多觀察"))
        wave_pass = int(str(wave_label) in ("第3浪", "推動浪", "修正浪"))
        fib_pass = int(float(fib_score or 0) >= 65)
        sakata_pass = int(str(sakata_label) in ("拉回承接", "偏多低接", "整理偏多", "區間低接"))
        volume_pass = int(str(volume_label) in ("買盤明顯偏強", "買盤偏強"))
        indicator_pass = int((45 <= float(rsi or 0) <= 72) and float(macd_hist or 0) >= 0 and float(k or 0) >= float(d or 0))
        modules_pass_count = int(kline_pass + wave_pass + fib_pass + sakata_pass + volume_pass + indicator_pass)
        strong_signal_flag = int(modules_pass_count >= 5)
        tradable_flag = int(modules_pass_count >= 4)
        return {
            "kline_pass": kline_pass,
            "wave_pass": wave_pass,
            "fib_pass": fib_pass,
            "sakata_pass": sakata_pass,
            "volume_pass": volume_pass,
            "indicator_pass": indicator_pass,
            "modules_pass_count": modules_pass_count,
            "strong_signal_flag": strong_signal_flag,
            "tradable_flag": tradable_flag,
        }

    def _path_probabilities(self, wave_label: str, modules_pass_count: int, position_tag: str, rsi: float, distribution_flag: str = "否") -> tuple[float, float, float]:
        bull = 33.0
        rng = 34.0
        bear = 33.0
        if wave_label == "第3浪":
            bull += 18; bear -= 8
        elif wave_label == "推動浪":
            bull += 10; rng -= 2
        elif wave_label == "整理浪":
            rng += 12
        elif wave_label == "第5浪":
            bear += 15; bull -= 10
        elif wave_label == "修正浪":
            bear += 8; bull -= 6
        if modules_pass_count >= 5:
            bull += 10; bear -= 5
        elif modules_pass_count <= 2:
            bear += 12; bull -= 8
        if position_tag == "高檔":
            bear += 12; bull -= 10
        elif position_tag == "突破":
            bull += 8
        if distribution_flag == "是" or float(rsi or 0) >= 75:
            bear += 10; bull -= 8
        vals = [max(5.0, bull), max(5.0, rng), max(5.0, bear)]
        s = sum(vals)
        vals = [round(v / s * 100.0, 1) for v in vals]
        diff = round(100.0 - sum(vals), 1)
        vals[0] = round(vals[0] + diff, 1)
        return tuple(vals)

    def _operation_grade(self, wave_label: str, modules_pass_count: int, bucket: str, execution_ready: int = 0, distribution_flag: str = "否") -> str:
        if distribution_flag == "是" or wave_label == "第5浪" or bucket == "排除":
            return "C"
        if wave_label == "第3浪" and modules_pass_count >= 5 and execution_ready == 1:
            return "S"
        if modules_pass_count >= 4 and bucket == "主攻":
            return "A"
        if modules_pass_count >= 3 or bucket in ("等待拉回", "觀察"):
            return "B"
        return "C"

    # legacy helper removed in v9.2 FINAL-RELEASE: _decision is no longer used

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
                "price": 0.0,
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
                "intraday_trend_score": 0.0,
                "attack_volume_score": 0.0,
                "range_breakout_score": 0.0,
                "vwap_distance_pct": 0.0,
                "relative_strength_market": 0.0,
                "relative_strength_industry": 0.0,
                "leader_follow_score": 0.0,
                "active_buy_score": 0.0,
                "orderflow_aggression_score": 0.0,
                "large_order_scan_score": 0.0,
                "institutional_participation_score": 0.0,
                "liquidity_status": "ELIMINATE",
                "elimination_reason": "資料不足",
                "liquidity_score": 0.0,
                "is_mainstream_funding": 0,
                "final_trade_decision": "AVOID",
            }

        x = IndicatorEngine.attach(hist.copy())
        x["atr14"] = StrategyEngineV91.calc_atr(x)
        last = x.iloc[-1]
        score = StrategyEngineV91.score(x)
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
        wave_label = WaveEngine.detect_wave_label(x)
        sakata_label = SakataEngine.detect(signal, close_, ma5, ma10, ma20, recent_high)
        vol_ma20 = x["volume"].tail(20).mean()
        vol_ratio = float(last["volume"] / vol_ma20) if vol_ma20 and pd.notna(vol_ma20) else 1.0
        volume_label = self._volume_label(vol_ratio, close_, ma20)
        recent_low60 = float(recent["low"].min()) if not recent.empty else close_
        range_pos60 = (close_ - recent_low60) / max(recent_high - recent_low60, 1e-6)
        position_score, position_tag = self._calc_position_score(close_, recent_high, recent_low60, wave_label, vol_ratio, range_pos60)
        theme_bonus, theme_bonus_tag = self._theme_bonus_score(stock.get("theme", ""))

        kline_score = float(V80_KLINE_SCORE.get(signal, 55))
        wave_score = float(V80_WAVE_SCORE.get(wave_label, 60))
        fib_score, fib1382, fib1618 = FibEngine.score_and_targets(close_, support, resistance)
        sakata_score = float(V80_SAKATA_SCORE.get(sakata_label, 20))
        volume_score = float(V80_VOLUME_SCORE.get(volume_label, 60))
        indicator_score = float(self._indicator_score(rsi, macd_hist, k, d))
        module_flags = self._module_pass_flags(signal, wave_label, fib_score, sakata_label, volume_label, rsi, macd_hist, k, d)

        model_score = round(
            kline_score * V80_WEIGHTS["kline"] +
            wave_score * V80_WEIGHTS["wave"] +
            fib_score * V80_WEIGHTS["fib"] +
            sakata_score * V80_WEIGHTS["sakata"] +
            volume_score * V80_WEIGHTS["volume"] +
            indicator_score * V80_WEIGHTS["indicator"], 2
        )

        wave_trade = StrategyEngineV91.wave_fib_trade_model(x)
        atr14 = float(last["atr14"]) if pd.notna(last["atr14"]) else max(close_ * 0.03, 0.01)
        atr_pct = round((atr14 / max(close_, 0.01)) * 100, 2)

        entry_low = float(wave_trade["entry_low_v91"])
        entry_high = float(wave_trade["entry_high_v91"])
        target = float(wave_trade["primary_target_v91"])
        fib1382 = float(wave_trade["fib1382_v91"])
        fib1618 = float(wave_trade["fib1618_v91"])
        trade_type = f"波浪+費波模型({wave_label})"

        stop = max(support * 0.97, entry_low - atr14 * 1.5)
        risk = max(entry_high - stop, 0.01)
        reward = max(target - entry_high, 0.0)
        rr = round(reward / risk, 2)

        trend_ok = int(close_ > ma5 > ma10 > ma20)
        macd_ok = int(macd_hist > 0)
        kd_ok = int(k >= d)
        volume_ok = int(vol_ratio >= 1.0)

        win_grade, win_rate = WinRateEngine.estimate(hist)
        decision, auto_state = StrategyEngineV91.decide_signal(model_score, float(wave_trade["wave_trade_score"]), rr, rsi, wave_label)
        if module_flags["modules_pass_count"] < 4:
            decision = "HOLD" if module_flags["modules_pass_count"] == 3 and decision != "AVOID" else "AVOID"
            auto_state = "觀察" if decision == "HOLD" else "不可買"

        preferred_theme = any(key.lower() in str(stock.get("theme", "")).lower() for key in ThemeStrengthEngine.PREFERRED_KEYWORDS)
        liquidity = self.intraday_engine.evaluate(stock, hist, theme_hot=preferred_theme)
        liquidity_status = str(liquidity.get("liquidity_status", "WATCH") or "WATCH")
        elimination_reason = str(liquidity.get("elimination_reason", "") or "")

        if liquidity_status == "ELIMINATE":
            decision = "AVOID"
            auto_state = "淘汰"
        elif liquidity_status == "WATCH" and decision == "BUY":
            decision = "HOLD"
            auto_state = "觀察"
        elif liquidity_status == "WATCH" and decision == "WEAK BUY":
            auto_state = "觀察"

        if is_etf:
            bucket = "防守"
        elif liquidity_status == "ELIMINATE":
            bucket = "淘汰"
        elif decision == "BUY" and module_flags["modules_pass_count"] >= 4:
            bucket = "主攻"
        elif decision in ("WEAK BUY", "HOLD") or liquidity_status == "WATCH" or module_flags["modules_pass_count"] == 3:
            bucket = "觀察"
        else:
            bucket = "排除"

        mainstream_score = round(score["momentum_score"] * 0.35 + score["volume_score"] * 0.25 + score["trend_score"] * 0.20 + score["ai_score"] * 0.20 - score["risk_score"] * 0.25 + theme_bonus, 2)
        breakout_score = round(score["momentum_score"] * 0.30 + score["volume_score"] * 0.25 + score["trend_score"] * 0.20 + score["ai_score"] * 0.15 + position_score * 0.10 - score["risk_score"] * 0.30 + theme_bonus, 2)

        selection_score = round(model_score * 0.38 + float(wave_trade["wave_trade_score"]) * 0.12 + win_rate * 0.10 + min(rr, 3.0) * 5 + float(liquidity.get("liquidity_score", 0)) * 0.18 + module_flags["modules_pass_count"] * 3.8 + max(position_score, 0) * 0.05 + theme_bonus * 0.8 + (6 if decision == "BUY" else 2 if decision == "WEAK BUY" else 0), 2)
        trade_score = round(model_score * 0.22 + float(wave_trade["wave_trade_score"]) * 0.18 + score["ai_score"] * 0.06 + win_rate * 0.10 + min(rr, 3.0) * 5 + float(liquidity.get("intraday_trend_score", 0)) * 0.12 + float(liquidity.get("attack_volume_score", 0)) * 0.10 + float(liquidity.get("leader_follow_score", 0)) * 0.08 + module_flags["modules_pass_count"] * 4.0 + max(position_score, 0) * 0.06 + theme_bonus * 0.9 + (6 if preferred_theme else 0), 2)

        bull_prob, range_prob, bear_prob = self._path_probabilities(wave_label, module_flags["modules_pass_count"], position_tag, rsi)
        operation_grade = self._operation_grade(wave_label, module_flags["modules_pass_count"], bucket, 0, "否")

        reason = (
            f"{signal}｜{wave_label}｜{trade_type}｜{volume_label}｜{position_tag}｜{theme_bonus_tag}｜"
            f"活性 {float(liquidity.get('liquidity_score',0) or 0):.1f}｜{liquidity_status}｜{elimination_reason or '盤中結構可接受'}｜"
            f"六模組 {model_score:.1f}｜成立數 {module_flags['modules_pass_count']}｜RR {rr:.2f}｜RSI {rsi:.1f}"
        )

        ui_state = self._ui_trade_state(decision, close_, entry_low, entry_high, rr, win_rate, liquidity_status)
        tactical_light = UITacticalPresenter.derive_signal_light(signal, model_score, 0, float(liquidity.get("intraday_trend_score", 0) or 0))
        if is_etf:
            bucket = "防守"
            operation_grade = "B"
        else:
            if tactical_light in ("🔵", "🟢"):
                bucket = "主攻"
                operation_grade = "S" if tactical_light == "🔵" else "A"
            elif tactical_light in ("🟡", "🟠"):
                bucket = "觀察"
                operation_grade = "B"
            else:
                bucket = "排除"
                operation_grade = "C"
        if tactical_light in ("🔴", "⚪"):
            decision = "AVOID"
            ui_state = "不可買" if liquidity_status != "ELIMINATE" else "淘汰"
        elif tactical_light in ("🟡", "🟠") and decision == "BUY":
            decision = "HOLD"
            ui_state = "觀察"

        return {
            "stock_id": stock_id,
            "stock_name": stock["stock_name"],
            "industry": stock["industry"],
            "theme": stock["theme"],
            "market": stock["market"],
            "price": round(close_, 2),
            "is_etf": 1 if is_etf else 0,
            "tactical_light": tactical_light,
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
            "position_score": round(position_score, 2),
            "position_tag": position_tag,
            "theme_bonus": round(theme_bonus, 2),
            "mainstream_score": mainstream_score,
            "breakout_score": breakout_score,
            "kline_pass": module_flags["kline_pass"],
            "wave_pass": module_flags["wave_pass"],
            "fib_pass": module_flags["fib_pass"],
            "sakata_pass": module_flags["sakata_pass"],
            "volume_pass_flag": module_flags["volume_pass"],
            "indicator_pass": module_flags["indicator_pass"],
            "modules_pass_count": module_flags["modules_pass_count"],
            "strong_signal_flag": module_flags["strong_signal_flag"],
            "tradable_flag": module_flags["tradable_flag"],
            "operation_grade": operation_grade,
            "bull_prob": bull_prob,
            "range_prob": range_prob,
            "bear_prob": bear_prob,
            "model_score": model_score,
            "wave_trade_score": round(float(wave_trade["wave_trade_score"]), 2),
            "atr14": round(atr14, 4),
            "atr_pct": atr_pct,
            "sakata_label": sakata_label,
            "volume_label": volume_label,
            "intraday_trend_score": round(float(liquidity.get("intraday_trend_score", 0) or 0), 2),
            "attack_volume_score": round(float(liquidity.get("attack_volume_score", 0) or 0), 2),
            "range_breakout_score": round(float(liquidity.get("range_breakout_score", 0) or 0), 2),
            "vwap_distance_pct": round(float(liquidity.get("vwap_distance_pct", 0) or 0), 2),
            "relative_strength_market": round(float(liquidity.get("relative_strength_market", 0) or 0), 2),
            "relative_strength_industry": round(float(liquidity.get("relative_strength_industry", 0) or 0), 2),
            "leader_follow_score": round(float(liquidity.get("leader_follow_score", 0) or 0), 2),
            "active_buy_score": round(float(liquidity.get("active_buy_score", 0) or 0), 2),
            "orderflow_aggression_score": round(float(liquidity.get("orderflow_aggression_score", 0) or 0), 2),
            "large_order_scan_score": round(float(liquidity.get("large_order_scan_score", 0) or 0), 2),
            "institutional_participation_score": round(float(liquidity.get("institutional_participation_score", 0) or 0), 2),
            "liquidity_status": liquidity_status,
            "liquidity_score": round(float(liquidity.get("liquidity_score", 0) or 0), 2),
            "elimination_reason": elimination_reason,
            "is_mainstream_funding": int(liquidity.get("is_mainstream_funding", 0) or 0),
            "final_trade_decision": decision if liquidity_status == "PASS" else ("WATCH" if liquidity_status == "WATCH" else "ELIMINATE"),
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
                "today_buy": empty, "wait_pullback": empty, "execution_pool": empty,
                "theme_summary": empty, "eliminated": empty
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
            if progress_cb and (idx2 % 25 == 0 or idx2 == total):
                progress_cb(idx2, total, sid)
            if log_cb and (idx2 % 100 == 0 or idx2 == total):
                log_cb(f"AI選股分析進度 {idx2}/{total}｜{sid}")
        plans_df = pd.DataFrame(plans)

        market = self.market_engine.get_market_regime()
        if plans_df.empty:
            empty = pd.DataFrame()
            return {
                "market": market, "trade_top20": empty, "attack": empty, "watch": empty, "defense": empty,
                "today_buy": empty, "wait_pullback": empty, "execution_pool": empty,
                "theme_summary": ThemeStrengthEngine.summarize(base), "eliminated": empty
            }

        preferred_mask = plans_df["theme"].isin(hot_themes) if hot_themes else pd.Series([True] * len(plans_df), index=plans_df.index)
        top_themes = ThemeStrengthEngine.summarize(base).head(3)["theme"].astype(str).tolist() if not base.empty else []
        plans_df["theme_bonus_dynamic"] = plans_df["theme"].astype(str).apply(lambda t: 15.0 if t in top_themes else (8.0 if t in hot_themes else 0.0))
        if "mainstream_score" not in plans_df.columns:
            plans_df["mainstream_score"] = plans_df.get("selection_score", 0)
        plans_df["mainstream_score"] = pd.to_numeric(plans_df["mainstream_score"], errors="coerce").fillna(0) + plans_df["theme_bonus_dynamic"]
        if "breakout_score" not in plans_df.columns:
            plans_df["breakout_score"] = plans_df.get("selection_score", 0)
        plans_df["breakout_score"] = pd.to_numeric(plans_df["breakout_score"], errors="coerce").fillna(0) + plans_df["theme_bonus_dynamic"]

        # 依 SOP 順序：先篩決策，再支撐>0，再壓力>支撐，最後按六模組總分排序
        eligible = plans_df[
            (plans_df["support"] > 0) &
            (plans_df["resistance"] > plans_df["support"])
        ].copy()
        eliminated = eligible[eligible["liquidity_status"].eq("ELIMINATE")].copy()
        tradable = eligible[~eligible["liquidity_status"].eq("ELIMINATE")].copy()

        if not tradable.empty:
            tradable["decision_rank"] = tradable["decision"].map({"BUY": 3, "WEAK BUY": 2, "HOLD": 1}).fillna(0)
            tradable["preferred_rank"] = preferred_mask.reindex(tradable.index).fillna(False).astype(int)
            tradable = tradable.sort_values(["modules_pass_count", "liquidity_score", "model_score", "trade_score", "win_rate"], ascending=False)

        mainstream_top20 = tradable.sort_values(["mainstream_score", "modules_pass_count", "liquidity_score", "model_score"], ascending=False).head(20).copy() if not tradable.empty else pd.DataFrame()
        breakout_top20 = tradable.sort_values(["breakout_score", "modules_pass_count", "position_score", "trade_score"], ascending=False).head(20).copy() if not tradable.empty else pd.DataFrame()
        combined_parts = []
        if not mainstream_top20.empty:
            tmp = mainstream_top20.copy(); tmp["candidate_engine"] = "主流TOP20"; combined_parts.append(tmp)
        if not breakout_top20.empty:
            tmp = breakout_top20.copy(); tmp["candidate_engine"] = "起爆TOP20"; combined_parts.append(tmp)
        trade_top20 = pd.concat(combined_parts, ignore_index=True) if combined_parts else tradable.head(20).copy()
        if not trade_top20.empty:
            trade_top20["stock_id"] = trade_top20["stock_id"].astype(str)
            trade_top20 = trade_top20.sort_values(["modules_pass_count", "liquidity_score", "model_score", "trade_score", "win_rate"], ascending=False)
            trade_top20 = trade_top20.drop_duplicates(subset=["stock_id"], keep="first").head(20).copy()
            trade_top20["pool_role"] = "交易候選池"

        attack = trade_top20[(trade_top20["decision"] == "BUY") & (trade_top20["liquidity_status"] == "PASS")].copy()
        watch = trade_top20[(trade_top20["liquidity_status"] == "WATCH") | (trade_top20["decision"].isin(["WEAK BUY", "HOLD"]))].copy()

        defense = plans_df[
            (plans_df["is_etf"] == 1) &
            (plans_df["support"] > 0) &
            (plans_df["resistance"] > plans_df["support"])
        ].copy()
        if not defense.empty:
            defense = defense.sort_values(["model_score", "trade_score", "rr", "win_rate"], ascending=False).head(10)
            defense["pool_role"] = "防守池"

        # 依規格書：Today_Buy / Wait_Pullback 必須只從 Trade_TOP20 派生，不可從 tradable 另選
        today_buy = trade_top20[
            (trade_top20["decision"].isin(["BUY", "WEAK BUY"])) &
            (trade_top20["modules_pass_count"] >= 4) &
            (trade_top20["liquidity_status"].isin(["PASS", "WATCH"])) &
            (trade_top20["rr"] >= 1.15) &
            (trade_top20["win_rate"] >= max(55.0, market["min_win_rate"] - 12))
        ].sort_values(["modules_pass_count", "liquidity_score", "model_score"], ascending=False).copy()
        if not today_buy.empty:
            today_buy["pool_role"] = "今日可買"

        wait_pullback = trade_top20[
            ((trade_top20["decision"].isin(["WEAK BUY", "HOLD"])) | (trade_top20["modules_pass_count"] == 3)) &
            (trade_top20["liquidity_status"].isin(["PASS", "WATCH"])) &
            (trade_top20["rr"] >= 1.0)
        ].sort_values(["modules_pass_count", "liquidity_score", "model_score"], ascending=False).copy()
        if not wait_pullback.empty:
            wait_pullback["pool_role"] = "等待拉回"

        execution_parts = []
        if not today_buy.empty:
            execution_parts.append(today_buy.copy())
        if not wait_pullback.empty:
            execution_parts.append(wait_pullback.copy())
        execution_pool = pd.concat(execution_parts, ignore_index=True) if execution_parts else pd.DataFrame(columns=trade_top20.columns.tolist() + ["pool_role"])
        if not execution_pool.empty and "stock_id" in execution_pool.columns:
            execution_pool = execution_pool.drop_duplicates(subset=["stock_id"], keep="first").reset_index(drop=True)

        dynamic_n = max(1, min(10, market["max_positions"] + 2))
        execution_pool = execution_pool.head(dynamic_n * 2).copy() if not execution_pool.empty else execution_pool

        return {
            "market": market,
            "trade_top20": trade_top20,
            "attack": attack.head(10),
            "watch": watch.head(10),
            "defense": defense.head(10),
            "today_buy": today_buy.head(dynamic_n),
            "wait_pullback": wait_pullback.head(dynamic_n),
            "execution_pool": execution_pool,
            "theme_summary": ThemeStrengthEngine.summarize(base),
            "mainstream_top20": mainstream_top20.head(20) if not mainstream_top20.empty else pd.DataFrame(),
            "breakout_top20": breakout_top20.head(20) if not breakout_top20.empty else pd.DataFrame(),
            "eliminated": eliminated.head(20),
        }



class SelectionEngine:  # deprecated compatibility helper, not used by v9.2 FINAL-RELEASE main flow
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
    v9.2 FINAL-RELEASE：
    - 真回測核心：單一模擬邏輯，供摘要統計與 Equity Curve 共用
    - 目前為簡化交易模型，不含滑價 / 手續費 / 多策略參數化；estimate_trade_quality 與 Equity Curve 共用同一核心
    - 輸出：勝率 / 平均報酬 / 平均RR / CAGR / MDD / Sharpe / 樣本數
    """
    def __init__(self, db: DBManager):
        self.db = db

    def simulate_trades(self, stock_id: str) -> pd.DataFrame:
        hist = self.db.get_price_history(stock_id)
        if hist is None or hist.empty or len(hist) < 140:
            return pd.DataFrame(columns=["win", "ret", "rr"])

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

        return pd.DataFrame(trades)

    def estimate_trade_quality(self, stock_id: str) -> dict:
        t = self.simulate_trades(stock_id)
        if t.empty:
            return {"backtest_win_rate": 45.0, "avg_return": 0.0, "avg_rr": 1.0, "cagr": 0.0, "mdd": 0.0, "sharpe": 0.0, "samples": 0}

        returns = t["ret"].astype(float)
        equity = (1 + returns).cumprod()
        years = max(len(returns) / 48.0, 0.25)
        cagr = (equity.iloc[-1] ** (1 / years) - 1) if len(equity) and equity.iloc[-1] > 0 else 0.0
        running_max = equity.cummax()
        dd = (equity / running_max - 1.0).min() if len(equity) else 0.0
        sharpe = (returns.mean() / (returns.std() + 1e-9)) * np.sqrt(48) if len(returns) > 1 else 0.0

        return {
            "backtest_win_rate": round(float(t["win"].mean() * 100), 2),
            "avg_return": round(float(t["ret"].mean() * 100), 2),
            "avg_rr": round(float(t["rr"].mean()), 2),
            "cagr": round(float(cagr * 100), 2),
            "mdd": round(float(dd * 100), 2),
            "sharpe": round(float(sharpe), 2),
            "samples": int(len(t)),
        }



class CapitalConfig:
    TOTAL_CAPITAL = 1000000.0
    MAX_TOTAL_EXPOSURE_PCT = 0.60
    MAX_SINGLE_POSITION_PCT = 0.12
    MAX_THEME_EXPOSURE_PCT = 0.25
    MAX_INDUSTRY_EXPOSURE_PCT = 0.30
    LOT_SIZE = 1000


class PortfolioEngine:
    """
    v9.2 FINAL-RELEASE：
    - 唯一資金管理引擎：依市場狀態、模型分數、交易分數、勝率、RR、ATR、Kelly 做配置
    - 控制總曝險、單檔曝險、題材集中度、產業集中度
    - 產出可執行『機構交易計畫』
    """
    def __init__(self, db: DBManager):
        self.db = db
        self.market_engine = MarketRegimeEngine(db)

    def _base_risk_budget_pct(self, regime: str) -> float:
        return {"多頭": 0.60, "震盪": 0.40, "空頭": 0.20}.get(regime, 0.35)

    def _score_strength(self, row: pd.Series) -> float:
        model_score = float(row.get("model_score", 0) or 0)
        trade_score = float(row.get("wave_trade_score", row.get("trade_score", 0)) or 0)
        win_rate = float(row.get("win_rate", 0) or 0)
        rr = float(row.get("rr", 0) or 0)
        decision = str(row.get("decision", ""))
        ui_state = str(row.get("ui_state", ""))

        score = 0.0
        score += min(max((model_score - 60) / 40, 0), 1) * 0.30
        score += min(max((trade_score - 60) / 40, 0), 1) * 0.25
        score += min(max((win_rate - 45) / 45, 0), 1) * 0.25
        score += min(rr / 3.0, 1) * 0.20

        if decision == "BUY":
            score += 0.10
        if ui_state == "預掛單":
            score -= 0.25
        elif ui_state == "準備買":
            score -= 0.10
        return max(0.0, min(score, 1.25))

    def build_institutional_plan(self, candidates: pd.DataFrame) -> pd.DataFrame:
        cols = [
            "優先級","代號","名稱","現價","市場","產業","題材","分類","狀態","盤中狀態","活性分","淘汰原因","進場區","停損",
            "1.382","1.618","RR","勝率","模型分數","交易分數","ATR%","Kelly%","建議張數","建議金額","單檔曝險%",
            "題材曝險%","產業曝險%","投資組合狀態","風險備註"
        ]
        if candidates is None or candidates.empty:
            return pd.DataFrame(columns=cols)

        market = self.market_engine.get_market_regime()
        total_capital = CapitalConfig.TOTAL_CAPITAL
        total_budget = total_capital * min(CapitalConfig.MAX_TOTAL_EXPOSURE_PCT, self._base_risk_budget_pct(market["regime"]))
        max_single_amt = total_capital * CapitalConfig.MAX_SINGLE_POSITION_PCT
        max_theme_amt = total_capital * CapitalConfig.MAX_THEME_EXPOSURE_PCT
        max_industry_amt = total_capital * CapitalConfig.MAX_INDUSTRY_EXPOSURE_PCT

        x = candidates.copy()
        x["strength"] = x.apply(self._score_strength, axis=1)
        x = x.sort_values(["strength","model_score","win_rate","rr"], ascending=False).reset_index(drop=True)

        theme_alloc = {}
        industry_alloc = {}
        deployed = 0.0
        rows = []

        for i, (_, r) in enumerate(x.iterrows(), start=1):
            close_proxy = float(r.get("entry_low", 0) or 0) or float(r.get("support", 0) or 0) or 1.0
            theme = str(r.get("theme", "全市場") or "全市場")
            industry = str(r.get("industry", "未分類") or "未分類")
            desired_amt = total_budget * (0.06 + 0.12 * float(r["strength"]))
            desired_amt = min(desired_amt, max_single_amt)

            # regime / state control
            if str(r.get("ui_state","")) == "預掛單":
                desired_amt = 0.0
            elif str(r.get("ui_state","")) == "準備買":
                desired_amt *= 0.5

            remain_total = max(total_budget - deployed, 0.0)
            remain_theme = max(max_theme_amt - theme_alloc.get(theme, 0.0), 0.0)
            remain_industry = max(max_industry_amt - industry_alloc.get(industry, 0.0), 0.0)
            allowed_amt = min(desired_amt, remain_total, remain_theme, remain_industry)

            kelly = StrategyEngineV91.kelly_position(
                win_rate_pct=float(r.get("win_rate", 0) or 0),
                rr=float(r.get("rr", 0) or 0),
                atr_pct=float(r.get("atr_pct", 0) or 0),
                total_capital=total_capital,
                regime=market["regime"],
            )
            desired_amt = min(desired_amt, kelly["suggest_amount"]) if kelly["suggest_amount"] > 0 else 0.0

            qty = 0.0
            amount = 0.0
            if allowed_amt > 0 and close_proxy > 0:
                raw_qty = min(allowed_amt, desired_amt if desired_amt > 0 else allowed_amt) / close_proxy / CapitalConfig.LOT_SIZE
                qty = max(0.0, int(raw_qty * 2) / 2.0)  # 0.5 張階梯
                amount = round(qty * CapitalConfig.LOT_SIZE * close_proxy, 2)

            if qty > 0:
                deployed += amount
                theme_alloc[theme] = theme_alloc.get(theme, 0.0) + amount
                industry_alloc[industry] = industry_alloc.get(industry, 0.0) + amount
                portfolio_state = "可執行"
            else:
                portfolio_state = "等待/預掛"

            single_pct = round(amount / total_capital * 100, 2) if total_capital else 0.0
            theme_pct = round(theme_alloc.get(theme, 0.0) / total_capital * 100, 2) if total_capital else 0.0
            industry_pct = round(industry_alloc.get(industry, 0.0) / total_capital * 100, 2) if total_capital else 0.0

            note_parts = [f"市場={market['regime']}"]
            if str(r.get("liquidity_status","")) == "ELIMINATE":
                note_parts.append(f"淘汰={r.get('elimination_reason','')}")
            if desired_amt > remain_theme:
                note_parts.append("受題材曝險上限限制")
            if desired_amt > remain_industry:
                note_parts.append("受產業曝險上限限制")
            if desired_amt > remain_total:
                note_parts.append("受總曝險上限限制")
            if str(r.get("ui_state","")) == "預掛單":
                note_parts.append("未到價不進場")
            elif str(r.get("ui_state","")) == "準備買":
                note_parts.append("僅半倉等待確認")
            elif qty > 0:
                note_parts.append("符合執行條件")

            rows.append({
                "優先級": i,
                "代號": r.get("stock_id",""),
                "名稱": r.get("stock_name",""),
                "現價": f"{float(r.get('price', 0) or 0):.2f}",
                "市場": r.get("market",""),
                "產業": industry,
                "題材": theme,
                "分類": r.get("bucket",""),
                "狀態": r.get("ui_state",""),
                "盤中狀態": r.get("liquidity_status",""),
                "活性分": round(float(r.get("liquidity_score",0) or 0),2),
                "淘汰原因": r.get("elimination_reason",""),
                "進場區": r.get("entry_zone","-"),
                "停損": r.get("stop_loss","-"),
                "1.382": f"{float(r.get('target_1382',0) or 0):.2f}",
                "1.618": f"{float(r.get('target_1618',0) or 0):.2f}",
                "RR": round(float(r.get("rr",0) or 0),2),
                "勝率": round(float(r.get("win_rate",0) or 0),1),
                "模型分數": round(float(r.get("model_score",0) or 0),2),
                "交易分數": round(float(r.get("wave_trade_score", r.get("trade_score", 0)) or 0),2),
                "ATR%": round(float(r.get("atr_pct",0) or 0),2),
                "Kelly%": kelly["position_pct"],
                "建議張數": qty,
                "建議金額": amount,
                "單檔曝險%": single_pct,
                "題材曝險%": theme_pct,
                "產業曝險%": industry_pct,
                "投資組合狀態": portfolio_state,
                "風險備註": "｜".join(note_parts),
            })

        return pd.DataFrame(rows, columns=cols)


class OperationGuideEngine:
    """V3.5 操作版：把資料轉成可執行的日內/波段操作 SOP。"""

    @staticmethod
    def build_playbook(market: dict, trade_top20: pd.DataFrame, today_buy: pd.DataFrame, wait_pullback: pd.DataFrame, attack: pd.DataFrame, defense: pd.DataFrame) -> pd.DataFrame:
        regime = str((market or {}).get("regime", "未定義"))
        memo = str((market or {}).get("memo", ""))
        top20_count = len(trade_top20) if trade_top20 is not None else 0
        buy_count = len(today_buy) if today_buy is not None else 0
        wait_count = len(wait_pullback) if wait_pullback is not None else 0
        attack_count = len(attack) if attack is not None else 0
        defense_count = len(defense) if defense is not None else 0

        regime_rule = {
            "多頭": "先做主攻股，再看預掛單；可接受拉回承接，不追高到 1.382 上方。",
            "震盪": "先挑 RR 與勝率都高的標的；沒有明確進場區就不買。",
            "空頭": "先看防守 ETF；個股只保留極高勝率與低 ATR 的 setup。",
        }.get(regime, "先確認市場狀態，再選股。")

        top_pick = "-"
        if trade_top20 is not None and not trade_top20.empty:
            r = trade_top20.iloc[0]
            top_pick = f"{r['stock_id']} {r['stock_name']}｜{r.get('ui_state','-')}｜進場 {r.get('entry_zone','-')}"

        rows = [
            {"step": 1, "module": "先看市場", "focus": f"市場狀態＝{regime}", "rule": regime_rule, "purpose": "先決定今天偏攻擊、偏等待、還是偏防守", "output": memo or "依市場狀態決定倉位"},
            {"step": 2, "module": "再看輪動", "focus": f"主攻 {attack_count} 檔 / 防守 {defense_count} 檔", "rule": "只做有族群與題材支持的標的；不要逆勢單打獨鬥。", "purpose": "確認今天是做主流股，還是退守 ETF", "output": f"TOP20 候選 {top20_count} 檔"},
            {"step": 3, "module": "看今日可買", "focus": f"今日可買 {buy_count} 檔", "rule": "決策需為 BUY，且支撐 > 0、壓力 > 支撐、RR 夠大。", "purpose": "找出今天真正可以下手的標的", "output": top_pick},
            {"step": 4, "module": "看預掛單", "focus": f"預掛單 {wait_count} 檔", "rule": "未進入進場區前不追價，只能預掛，不可提前亂買。", "purpose": "把看好的股票留在觀察名單，等價格到位", "output": "價格進入進場區才升級為準備買"},
            {"step": 5, "module": "核對六模組", "focus": "K線 / 波浪 / 費波 / 阪田 / 量能 / 指標", "rule": "至少確認波浪位置、1.382/1.618 目標、RR、ATR%、Kelly%。", "purpose": "避免只有題材沒有結構，或只有指標沒有風險控管", "output": "決定可買 / 預掛單 / 觀察 / 不可買"},
            {"step": 6, "module": "下單與倉位", "focus": "下單清單 / 機構交易計畫", "rule": "先看 Kelly% 與建議張數；有風險備註就縮小倉位。", "purpose": "把分析轉成可執行部位", "output": "建議張數、建議金額、單檔曝險%"},
            {"step": 7, "module": "盤後驗證", "focus": "回測視覺化 / Log", "rule": "看勝率、CAGR、MDD、Sharpe，不好的 setup 下次降權。", "purpose": "讓系統愈用愈準，而不是每天重複犯錯", "output": "策略保留 / 降權 / 淘汰"},
        ]
        return pd.DataFrame(rows, columns=["step", "module", "focus", "rule", "purpose", "output"])

    @staticmethod
    def explain_state(ui_state: str) -> str:
        mapping = {
            "可買": "已同時滿足決策、進場區與風險報酬條件，可執行。",
            "準備買": "條件接近完成，通常代表進場價快到位，可小倉位等待。",
            "預掛單": "只列入名單，不追價，等價格回到進場區再動作。",
            "觀察": "可以追蹤，但目前不應出手。",
            "不可買": "不符合 SOP，應直接排除。",
            "淘汰": "已被盤中資金活性規則淘汰，不進觀察清單。",
        }
        return mapping.get(str(ui_state or ""), "依 SOP 判斷，不做主觀硬拗。")


class FinalDecisionEngine:
    @staticmethod
    def _safe_float(v, default: float = 0.0) -> float:
        try:
            return float(v)
        except Exception:
            return float(default)

    @staticmethod
    def _safe_int(v, default: int = 0) -> int:
        try:
            return int(float(v))
        except Exception:
            return int(default)

    @staticmethod
    def _position_bucket(price: float, support: float, resistance: float) -> str:
        if support <= 0 or resistance <= support:
            return "未知"
        width = max(resistance - support, 1e-6)
        pos = (price - support) / width
        if pos <= 0.33:
            return "低位"
        if pos <= 0.66:
            return "中位"
        return "高位"

    @staticmethod
    def _distribution_flag(row: pd.Series) -> str:
        price = FinalDecisionEngine._safe_float(row.get("price", 0))
        resistance = FinalDecisionEngine._safe_float(row.get("resistance", 0))
        rsi = FinalDecisionEngine._safe_float(row.get("rsi", 50))
        liq = FinalDecisionEngine._safe_float(row.get("liquidity_score", 0))
        signal = str(row.get("signal", "") or "")
        if resistance > 0 and price >= resistance * 0.985 and (rsi >= 72 or liq >= 85):
            return "是"
        if signal == "突破強勢" and rsi >= 75:
            return "是"
        return "否"

    @staticmethod
    def _distance_to_entry_pct(price: float, entry_high: float) -> float:
        if entry_high <= 0:
            return 0.0
        return round((price - entry_high) / entry_high * 100.0, 2)

    @staticmethod
    def _operation_grade_from_row(row: pd.Series, decision_bucket: str, execution_ready: int, distribution_flag: str) -> str:
        wave = str(row.get("wave", "") or "")
        count = FinalDecisionEngine._safe_int(row.get("modules_pass_count", 0), 0)
        if distribution_flag == "是" or decision_bucket == "排除" or wave == "第5浪":
            return "C"
        if wave == "第3浪" and count >= 5 and execution_ready == 1:
            return "S"
        if count >= 4 and decision_bucket == "主攻":
            return "A"
        if count >= 3 or decision_bucket == "等待拉回":
            return "B"
        return "C"

    @staticmethod
    def _normalize_light_value(light: str) -> str:
        light = str(light or "").strip()
        alias = {
            "red": "🔴", "orange": "🟠", "yellow": "🟡", "green": "🟢", "blue": "🔵", "neutral": "⚪",
            "light_red": "🔴", "light_orange": "🟠", "light_yellow": "🟡", "light_green": "🟢", "light_blue": "🔵", "light_neutral": "⚪",
            "R": "🔴", "O": "🟠", "Y": "🟡", "G": "🟢", "B": "🔵", "N": "⚪",
            "紅": "🔴", "橘": "🟠", "黃": "🟡", "綠": "🟢", "藍": "🔵", "白": "⚪",
            "○": "⚪",
        }
        return alias.get(light, light if light in ("🔴", "🟠", "🟡", "🟢", "🔵", "⚪") else "⚪")

    @staticmethod
    def _bucket_from_light(light: str, is_etf: bool = False) -> str:
        light = FinalDecisionEngine._normalize_light_value(light)
        if is_etf:
            return "防守"
        if light in ("🔵", "🟢"):
            return "主攻"
        if light in ("🟡", "🟠"):
            return "等待拉回"
        return "排除"

    @staticmethod
    def _grade_from_light(light: str, is_etf: bool = False) -> str:
        light = FinalDecisionEngine._normalize_light_value(light)
        if is_etf:
            return "B"
        if light == "🔵":
            return "S"
        if light == "🟢":
            return "A"
        if light in ("🟡", "🟠"):
            return "B"
        return "C"

    @staticmethod
    def finalize(trade_top20: pd.DataFrame, today_buy: pd.DataFrame, wait_pullback: pd.DataFrame, defense: pd.DataFrame, institutional_plan: pd.DataFrame, theme_summary: pd.DataFrame) -> pd.DataFrame:
        frames = []
        for role, df in (("交易候選", trade_top20), ("今日可買", today_buy), ("等待拉回", wait_pullback), ("防守", defense)):
            if df is not None and not df.empty:
                part = df.copy()
                part["source_role"] = role
                frames.append(part)
        if not frames:
            return pd.DataFrame(columns=["stock_id", "stock_name", "decision_bucket", "decision_reason", "risk_reason", "execution_ready", "empty_reason", "strategy_layer_source"])

        merged = pd.concat(frames, ignore_index=True, sort=False)
        if "stock_id" in merged.columns:
            merged["stock_id"] = merged["stock_id"].astype(str)
            merged = merged.drop_duplicates(subset=["stock_id"], keep="first").reset_index(drop=True)

        theme_weight_map = {}
        if theme_summary is not None and not theme_summary.empty and all(c in theme_summary.columns for c in ["theme", "hot_score"]):
            ts = theme_summary[["theme", "hot_score"]].copy()
            ts["hot_score"] = pd.to_numeric(ts["hot_score"], errors="coerce").fillna(0)
            ts["top_name"] = ""
            max_hot = float(ts["hot_score"].max()) if not ts.empty else 0.0
            ts["theme_weight"] = (ts["hot_score"] / max_hot * 100).round(2) if max_hot > 0 else 0.0
            theme_weight_map = dict(zip(ts["theme"].astype(str), ts["theme_weight"]))
        merged["theme_weight"] = merged.get("theme", pd.Series(dtype=object)).astype(str).map(theme_weight_map).fillna(0.0)

        plan_lookup = {}
        if institutional_plan is not None and not institutional_plan.empty and "代號" in institutional_plan.columns:
            plan_lookup = institutional_plan.set_index(institutional_plan["代號"].astype(str)).to_dict("index")

        today_buy_ids = set(today_buy["stock_id"].astype(str).tolist()) if today_buy is not None and not today_buy.empty and "stock_id" in today_buy.columns else set()
        wait_ids = set(wait_pullback["stock_id"].astype(str).tolist()) if wait_pullback is not None and not wait_pullback.empty and "stock_id" in wait_pullback.columns else set()
        defense_ids = set(defense["stock_id"].astype(str).tolist()) if defense is not None and not defense.empty and "stock_id" in defense.columns else set()

        rows = []
        for _, row in merged.iterrows():
            sid = str(row.get("stock_id", "") or "")
            price = FinalDecisionEngine._safe_float(row.get("price", 0))
            entry_high = FinalDecisionEngine._safe_float(row.get("entry_high", 0))
            support = FinalDecisionEngine._safe_float(row.get("support", 0))
            resistance = FinalDecisionEngine._safe_float(row.get("resistance", 0))
            trade_action = str(row.get("trade_action", row.get("decision", "")) or "").upper()
            ui_state = str(row.get("ui_state", "") or "")
            liquidity_status = str(row.get("liquidity_status", "") or "")
            is_etf = FinalDecisionEngine._safe_int(row.get("is_etf", 0)) == 1
            plan_row = plan_lookup.get(sid, {})
            inst_status = str(plan_row.get("盤中狀態", "") or "")
            suggested_qty = FinalDecisionEngine._safe_float(plan_row.get("建議張數", 0), 0)
            distance = FinalDecisionEngine._distance_to_entry_pct(price, entry_high)
            position_bucket = FinalDecisionEngine._position_bucket(price, support, resistance)
            distribution_flag = FinalDecisionEngine._distribution_flag(row)
            final_trade_decision = str(row.get("final_trade_decision", trade_action) or trade_action).upper()
            modules_pass_count = FinalDecisionEngine._safe_int(row.get("modules_pass_count", 0), 0)
            operation_grade = str(row.get("operation_grade", "") or "")
            risk_reasons = []
            reasons = []
            empty_reason = ""
            strategy_layer_source = str(row.get("source_role", "交易候選") or "交易候選")

            tactical_light = FinalDecisionEngine._normalize_light_value(row.get("tactical_light", "⚪"))
            light_bucket = FinalDecisionEngine._bucket_from_light(tactical_light, is_etf=is_etf)

            if is_etf or sid in defense_ids or str(row.get("bucket", "")) == "防守":
                decision_bucket = "防守"
                reasons.append("ETF/防守池")
                strategy_layer_source = "Defense"
            elif tactical_light in ("🔴", "⚪") or trade_action == "AVOID" or final_trade_decision in ("AVOID", "ELIMINATE") or liquidity_status == "ELIMINATE":
                decision_bucket = "排除"
                reasons.append(f"燈號={tactical_light} 不可列為主攻")
                strategy_layer_source = "Eliminated"
            elif tactical_light in ("🟡", "🟠"):
                decision_bucket = "等待拉回"
                reasons.append(f"燈號={tactical_light}，僅保留等待拉回")
                strategy_layer_source = "Wait_Pullback"
            elif tactical_light in ("🔵", "🟢") and ((sid in today_buy_ids) or (trade_action in ("BUY", "WEAK BUY") and modules_pass_count >= 4 and position_bucket != "高位")):
                decision_bucket = "主攻"
                reasons.append(f"燈號={tactical_light}，策略層主攻成立")
                strategy_layer_source = "Final主攻"
            elif light_bucket == "等待拉回" or sid in wait_ids or distance > 3.5 or trade_action == "HOLD" or ui_state in ("預掛單", "觀察"):
                decision_bucket = "等待拉回"
                reasons.append("方向正確但未到執行條件")
                strategy_layer_source = "Wait_Pullback"
            else:
                decision_bucket = "排除"
                reasons.append("不符合唯一決策規則")
                strategy_layer_source = "Eliminated"

            if distribution_flag == "是":
                risk_reasons.append("高位/出貨警示")
                if decision_bucket == "主攻":
                    decision_bucket = "等待拉回"
                    reasons.append("高位/出貨警示降級")
                    strategy_layer_source = "Wait_Pullback"
            if position_bucket == "高位":
                risk_reasons.append("位置偏高")
            rsi_val = FinalDecisionEngine._safe_float(row.get("rsi", row.get("rsi14", 50)), 50)
            if rsi_val >= 72:
                risk_reasons.append("RSI偏高")
            if liquidity_status == "WATCH":
                risk_reasons.append("盤中狀態WATCH")
            if liquidity_status == "ELIMINATE":
                risk_reasons.append("盤中結構淘汰")

            execution_ready = int(decision_bucket == "主攻" and inst_status == "PASS" and suggested_qty > 0 and trade_action in ("BUY", "WEAK BUY"))
            if decision_bucket == "主攻" and not execution_ready:
                fail_reasons = []
                if inst_status != "PASS":
                    fail_reasons.append(f"盤中狀態={inst_status or '未評級'}")
                if suggested_qty <= 0:
                    fail_reasons.append("建議張數=0")
                if trade_action not in ("BUY", "WEAK BUY"):
                    fail_reasons.append(f"交易動作={trade_action or '-'}")
                empty_reason = " / ".join(fail_reasons)
                if empty_reason:
                    reasons.append(f"暫不可執行：{empty_reason}")

            reason_tail = []
            if str(row.get("signal", "")):
                reason_tail.append(str(row.get("signal", "")))
            if str(row.get("wave", "")):
                reason_tail.append(str(row.get("wave", "")))
            if sid in today_buy_ids:
                reason_tail.append("今日可買")
            elif sid in wait_ids:
                reason_tail.append("等待拉回")
            if inst_status:
                reason_tail.append(f"盤中={inst_status}")
            if suggested_qty > 0:
                reason_tail.append(f"建議張數={suggested_qty:g}")

            out = row.to_dict()
            operation_grade = FinalDecisionEngine._grade_from_light(tactical_light, is_etf=is_etf)
            out.update({
                "distance_to_entry_pct": distance,
                "position_bucket": position_bucket,
                "distribution_flag": distribution_flag,
                "execution_ready": execution_ready,
                "decision_bucket": decision_bucket,
                "decision_reason": "｜".join(reasons + reason_tail),
                "risk_reason": "｜".join(dict.fromkeys([r for r in risk_reasons if r])) if risk_reasons else "",
                "empty_reason": empty_reason,
                "strategy_layer_source": strategy_layer_source,
                "theme_weight": round(FinalDecisionEngine._safe_float(out.get("theme_weight", 0)), 2),
                "inst_status": inst_status,
                "suggested_qty": suggested_qty,
                "operation_grade": operation_grade,
            })
            rows.append(out)

        final_df = pd.DataFrame(rows)
        if not final_df.empty:
            priority_map = {"主攻": 0, "等待拉回": 1, "防守": 2, "排除": 3}
            final_df["decision_priority"] = final_df["decision_bucket"].map(priority_map).fillna(9)
            final_df = final_df.sort_values(["decision_priority", "execution_ready", "modules_pass_count", "theme_weight", "liquidity_score", "model_score", "trade_score", "win_rate"], ascending=[True, False, False, False, False, False, False, False]).reset_index(drop=True)
        return final_df


class AppUI:
    def __init__(self, root, db: DBManager):
        self.root = root
        self.db = db
        self.data_engine = DataEngine(db)
        self.rank_engine = RankingEngine(db)
        self.master_trading_engine = MasterTradingEngine(db)
        self.backtest_engine = BacktestEngine(db)
        self.portfolio_engine = PortfolioEngine(db)
        self.realtime_quote_engine = RealtimeQuoteEngine()
        self.tactical_score_engine = TacticalScoreEngine()
        self.single_stock_narrative_engine = SingleStockNarrativeEngine()
        self.ui_tactical_presenter = UITacticalPresenter()
        self.last_top20_df = pd.DataFrame()
        self.last_mainstream_top20_df = pd.DataFrame()
        self.last_breakout_top20_df = pd.DataFrame()
        self.last_top5_df = pd.DataFrame()
        self.last_theme_summary_df = pd.DataFrame()
        self.last_attack_df = pd.DataFrame()
        self.last_watch_df = pd.DataFrame()
        self.last_defense_df = pd.DataFrame()
        self.last_order_list_df = pd.DataFrame()
        self.last_institutional_plan_df = pd.DataFrame()
        self.last_execution_pool_df = pd.DataFrame()
        self.last_final_decision_df = pd.DataFrame()
        self.last_today_buy_df = pd.DataFrame()
        self.last_wait_df = pd.DataFrame()
        self.last_operation_sop_df = pd.DataFrame()
        self.current_chart_path = None
        self.plan_cache = {}
        self.backtest_cache = {}
        self.selection_job_token = 0
        self.selection_source = ""
        self.selector_syncing = False
        self.last_selected_stock_id = None
        self.last_selected_source = ""
        self.last_selected_ts = 0.0
        self.worker = None
        self.cancel_event = threading.Event()
        self.current_job = None
        self.history_batch_size = 25
        self.history_sleep_sec = 0.6
        self.last_job_summary = {}
        self.quote_cache = {}
        self.last_filter_signature = None

        self.root.title(APP_NAME)
        self._configure_startup_window()
        log_info(f"應用程式啟動｜DB={DB_PATH}｜LOG={LOG_PATH}")

        self.market_var = tk.StringVar(value="全部")
        self.multi_window_var = tk.BooleanVar(value=True)
        self.top20_window = None
        self.chart_window = None
        self.plan_window = None
        self.win_top20_tree = None
        self.win_plan_text = None
        self.chart_fig = None
        self.chart_canvas = None
        self.window_current_stock_id = None
        self.chart_updating = False
        self.pending_stock_id = None
        self.chart_update_job = None
        self.pending_chart_image = None
        self.chart_image_job = None
        self.selection_chart_pending_token = 0
        self.backtest_selection_token = 0
        self.industry_var = tk.StringVar(value="全部")
        self.theme_var = tk.StringVar(value="全部")
        self.search_var = tk.StringVar(value="")

        self._build_ui()
        set_classification_log_callback(lambda message, level="INFO": self.root.after(0, lambda: self.append_log(message, level)))
        self.root.after(80, self.refresh_filters)
        self.root.after(120, self.show_welcome_message)
        self.root.after(180, self._apply_initial_layout)
        self.root.after(260, self.bootstrap_after_startup)
        self.root.after(600, self._apply_initial_layout)
        self.set_status(f"啟動畫面已建立｜PACKED={PACKED_DATA_DIR.name} | EXTERNAL={EXTERNAL_DATA_DIR.name}")

    def _configure_startup_window(self):
        """啟動時自動貼齊可視區域，避免主視窗超出螢幕範圍。"""
        try:
            self.root.update_idletasks()
            if sys.platform.startswith("win"):
                try:
                    self.root.state("zoomed")
                    return
                except Exception:
                    pass

            sw = int(self.root.winfo_screenwidth() or 1600)
            sh = int(self.root.winfo_screenheight() or 900)
            width = max(1280, int(sw * 0.96))
            height = max(780, int(sh * 0.90))
            width = min(width, sw - 20) if sw > 40 else width
            height = min(height, sh - 80) if sh > 120 else height
            x = max((sw - width) // 2, 0)
            y = max((sh - height) // 2, 0)
            self.root.geometry(f"{width}x{height}+{x}+{y}")
        except Exception:
            self.root.geometry("1500x860")

    def bootstrap_after_startup(self):
        if getattr(self, "_bootstrap_started", False):
            return
        self._bootstrap_started = True

        def worker():
            try:
                self.ui_call(self.set_status, "啟動中：載入主檔 / 排行...")
                master = self.db.get_master()
                init_message = f"股票主檔已載入，共 {len(master)} 檔" if master is not None and not master.empty else "股票主檔尚未建立"
                if master is None or master.empty:
                    universe = build_full_market_universe()
                    if universe is not None and not universe.empty:
                        self.db.import_master_df(universe)
                        master = self.db.get_master()
                        init_message = f"已於背景建立全市場股票主檔，共 {len(master)} 檔"
                    else:
                        csv_path = resolve_master_csv()
                        self.db.import_master_csv(csv_path)
                        master = self.db.get_master()
                        init_message = f"已於背景改用本地主檔，共 {len(master)} 檔 | {csv_path}"

                rank_count = self.db.get_ranking_rows_count()
                if rank_count == 0 and self.db.get_total_price_rows() > 0:
                    self.ui_call(self.set_status, "啟動中：背景重建排行...")
                    rank_count = self.rank_engine.rebuild(
                        log_cb=lambda msg: self.ui_call(self.append_log, msg),
                        cancel_cb=lambda: self.cancel_event.is_set(),
                    )
                    if rank_count > 0:
                        init_message += f"｜已於背景重建排行 {rank_count} 檔"
                    else:
                        init_message += "｜已有歷史資料，但目前不足以形成排行"

                self.ui_call(self.refresh_filters)
                self.ui_call(self.refresh_all_tables)
                self.ui_call(self.refresh_classification_summary_ui)
                self.ui_call(self.show_welcome_message)
                self.ui_call(self.set_status, init_message)
            except Exception as e:
                log_exception("background bootstrap failed", e)
                self.ui_call(self.append_log, f"背景初始化失敗：{e}", "ERROR")
                self.ui_call(self.set_status, f"背景初始化失敗：{e}")

        self._run_in_thread(worker, "startup_bootstrap")

    def _apply_initial_layout(self):
        """啟動後固定三區比例，減少人工手動調整。"""
        try:
            self.root.update_idletasks()
            total_w = max(int(self.root.winfo_width() or 0), 1200)
            total_h = max(int(self.root.winfo_height() or 0), 780)

            if getattr(self, "main_paned", None) is not None:
                left_w = max(760, int(total_w * 0.64))
                try:
                    self.main_paned.sashpos(0, left_w)
                except Exception:
                    pass

            if getattr(self, "right_paned", None) is not None:
                usable_h = max(total_h - 180, 520)
                upper_h = max(260, int(usable_h * 0.56))
                try:
                    self.right_paned.sashpos(0, upper_h)
                except Exception:
                    pass
        except Exception as exc:
            log_warning(f"套用啟動版型失敗：{exc}")

    def show_welcome_message(self):
        set_classification_log_callback(lambda message, level="INFO": self.root.after(0, lambda: self.append_log(message, level)))
        last_date = self.db.get_last_price_date() or "尚未建立"
        ranking_count = self.db.get_ranking_rows_count()
        price_rows = self.db.get_total_price_rows()
        cls_status = get_classification_status()
        cls_file = cls_status.get("file", "未載入")
        cls_rows = int(cls_status.get("loaded_rows", 0) or 0)
        cls_note = str(cls_status.get("load_note", "") or cls_status.get("note", ""))
        if cls_status.get("loaded") and cls_rows > 0:
            cls_mark = f"✔ 正常（已載入 {cls_rows} 筆）"
        elif cls_status.get("exists"):
            cls_mark = "⚠ 降級模式（檔案存在但未成功載入）"
        else:
            cls_mark = "❌ 缺失"
        cls_v2 = get_classification_v2_summary()
        coverage_text = "-"
        coverage_detail = ""
        if cls_v2:
            coverage_text = f"{float(cls_v2.get('coverage_pct', 0) or 0):.2f}%"
            coverage_detail = f"官方 {int(cls_v2.get('official', 0) or 0)}｜手動 {int(cls_v2.get('manual', 0) or 0)}｜規則 {int(cls_v2.get('rule_engine', 0) or 0)}｜AI {int(cls_v2.get('ai_infer', 0) or 0)}｜未分類 {int(cls_v2.get('unclassified', 0) or 0)}"
        lines = [
            "《GTC AI Trading System v9.2 FINAL-RELEASE》",
            "",
            f"主檔狀態：{len(self.db.get_master())} 檔",
            f"歷史資料：{price_rows} 筆｜最後交易日：{last_date}",
            f"最新排行筆數：{ranking_count}",
            f"分類檔狀態：{cls_mark}｜{cls_file}",
            f"分類檔備註：{cls_note or '-'}",
            f"分類覆蓋率：{coverage_text}",
            f"分類V2統計：{coverage_detail or '-'}",
            "",
            "建議操作順序：",
            "1. 初始化全市場（第一次或要重整主檔時）",
            "2. 建立完整歷史（第一次建庫）",
            "3. 每日增量更新",
            "4. 重建排行",
            "5. AI選股TOP20",
            "6. 採用 v9.2 FINAL-RELEASE：唯一核心策略引擎 / 波浪費波模型 / Kelly+ATR / Equity Curve",
            "7. V3.5操作版重點：先看市場，再看輪動，再看今日可買 / 預掛單，最後才下單",
            f"8. 圖表字型：{SELECTED_PLOT_FONT}",
        ]
        self.detail.delete("1.0", tk.END)
        self.detail.insert("1.0", "\n".join(lines))

    def refresh_classification_summary_ui(self):
        try:
            _ = get_classification_status()
            _ = get_classification_v2_summary()
        except Exception:
            pass
        try:
            self.show_welcome_message()
        except Exception:
            pass
        try:
            self.root.update_idletasks()
        except Exception:
            pass


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
            "V3.5操作說明",
            "v9策略回測",
            "初始化全市場",
            "建立完整歷史（一次）",
            "續跑建庫",
            "每日增量更新",
            "重建排行",
            "更新分類檔",
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
        self.download_target_cb["values"] = ["TOP20", "TOP5", "今日可買", "等待拉回", "預掛單", "主攻", "次強", "防守", "唯一決策", "下單清單", "機構交易計畫", "操作SOP", "排行", "類股", "題材", "未分類清單", "分類V2摘要"]
        self.download_target_cb.pack(side="left", padx=4)
        self.btn_export_data = ttk.Button(row2, text="下載資料", command=self.export_selected_data)
        self.btn_export_data.pack(side="left", padx=(4, 12))

        self.btn_export_excel = ttk.Button(row2, text="匯出分析Excel", command=self.export_analysis_excel)
        self.btn_export_excel.pack(side="left", padx=4)
        self.btn_open_chart = ttk.Button(row2, text="開啟圖表", command=self.open_current_chart)
        self.btn_open_chart.pack(side="left", padx=4)
        self.multi_window_chk = ttk.Checkbutton(row2, text="左主區＋右上分析＋右下圖表/Log", variable=self.multi_window_var)
        self.multi_window_chk.state(["selected", "disabled"])
        self.multi_window_chk.pack(side="left", padx=(8, 2))
        self.btn_open_3wins = ttk.Button(row2, text="同步右側面板", command=self.open_three_windows)
        self.btn_open_3wins.pack(side="left", padx=4)

        self.progress_var = tk.DoubleVar(value=0)
        self.progress = ttk.Progressbar(row2, variable=self.progress_var, maximum=100, length=180, mode="determinate")
        self.progress.pack(side="left", padx=(12, 6))
        self.progress_text_var = tk.StringVar(value="0% | 0/0 | 成功 0 | 失敗 0")
        self.progress_text_label = ttk.Label(row2, textvariable=self.progress_text_var, width=44)
        self.progress_text_label.pack(side="left", padx=4)

        self.main_paned = ttk.Panedwindow(self.root, orient="horizontal")
        self.main_paned.pack(fill="both", expand=True, padx=8, pady=8)

        self.left_notebook = ttk.Notebook(self.main_paned)
        right = ttk.Frame(self.main_paned, padding=8)
        self.main_paned.add(self.left_notebook, weight=5)
        self.main_paned.add(right, weight=2)

        self.tab_dashboard = ttk.Frame(self.left_notebook)
        self.tab_sop = ttk.Frame(self.left_notebook)
        self.tab_rotation = ttk.Frame(self.left_notebook)
        self.tab_rank = ttk.Frame(self.left_notebook)
        self.tab_sector = ttk.Frame(self.left_notebook)
        self.tab_theme = ttk.Frame(self.left_notebook)
        self.tab_top20 = ttk.Frame(self.left_notebook)
        self.tab_top5 = ttk.Frame(self.left_notebook)
        self.tab_order = ttk.Frame(self.left_notebook)
        self.tab_inst = ttk.Frame(self.left_notebook)
        self.tab_backtest = ttk.Frame(self.left_notebook)
        self.left_notebook.add(self.tab_dashboard, text="交易儀表板")
        self.left_notebook.add(self.tab_sop, text="V3.5操作SOP")
        self.left_notebook.add(self.tab_rotation, text="產業輪動")
        self.left_notebook.add(self.tab_rank, text="排行榜")
        self.left_notebook.add(self.tab_sector, text="類股熱度")
        self.left_notebook.add(self.tab_theme, text="題材輪動")
        self.left_notebook.add(self.tab_top20, text="雙引擎TOP20")
        self.left_notebook.add(self.tab_top5, text="AI選股TOP5")
        self.left_notebook.add(self.tab_order, text="下單清單")
        self.left_notebook.add(self.tab_inst, text="機構交易計畫")
        self.left_notebook.add(self.tab_backtest, text="回測視覺化")

        self.dashboard_tree = self._make_tree(self.tab_dashboard, ("metric", "value", "desc"), {
            "metric": "指標", "value": "數值", "desc": "說明"
        })

        self.sop_tree = self._make_tree(self.tab_sop, ("step", "module", "focus", "rule", "purpose", "output"), {
            "step": "步驟", "module": "模組", "focus": "先看什麼", "rule": "判斷規則", "purpose": "用途", "output": "輸出"
        })

        self.rotation_tree = self._make_tree(self.tab_rotation, ("industry", "count", "avg_total", "avg_ai", "trend_count", "hot_score", "rotation"), {
            "industry": "產業", "count": "檔數", "avg_total": "平均總分", "avg_ai": "平均AI分", "trend_count": "強勢數", "hot_score": "輪動分", "rotation": "輪動狀態"
        })

        self.rank_tree = self._make_tree(self.tab_rank, ("rank", "id", "name", "price", "industry", "theme", "total", "ai", "signal", "action"), {
            "rank": "排名", "id": "代號", "name": "名稱", "price": "股價", "industry": "產業", "theme": "題材", "total": "總分", "ai": "AI分", "signal": "訊號", "action": "建議"
        })
        self.rank_tree.bind("<<TreeviewSelect>>", self.on_select_stock)

        self.sector_tree = self._make_tree(self.tab_sector, ("industry", "count", "avg_total", "avg_ai", "top_name"), {
            "industry": "產業", "count": "檔數", "avg_total": "平均總分", "avg_ai": "平均AI分", "top_name": "代表股"
        })

        self.theme_tree = self._make_tree(self.tab_theme, ("theme", "count", "avg_total", "avg_ai", "top_name"), {
            "theme": "題材", "count": "檔數", "avg_total": "平均總分", "avg_ai": "平均AI分", "top_name": "代表股"
        })

        self.top20_tree = self._make_tree(self.tab_top20, ("rank", "id", "name", "light", "engine", "price", "bucket", "grade", "ui_action", "orderbook", "intra", "liquidity", "liq_score", "entry", "stop", "target1382", "target1618", "rr", "win_rate", "elim_reason"), {
            "rank": "排序", "id": "代號", "name": "名稱", "light": "燈態", "engine": "來源", "price": "股價", "bucket": "分類", "grade": "操作權", "ui_action": "狀態", "orderbook": "五檔偏向", "intra": "盤中分", "liquidity": "盤中狀態", "liq_score": "活性分", "entry": "進場區", "stop": "停損", "target1382": "1.382", "target1618": "1.618", "rr": "RR", "win_rate": "勝率%", "elim_reason": "淘汰原因"
        })
        self.top20_tree.bind("<<TreeviewSelect>>", self.on_select_top20)

        self.top5_tree = self._make_tree(self.tab_top5, ("rank", "id", "light", "name", "price", "state", "orderbook", "intra", "liquidity", "liq_score", "entry", "stop", "target1382", "rr", "win_rate", "backtest", "cagr", "mdd"), {
            "rank": "排序", "id": "代號", "light": "燈號", "name": "名稱", "price": "股價", "state": "狀態", "orderbook": "五檔偏向", "intra": "盤中分", "liquidity": "盤中狀態", "liq_score": "活性分", "entry": "進場區", "stop": "停損", "target1382": "1.382", "rr": "RR", "win_rate": "勝率%", "backtest": "回測勝率%", "cagr": "CAGR%", "mdd": "MDD%"
        })
        self.top5_tree.bind("<<TreeviewSelect>>", self.on_select_top5)

        self.order_tree = self._make_tree(self.tab_order, ("priority", "id", "name", "price", "bucket", "action", "liquidity", "liq_score", "entry", "stop", "target1382", "target1618", "rr", "win_rate", "atr_pct", "kelly_pct", "qty", "amount", "single_pct", "portfolio_state", "risk_note", "reason"), {
            "priority": "優先級", "id": "代號", "name": "名稱", "price": "股價", "bucket": "分類", "action": "狀態", "liquidity": "盤中狀態", "liq_score": "活性分", "entry": "進場區", "stop": "停損", "target1382": "1.382", "target1618": "1.618", "rr": "RR", "win_rate": "勝率%", "atr_pct": "ATR%", "kelly_pct": "Kelly%", "qty": "建議張數", "amount": "建議金額", "single_pct": "單檔曝險%", "portfolio_state": "組合狀態", "risk_note": "風險備註", "reason": "決策/空表說明"
        })
        self.order_tree.bind("<<TreeviewSelect>>", self.on_select_order)

        self.inst_tree = self._make_tree(self.tab_inst, ("priority", "id", "name", "price", "market", "industry", "theme", "bucket", "action", "liquidity", "liq_score", "entry", "stop", "rr", "win_rate", "model_score", "trade_score", "atr_pct", "kelly_pct", "qty", "amount", "single_pct", "theme_pct", "industry_pct", "portfolio_state"), {
            "priority": "優先級", "id": "代號", "name": "名稱", "price": "股價", "market": "市場", "industry": "產業", "theme": "題材", "bucket": "分類", "action": "狀態", "liquidity": "盤中狀態", "liq_score": "活性分", "entry": "進場區", "stop": "停損", "rr": "RR", "win_rate": "勝率%", "model_score": "模型分數", "trade_score": "交易分數", "atr_pct": "ATR%", "kelly_pct": "Kelly%", "qty": "建議張數", "amount": "建議金額", "single_pct": "單檔曝險%", "theme_pct": "題材曝險%", "industry_pct": "產業曝險%", "portfolio_state": "組合狀態"
        })
        self.inst_tree.bind("<<TreeviewSelect>>", self.on_select_institutional)

        self.backtest_tree = self._make_tree(self.tab_backtest, ("rank", "id", "name", "price", "win", "avg_ret", "cagr", "mdd", "sharpe", "samples"), {
            "rank": "排序", "id": "代號", "name": "名稱", "price": "股價", "win": "勝率%", "avg_ret": "平均報酬%", "cagr": "CAGR%", "mdd": "MDD%", "sharpe": "Sharpe", "samples": "樣本數"
        })
        self.backtest_tree.bind("<<TreeviewSelect>>", self.on_select_backtest)
        self.left_notebook.bind("<<NotebookTabChanged>>", self.on_left_tab_changed)

        self.right_paned = ttk.Panedwindow(right, orient="vertical")
        self.right_paned.pack(fill="both", expand=True)

        upper = ttk.LabelFrame(self.right_paned, text="個股分析 / 交易計畫", padding=6)
        right_lower = ttk.LabelFrame(self.right_paned, text="圖表 / Log", padding=6)
        self.right_paned.add(upper, weight=3)
        self.right_paned.add(right_lower, weight=2)

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

        self.win_plan_text = None
        self.win_top20_tree = None

        self.right_lower_notebook = ttk.Notebook(right_lower)
        self.right_lower_notebook.pack(fill="both", expand=True)

        self.chart_tab = ttk.Frame(self.right_lower_notebook)
        self.log_tab = ttk.Frame(self.right_lower_notebook)
        self.right_lower_notebook.add(self.chart_tab, text="圖表")
        self.right_lower_notebook.add(self.log_tab, text="Log")

        chart_wrap = ttk.Frame(self.chart_tab)
        chart_wrap.pack(fill="both", expand=True)
        self.chart_fig = plt.Figure(figsize=(8.6, 4.8), dpi=100)
        self.chart_canvas = FigureCanvasTkAgg(self.chart_fig, master=chart_wrap)
        self.chart_canvas.get_tk_widget().pack(fill="both", expand=True)

        log_body = ttk.Frame(self.log_tab)
        log_body.pack(fill="both", expand=True)
        self.log_text = tk.Text(log_body, wrap="none", font=("Consolas", 10), height=12)
        self.log_vsb = ttk.Scrollbar(log_body, orient="vertical", command=self.log_text.yview)
        self.log_hsb = ttk.Scrollbar(log_body, orient="horizontal", command=self.log_text.xview)
        self.log_text.configure(yscrollcommand=self.log_vsb.set, xscrollcommand=self.log_hsb.set)
        self.log_text.grid(row=0, column=0, sticky="nsew")
        self.log_vsb.grid(row=0, column=1, sticky="ns")
        self.log_hsb.grid(row=1, column=0, sticky="ew")
        log_body.rowconfigure(0, weight=1)
        log_body.columnconfigure(0, weight=1)

    def _make_tree(self, parent, cols, headers):
        frame = ttk.Frame(parent)
        frame.pack(fill="both", expand=True)
        tree = ttk.Treeview(frame, columns=cols, show="headings", height=28)
        try:
            tree.tag_configure("light_red", foreground="#9f1239", background="#fff1f2")
            tree.tag_configure("light_orange", foreground="#9a3412", background="#fff7ed")
            tree.tag_configure("light_yellow", foreground="#854d0e", background="#fefce8")
            tree.tag_configure("light_green", foreground="#166534", background="#f0fdf4")
            tree.tag_configure("light_blue", foreground="#1d4ed8", background="#eff6ff")
            tree.tag_configure("light_neutral", foreground="#374151", background="#f8fafc")
            tree.tag_configure("light_disabled", foreground="#6b7280", background="#f3f4f6")
        except Exception:
            pass
        narrow_cols = {"rank", "count", "avg_total", "avg_ai", "id", "total", "ai", "price", "intra", "liq_score", "rr", "win_rate", "atr_pct", "kelly_pct", "qty", "single_pct", "theme_pct", "industry_pct", "backtest", "cagr", "mdd", "win", "avg_ret", "samples", "light"}
        medium_cols = {"state", "action", "signal", "liquidity", "market", "bucket", "ui_action", "priority", "engine", "grade"}
        wide_cols = {"name", "industry", "theme", "entry", "stop", "target1382", "target1618", "portfolio_state", "risk_note", "elim_reason", "reason"}
        for c in cols:
            tree.heading(c, text=headers[c])
            width = 140
            if c in narrow_cols:
                width = 90
            elif c in medium_cols:
                width = 110
            elif c in wide_cols:
                width = 130
            tree.column(c, width=width, anchor="center")
        vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)
        return tree

    def _normalize_light_value(self, light):
        light = str(light or "").strip()
        alias = {
            "red": "🔴", "orange": "🟠", "yellow": "🟡", "green": "🟢", "blue": "🔵", "neutral": "⚪",
            "light_red": "🔴", "light_orange": "🟠", "light_yellow": "🟡", "light_green": "🟢", "light_blue": "🔵", "light_neutral": "⚪",
            "R": "🔴", "O": "🟠", "Y": "🟡", "G": "🟢", "B": "🔵", "N": "⚪",
            "紅": "🔴", "橘": "🟠", "黃": "🟡", "綠": "🟢", "藍": "🔵", "白": "⚪",
            "○": "⚪",
        }
        return alias.get(light, light if light in ("🔴", "🟠", "🟡", "🟢", "🔵", "⚪") else "⚪")

    def _get_display_light(self, row):
        row = dict(row or {})
        light = self._normalize_light_value(row.get("tactical_light", ""))
        if light and light != "⚪":
            return light
        signal = str(row.get("signal", "") or "").strip()
        if signal:
            return self._normalize_light_value(UITacticalPresenter.derive_signal_light(
                signal,
                score=float(row.get("score", row.get("selection_score", 0)) or 0),
                change_pct=float(row.get("change_pct", 0) or 0),
                intraday_score=float(row.get("intraday_score", 0) or 0),
            ))
        return "⚪"

    def _display_light_symbol(self, light):
        light = self._normalize_light_value(light)
        mapping = {
            "🔴": "紅",
            "🟠": "橘",
            "🟡": "黃",
            "🟢": "綠",
            "🔵": "藍",
            "⚪": "白",
        }
        return mapping.get(light, "白")

    def _derive_bucket_from_light(self, light, is_etf: bool = False):
        light = self._normalize_light_value(light)
        if is_etf:
            return "防守"
        if light in ("🔵", "🟢"):
            return "主攻"
        if light in ("🟡", "🟠"):
            return "觀察"
        return "排除"

    def _derive_operation_grade_from_light(self, light, is_etf: bool = False):
        light = self._normalize_light_value(light)
        if is_etf:
            return "B"
        if light == "🔵":
            return "S"
        if light == "🟢":
            return "A"
        if light in ("🟡", "🟠"):
            return "B"
        return "C"

    def _resolve_light_tag(self, light=""):
        """燈號顏色唯一來源：只接受 tactical_light / signal fallback 的最終燈號。"""
        light = self._normalize_light_value(light)
        mapping = {
            "🔴": "light_red",
            "🟠": "light_orange",
            "🟡": "light_yellow",
            "🟢": "light_green",
            "🔵": "light_blue",
            "⚪": "light_neutral",
        }
        return mapping.get(light, "light_neutral")

    def _insert_colored_row(self, tree, values, light=""):
        tag = self._resolve_light_tag(light=light)
        return tree.insert("", "end", values=values, tags=(tag,))

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

    def _fmt_price(self, value) -> str:
        try:
            return f"{float(value):.2f}"
        except Exception:
            return "-"

    def _get_stock_display_price(self, stock_id: str, row: dict | pd.Series | None = None) -> str:
        try:
            if row is not None:
                for key in ("price", "close", "last_price", "現價"):
                    if hasattr(row, "get"):
                        v = row.get(key, None)
                        if v not in (None, "", "-", "--"):
                            return self._fmt_price(v)
        except Exception:
            pass
        try:
            hist = self.db.get_price_history(str(stock_id))
            if hist is not None and not hist.empty:
                return self._fmt_price(hist.iloc[-1].get("close", 0))
        except Exception:
            pass
        return "-"

    def build_enriched_ranking_dataframe(self, df: pd.DataFrame, scope: str = "ui", max_rows: int | None = None) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame() if df is None else df.copy()
        x = df.copy()
        if max_rows is not None and max_rows > 0:
            head = x.head(max_rows).copy()
            tail = x.iloc[max_rows:].copy() if len(x) > max_rows else pd.DataFrame(columns=x.columns)
        else:
            head = x
            tail = pd.DataFrame(columns=x.columns)
        prices = []
        for _, row in head.iterrows():
            sid = str(row.get("stock_id", "") or "")
            prices.append(self._get_stock_display_price(sid, row))
        head["display_price"] = prices
        if not tail.empty:
            tail["display_price"] = tail.apply(lambda r: self._get_stock_display_price(str(r.get("stock_id", "") or ""), r), axis=1)
        out = pd.concat([head, tail], ignore_index=True) if not tail.empty else head
        if "現價" not in out.columns:
            out["現價"] = out["display_price"]
        return out

    def append_log(self, text, level: str = "INFO"):
        ts = datetime.now().strftime("%H:%M:%S")
        level_upper = str(level or "INFO").upper()
        msg = f"[{ts}] [{level_upper}] {text}"
        try:
            if level_upper == "ERROR":
                log_error(text)
            elif level_upper == "WARNING":
                log_warning(text)
            else:
                log_info(text)
        except Exception:
            pass
        self.log_text.insert(tk.END, msg + "\n")
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

    def update_classification_book(self):
        set_classification_log_callback(lambda message, level="INFO": self.ui_call(self.append_log, message, level))
        def worker():
            self.ui_call(self.start_task, "更新分類檔", 4)
            self.ui_call(self.update_task, "更新分類檔", 1, 4, item="檢查本機快取")
            current = ensure_classification_book(force_refresh=False, log_cb=lambda m: self.ui_call(self.append_log, m))
            if current is not None:
                self.ui_call(self.append_log, f"目前官方分類來源：{current}")
            self.ui_call(self.update_task, "更新分類檔", 2, 4, item="下載最新官方分類來源")
            refreshed = ensure_classification_book(force_refresh=True, log_cb=lambda m: self.ui_call(self.append_log, m))
            if refreshed is None:
                raise RuntimeError("官方分類來源下載失敗，且本機沒有可用快取。")
            self.ui_call(self.update_task, "更新分類檔", 3, 4, item="驗證官方分類來源")
            official = load_official_classification_book()
            if official is None or official.empty:
                raise RuntimeError(f"分類來源存在，但無法成功讀取：{refreshed}")
            self.ui_call(self.update_task, "更新分類檔", 4, 4, success=1, item=Path(refreshed).name)
            self.ui_call(self.refresh_classification_summary_ui)
            status = get_classification_status()
            stale_text = "是" if status.get("is_stale") else "否"
            self.ui_call(self.finish_task, "更新分類檔", f"分類來源更新完成：{Path(refreshed).name}｜共 {len(official)} 筆")
            self.ui_call(self.append_log, f"分類來源更新完成：{refreshed}｜可辨識 {len(official)} 筆｜過期={stale_text}")
            self.ui_call(messagebox.showinfo, "完成", f"分類來源更新完成：\n{refreshed}\n\n可辨識筆數：{len(official)}\n是否過期：{stale_text}\n\n下一步請執行「初始化全市場」以重建主檔分類。\n\n現在官方產業優先採用 MOPS CSV，題材/子題材則由手動映射 + 規則引擎 + AI 補值。")
        self._run_in_thread(worker, "update_classification_book")

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
            self.btn_export_excel, self.btn_open_chart, self.btn_open_3wins
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
            "更新分類檔": self.update_classification_book,
            "AI選股TOP20": self.show_top20,
            "AI選股TOP5": self.show_top5,
            "V3.5操作說明": self.show_operation_guide,
            "v9策略回測": self.show_strategy_backtest,
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
            self.ui_call(self.append_log, f"背景作業啟動：{name}")
            log_info(f"背景作業啟動：{name}")
            try:
                target()
                self.ui_call(self.append_log, f"背景作業完成：{name}")
            except Exception as exc:
                log_exception(f"背景作業失敗：{name}", exc)
                self.ui_call(self.append_log, f"背景作業失敗：{name}｜{exc}", "ERROR")
                self.ui_call(messagebox.showerror, "背景作業錯誤", f"{name} 執行失敗：\n{exc}")
            finally:
                self.current_job = None
                self.ui_call(self.set_busy, False)

        self.worker = threading.Thread(target=runner, name=name, daemon=True)
        self.worker.start()

    def open_current_chart(self):
        if self.current_chart_path is None or not Path(self.current_chart_path).exists():
            return messagebox.showwarning("提醒", "目前沒有可開啟的圖表，請先點選股票。")
        open_path(Path(self.current_chart_path))

    def _should_ignore_select_event(self, stock_id: str, source: str) -> bool:
        sid = str(stock_id or "").strip()
        src = str(source or "").strip()
        if not sid:
            return True
        if getattr(self, "selector_syncing", False):
            log_info(f"忽略程式性選取事件：{sid}｜來源={src}")
            return True
        now = time.time()
        last_sid = str(getattr(self, "last_selected_stock_id", "") or "")
        last_src = str(getattr(self, "last_selected_source", "") or "")
        last_ts = float(getattr(self, "last_selected_ts", 0.0) or 0.0)
        if sid == last_sid and src != last_src and (now - last_ts) <= 0.8:
            log_info(f"忽略短時間重複點股事件：{sid}｜來源={src}｜前次來源={last_src}")
            return True
        self.last_selected_stock_id = sid
        self.last_selected_source = src
        self.last_selected_ts = now
        return False

    def _resolve_stock_id_from_tree_values(self, values, source: str = "") -> str:
        vals = list(values or [])
        src = str(source or "").strip()
        if not vals:
            return ""
        index_candidates = {
            "排行榜": [1],
            "AI交易TOP20": [1, 0, 4],
            "AI選股TOP5": [1],
            "下單清單": [1],
            "機構交易計畫": [1],
            "回測視覺化": [1],
        }.get(src, [1, 2, 0])
        for idx in index_candidates:
            if idx < len(vals):
                sid = normalize_stock_id(vals[idx])
                if sid:
                    return sid
        for v in vals:
            sid = normalize_stock_id(v)
            if sid:
                return sid
        return ""


    def open_three_windows(self):
        self.left_notebook.select(self.tab_top20)
        # 題材頁改用最新題材摘要，避免後端有資料但 UI 空白
        if self.theme_tree is not None:
            for item in self.theme_tree.get_children():
                self.theme_tree.delete(item)
            theme_df = getattr(self, "last_theme_summary_df", pd.DataFrame())
            if theme_df is not None and not theme_df.empty:
                tdf = theme_df.copy()
                if "top_name" not in tdf.columns:
                    top_map = {}
                    source_df = self.last_top20_df if self.last_top20_df is not None and not self.last_top20_df.empty else pd.DataFrame()
                    if source_df is not None and not source_df.empty and "theme" in source_df.columns:
                        for theme_name, grp in source_df.groupby("theme"):
                            try:
                                top_map[str(theme_name)] = str(grp.sort_values(["model_score", "trade_score", "win_rate"], ascending=False).iloc[0].get("stock_name", ""))
                            except Exception:
                                top_map[str(theme_name)] = ""
                    tdf["top_name"] = tdf["theme"].astype(str).map(top_map).fillna("")
                for _, r in tdf.iterrows():
                    self.theme_tree.insert("", "end", values=(r.get("theme", ""), int(r.get("count", 0) or 0), f"{float(r.get('avg_total', 0) or 0):.2f}", f"{float(r.get('avg_ai', 0) or 0):.2f}", r.get("top_name", "")))
        self.sync_multi_windows()
        stock_id = self.window_current_stock_id
        if not stock_id:
            stock_id = self.get_current_selected_stock_id()
        if not stock_id and self.last_top20_df is not None and not self.last_top20_df.empty:
            stock_id = str(self.last_top20_df.iloc[0]["stock_id"])
        if stock_id:
            self.update_multi_window_stock(stock_id)
        self.set_status("已同步到右側分析 / 圖表面板。")

    def ensure_multi_windows(self):
        return

    def sync_multi_windows(self):
        return

    def on_select_window_top20(self, event=None):
        stock_id = self.get_current_selected_stock_id()
        if stock_id and not self._should_ignore_select_event(stock_id, "桌面同步"):
            self.sync_all_views(stock_id, source="桌面同步")
        return

    def _tree_selected_stock_id(self, tree, value_index: int = 1):
        try:
            if tree is None:
                return None
            sel = tree.selection()
            if not sel:
                return None
            vals = tree.item(sel[0], "values")
            if vals and len(vals) > value_index:
                sid = str(vals[value_index]).strip()
                return sid or None
        except Exception:
            return None
        return None

    def _row_lookup(self, df: pd.DataFrame, stock_id: str):
        if df is None or df.empty or not stock_id:
            return None
        try:
            row = df[df["stock_id"].astype(str) == str(stock_id)]
            if row.empty:
                return None
            return row.iloc[0]
        except Exception:
            return None

    def get_current_selected_stock_id(self):
        for tree, idx in [
            (self.top20_tree, 1),
            (self.top5_tree, 1),
            (self.order_tree, 1),
            (self.inst_tree, 1),
            (self.backtest_tree, 1),
            (self.rank_tree, 1),
        ]:
            sid = self._tree_selected_stock_id(tree, idx)
            if sid:
                return sid
        if self.window_current_stock_id:
            return self.window_current_stock_id
        if self.last_top20_df is not None and not self.last_top20_df.empty:
            return str(self.last_top20_df.iloc[0]["stock_id"])
        ranking = self.build_enriched_ranking_dataframe(self._filtered_ranking(), scope="export")
        if ranking is not None and not ranking.empty:
            return str(ranking.iloc[0]["stock_id"])
        return None

    def _set_tree_selection_by_stock_id(self, tree, stock_id: str, value_index: int = 1):
        try:
            if tree is None or not stock_id:
                return
            found = None
            for item in tree.get_children():
                vals = tree.item(item, "values")
                if vals and len(vals) > value_index and str(vals[value_index]) == str(stock_id):
                    found = item
                    break
            if found is not None:
                tree.selection_set(found)
                tree.focus(found)
                tree.see(found)
        except Exception:
            pass

    def sync_multi_windows_selectors(self, stock_id: str):
        if not stock_id:
            return
        self.selector_syncing = True
        try:
            for tree, idx in [
                (self.top20_tree, 1),
                (self.top5_tree, 1),
                (self.order_tree, 1),
                (self.inst_tree, 1),
                (self.backtest_tree, 1),
                (self.rank_tree, 1),
            ]:
                self._set_tree_selection_by_stock_id(tree, stock_id, idx)
        finally:
            try:
                self.root.after(120, lambda: setattr(self, "selector_syncing", False))
            except Exception:
                self.selector_syncing = False


    def cache_trade_dataframe(self, df: pd.DataFrame):
        if df is None or df.empty or "stock_id" not in df.columns:
            return
        try:
            for _, row in df.iterrows():
                sid = str(row.get("stock_id", "")).strip()
                if not sid:
                    continue
                payload = row.to_dict()
                payload["_cached_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.plan_cache[sid] = payload
        except Exception:
            pass

    def cache_backtest_dataframe(self, df: pd.DataFrame):
        if df is None or df.empty or "stock_id" not in df.columns:
            return
        try:
            for _, row in df.iterrows():
                sid = str(row.get("stock_id", "")).strip()
                if not sid:
                    continue
                payload = row.to_dict()
                payload["_cached_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.backtest_cache[sid] = payload
        except Exception:
            pass

    def get_cached_trade_plan(self, stock_id: str):
        sid = str(stock_id).strip()
        if not sid:
            return None
        plan = self.plan_cache.get(sid)
        if plan:
            return plan
        for df in [getattr(self, "last_top20_df", pd.DataFrame()), getattr(self, "last_top5_df", pd.DataFrame()),
                   getattr(self, "last_today_buy_df", pd.DataFrame()), getattr(self, "last_wait_df", pd.DataFrame()),
                   getattr(self, "last_attack_df", pd.DataFrame()), getattr(self, "last_watch_df", pd.DataFrame()),
                   getattr(self, "last_defense_df", pd.DataFrame())]:
            row = self._row_lookup(df, sid)
            if row is not None:
                payload = row.to_dict()
                self.plan_cache[sid] = payload
                return payload
        return None

    def get_cached_backtest(self, stock_id: str):
        sid = str(stock_id).strip()
        if not sid:
            return None
        bt = self.backtest_cache.get(sid)
        if bt:
            return bt
        row = self._row_lookup(getattr(self, "last_top5_df", pd.DataFrame()), sid)
        if row is not None and "backtest_win_rate" in row.index:
            payload = {
                "backtest_win_rate": float(row.get("backtest_win_rate", 0) or 0),
                "avg_return": float(row.get("avg_return", 0) or 0),
                "cagr": float(row.get("cagr", 0) or 0),
                "mdd": float(row.get("mdd", 0) or 0),
                "sharpe": float(row.get("sharpe", 0) or 0),
                "samples": int(row.get("samples", 0) or 0),
            }
            self.backtest_cache[sid] = payload
            return payload
        return None

    def build_lightweight_plan(self, stock_id: str, hist: pd.DataFrame, stock=None) -> dict:
        if hist is None or hist.empty:
            return {
                "stock_id": stock_id, "stock_name": stock.get("stock_name", stock_id) if stock is not None else stock_id,
                "market": stock.get("market", "") if stock is not None else "",
                "industry": stock.get("industry", "") if stock is not None else "",
                "theme": stock.get("theme", "") if stock is not None else "",
                "price": round(close_, 2),
                "ui_state": "觀察", "trade_action": "HOLD", "entry_zone": "-", "stop_loss": "-",
                "target_1382": 0.0, "target_1618": 0.0, "support": 0.0, "resistance": 0.0, "rr": 0.0,
                "win_rate": 0.0, "wave": "資料不足", "signal": "載入中", "trade_type": "快速模式", "bucket": "觀察",
                "reason": "使用快速模式顯示，背景計算完成後會自動更新。", "kline_score": 0.0, "wave_score": 0.0,
                "fib_score": 0.0, "sakata_score": 0.0, "volume_score": 0.0, "indicator_score": 0.0
            }
        x = hist.copy()
        if "ma20" not in x.columns:
            x = DataEngine.attach(x)
        last = x.iloc[-1]
        close_ = float(last["close"])
        ma20 = float(last["ma20"]) if pd.notna(last.get("ma20")) else close_
        ma60 = float(last["ma60"]) if pd.notna(last.get("ma60")) else close_
        recent = x.tail(60)
        support = float(min(ma20, recent["low"].tail(20).min())) if not recent.empty else close_
        resistance = float(recent["high"].max()) if not recent.empty else close_
        fib_score, fib1382, fib1618 = FibEngine.score_and_targets(close_, support, resistance)
        signal = "偏多觀察" if close_ >= ma20 >= ma60 else "區間整理" if close_ >= ma20 else "轉弱警戒"
        wave = WaveEngine.detect_wave_label(x)
        entry_low = support * 1.002 if support > 0 else close_ * 0.99
        entry_high = entry_low * 1.01
        stop = support * 0.97 if support > 0 else close_ * 0.95
        risk = max(entry_high - stop, 0.01)
        reward = max(fib1382 - entry_high, 0.0)
        rr = round(reward / risk, 2)
        ui_state = "觀察" if signal in ("偏多觀察", "區間整理") else "不可買"
        return {
            "stock_id": stock_id,
            "stock_name": stock.get("stock_name", stock_id) if stock is not None else stock_id,
            "market": stock.get("market", "") if stock is not None else "",
            "industry": stock.get("industry", "") if stock is not None else "",
            "theme": stock.get("theme", "") if stock is not None else "",
            "ui_state": ui_state, "trade_action": "HOLD" if ui_state == "觀察" else "AVOID",
            "entry_zone": f"{entry_low:.2f} ~ {entry_high:.2f}",
            "entry_low": round(entry_low, 2), "entry_high": round(entry_high, 2),
            "stop_loss": f"{stop:.2f}",
            "target_1382": round(fib1382, 2), "target_1618": round(fib1618, 2),
            "support": round(support, 2), "resistance": round(resistance, 2), "rr": rr,
            "win_rate": 0.0, "wave": wave, "signal": signal, "trade_type": "快速模式", "bucket": "觀察",
            "reason": "已先顯示快速資料，完整交易計畫與回測由背景更新。", "kline_score": 0.0,
            "wave_score": 0.0, "fib_score": round(fib_score, 2), "sakata_score": 0.0,
            "volume_score": 0.0, "indicator_score": 0.0
        }

    def start_selection_analysis(self, stock_id: str, source: str = ""):
        if not stock_id:
            return
        self.append_log(f"點股分析啟動：{stock_id}｜來源 {source or '-'}")
        self.selection_job_token += 1
        token = self.selection_job_token
        self.selection_chart_pending_token = token
        self.selection_source = source or ""
        stock = self.db.get_stock_row(stock_id)
        hist = self.db.get_price_history(stock_id)
        if stock is None or hist.empty:
            return

        quick_plan = self.get_cached_trade_plan(stock_id)
        if quick_plan is None:
            try:
                quick_plan = self.build_lightweight_plan(stock_id, DataEngine.attach(hist.copy()), stock=stock)
                self.plan_cache[str(stock_id)] = quick_plan
            except Exception:
                quick_plan = None

        lines = self.build_unified_detail_lines(stock_id, source=(source or "快速顯示"), quick_only=True)
        lines.append("")
        lines.append("圖表狀態：背景輸出 PNG 後再載入右下圖表，避免點股時主畫面卡住。")
        self.detail.delete("1.0", tk.END)
        self.detail.insert("1.0", "\n".join(lines))
        try:
            self.show_chart_loading(stock_id)
        except Exception:
            pass

        t = threading.Thread(target=self._selection_analysis_worker, args=(str(stock_id), str(source or ""), token), daemon=True, name=f"select_{stock_id}")
        t.start()

    def _selection_analysis_worker(self, stock_id: str, source: str, token: int):
        try:
            log_info(f"點股背景分析開始：{stock_id}｜token={token}｜來源={source or '-'}")
            if token != self.selection_job_token:
                log_warning(f"點股背景分析略過（token過期）：{stock_id}｜token={token}")
                return

            stock = self.db.get_stock_row(stock_id)
            hist = self.db.get_price_history(stock_id)
            if stock is None or hist is None or hist.empty:
                log_warning(f"點股背景分析無資料：{stock_id}")
                self.ui_call(self.append_log, f"點股背景分析無資料：{stock_id}", "WARNING")
                return

            log_info(f"點股背景分析讀取資料完成：{stock_id}｜rows={len(hist)}")

            hist_attached = None
            try:
                hist_attached = DataEngine.attach(hist.copy())
            except Exception:
                hist_attached = hist.copy()

            plan = self.get_cached_trade_plan(stock_id)
            plan_is_full = bool(plan) and str(plan.get("trade_type", "")) != "快速模式" and float(plan.get("model_score", 0) or 0) > 0
            if not plan_is_full:
                try:
                    log_info(f"建立完整交易計畫：{stock_id}")
                    plan = self.master_trading_engine.plan_engine.build_plan(stock_id)
                except Exception as e:
                    log_exception(f"完整交易計畫失敗，改用快速模式：{stock_id}", e)
                    self.ui_call(self.append_log, f"完整交易計畫失敗，改用快速模式：{stock_id}｜{e}", "WARNING")
                    plan = self.build_lightweight_plan(stock_id, hist_attached, stock=stock)
                self.plan_cache[str(stock_id)] = plan

            if token != self.selection_job_token:
                return

            bt = self.get_cached_backtest(stock_id)
            bt_ready = bool(bt) and int(bt.get("samples", 0) or 0) >= 0
            if not bt_ready:
                try:
                    log_info(f"背景回測開始：{stock_id}")
                    bt = self.backtest_engine.estimate_trade_quality(stock_id)
                    log_info(f"背景回測完成：{stock_id}｜samples={bt.get('samples',0)}")
                except Exception as e:
                    log_exception(f"背景回測失敗：{stock_id}", e)
                    self.ui_call(self.append_log, f"背景回測失敗：{stock_id}｜{e}", "WARNING")
                    bt = {"backtest_win_rate": 0.0, "avg_return": 0.0, "avg_rr": 0.0, "cagr": 0.0, "mdd": 0.0, "sharpe": 0.0, "samples": 0}
                self.backtest_cache[str(stock_id)] = bt

            chart_path = None
            if token == self.selection_job_token:
                try:
                    self.ui_call(self.append_log, f"背景圖表輸出開始：{stock_id}")
                    log_info(f"背景圖表輸出開始：{stock_id}")
                    chart_path = self.export_chart(stock_id, hist)
                    self.ui_call(self.append_log, f"背景圖表輸出完成：{stock_id}")
                    log_info(f"背景圖表輸出完成：{stock_id}｜{chart_path}")
                except Exception as e:
                    log_exception(f"背景圖檔輸出失敗：{stock_id}", e)
                    self.ui_call(self.append_log, f"背景圖檔輸出失敗：{stock_id}｜{e}", "ERROR")

            def apply_result():
                if token != self.selection_job_token:
                    return
                if chart_path:
                    self.current_chart_path = chart_path
                try:
                    self.update_detail_panel(stock_id, source=source or "背景完成")
                except Exception as e:
                    self.append_log(f"背景分析更新失敗：{stock_id}｜{e}")
                if token != self.selection_chart_pending_token:
                    return
                try:
                    if chart_path:
                        self._schedule_chart_file_update(stock_id, chart_path)
                    else:
                        self.show_chart_message("圖表產生失敗，請改用『開啟圖表』或重新點選。")
                except Exception as e:
                    self.append_log(f"背景圖表更新失敗：{stock_id}｜{e}")

            self.ui_call(apply_result)
        except Exception as e:
            log_exception(f"背景選股分析失敗：{stock_id}", e)
            self.ui_call(self.append_log, f"背景選股分析失敗：{stock_id}｜{e}", "ERROR")

    def build_unified_detail_lines(self, stock_id: str, source: str = "", quick_only: bool = False):
        stock = self.db.get_stock_row(stock_id)
        hist = self.db.get_price_history(stock_id)
        if stock is None or hist.empty:
            return [f"《{source or '同步檢視'}》", f"股票：{stock_id}", "無資料"]

        hist = DataEngine.attach(hist.copy())
        last = hist.iloc[-1]
        trade_plan = self.get_cached_trade_plan(stock_id)
        if trade_plan is None:
            trade_plan = self.build_lightweight_plan(stock_id, hist, stock=stock)
            self.plan_cache[str(stock_id)] = trade_plan
        bt = self.get_cached_backtest(stock_id)
        if bt is None:
            bt = {"backtest_win_rate": 0.0, "avg_return": 0.0, "cagr": 0.0, "mdd": 0.0, "sharpe": 0.0, "samples": 0}

        rank_text = "-"
        try:
            ranking = self._filtered_ranking()
            if ranking is not None and not ranking.empty:
                row = ranking[ranking["stock_id"].astype(str) == str(stock_id)]
                if not row.empty:
                    rank_text = str(int(row.iloc[0]["rank_all"]))
        except Exception:
            pass

        close_ = float(last["close"]) if pd.notna(last.get("close")) else 0.0
        ma5 = float(last["ma5"]) if pd.notna(last.get("ma5")) else close_
        ma10 = float(last["ma10"]) if pd.notna(last.get("ma10")) else close_
        ma20 = float(last["ma20"]) if pd.notna(last.get("ma20")) else close_
        ma60 = float(last["ma60"]) if pd.notna(last.get("ma60")) else close_
        macd_hist = float(last["macd_hist"]) if pd.notna(last.get("macd_hist")) else 0.0
        rsi14 = float(last["rsi14"]) if pd.notna(last.get("rsi14")) else 50.0
        k_val = float(last["k"]) if pd.notna(last.get("k")) else 50.0
        d_val = float(last["d"]) if pd.notna(last.get("d")) else 50.0

        def _avg_slope(series, n=5):
            try:
                s = pd.to_numeric(series.tail(n), errors='coerce').dropna()
                if len(s) < 2:
                    return 0.0
                return float(s.diff().dropna().mean())
            except Exception:
                return 0.0

        slope5 = _avg_slope(hist["ma5"], 3) if "ma5" in hist.columns else 0.0
        slope20 = _avg_slope(hist["ma20"], 5) if "ma20" in hist.columns else 0.0
        slope60 = _avg_slope(hist["ma60"], 5) if "ma60" in hist.columns else 0.0

        def _trend_state(close_price, fast, slow, slope_fast, slope_slow, macd_value, rsi_value, k_now, d_now):
            score = 0
            if close_price >= fast:
                score += 1
            if fast >= slow:
                score += 1
            if slope_fast > 0:
                score += 1
            if slope_slow > 0:
                score += 1
            if macd_value > 0:
                score += 1
            if 45 <= rsi_value <= 72:
                score += 1
            if k_now >= d_now:
                score += 1
            if score >= 5:
                return "多"
            if score <= 2:
                return "空"
            return "盤整"

        trend_short = _trend_state(close_, ma5, ma10, slope5, slope20, macd_hist, rsi14, k_val, d_val)
        trend_mid = _trend_state(close_, ma20, ma60, slope20, slope60, macd_hist, rsi14, k_val, d_val)
        trend_long = _trend_state(close_, ma60, ma60, slope60, slope60, macd_hist, rsi14, k_val, d_val)

        recent = hist.tail(89)
        recent_high = float(recent["high"].max()) if not recent.empty else close_
        recent_low = float(recent["low"].min()) if not recent.empty else close_
        width = max(recent_high - recent_low, 1e-6)
        pos = (close_ - recent_low) / width if width > 0 else 0.5
        signal = str(trade_plan.get("signal", "-"))
        if close_ >= recent_high * 0.995 and rsi14 > 72:
            wave_position = "第5浪"
        elif close_ >= recent_high * 0.995 and macd_hist > 0:
            wave_position = "第3浪"
        elif close_ > ma20 > ma60 and macd_hist > 0 and 0.45 <= pos <= 0.7:
            wave_position = "第1浪"
        elif close_ > ma20 and macd_hist >= 0 and pos < 0.45:
            wave_position = "第2浪"
        elif close_ >= ma20 and abs(close_ - ma20) / max(ma20, 1e-6) <= 0.04 and 0.45 <= pos <= 0.75:
            wave_position = "第4浪"
        elif close_ < ma20 and macd_hist < 0 and pos <= 0.35:
            wave_position = "A浪"
        elif close_ < (recent_low + width * 0.5) and macd_hist >= 0 and 0.25 <= pos <= 0.55:
            wave_position = "B浪"
        elif close_ < ma60 and macd_hist < 0 and pos < 0.25:
            wave_position = "C浪"
        else:
            wave_position = "區間整理"

        if wave_position in ("第1浪", "第3浪", "第5浪"):
            wave_structure = "推動浪"
        elif wave_position in ("A浪", "B浪", "C浪"):
            wave_structure = "修正浪"
        else:
            wave_structure = "整理浪"

        entry_low = float(trade_plan.get("entry_low", 0) or 0)
        entry_high = float(trade_plan.get("entry_high", 0) or 0)
        rr_value = float(trade_plan.get("rr", 0) or 0)
        model_win_rate = float(trade_plan.get("win_rate", 0) or 0)
        backtest_win_rate = float(bt.get("backtest_win_rate", 0) or 0)
        decision = str(trade_plan.get("trade_action", trade_plan.get("decision", "")) or "")
        in_entry_zone = TradingPlanEngine._in_entry_zone(close_, entry_low, entry_high)

        if decision == "BUY":
            display_ai = "可買" if in_entry_zone else "準備買"
        elif decision == "WEAK BUY":
            display_ai = "預掛單"
        elif decision == "HOLD":
            display_ai = "觀察"
        elif decision == "AVOID":
            display_ai = "不可買"
        else:
            display_ai = str(trade_plan.get("ui_state", "觀察") or "觀察")

        semantic_note = ""
        if rr_value >= 2.0 and max(model_win_rate, backtest_win_rate) < 50:
            semantic_note = "屬高RR型，宜小倉位"
        elif rr_value < 1.2 and max(model_win_rate, backtest_win_rate) >= 60:
            semantic_note = "屬穩健型，目標空間有限"

        analysis_mode = "快速模式" if quick_only or str(trade_plan.get("trade_type", "")) == "快速模式" else "完整模式"
        reason_text = str(trade_plan.get("reason", "-"))
        if semantic_note:
            reason_text = f"{reason_text}｜{semantic_note}"

        lines = [
            f"《V3.6.1 AI語意收斂版｜{analysis_mode}》",
            f"股票：{stock['stock_name']} ({stock_id})｜排行：{rank_text}",
            f"市場 / 產業 / 題材：{stock['market']} / {stock['industry']} / {stock['theme']}",
            f"資料來源：{source or '-'}｜最新收盤：{close_:.2f}",
            "",
            "【趨勢判斷模組】",
            f"短線趨勢：{trend_short}｜依據 MA5/MA10、短斜率、MACD、RSI、KD",
            f"中線趨勢：{trend_mid}｜依據 MA20/MA60、均線斜率與收盤位置",
            f"長線趨勢：{trend_long}｜依據 MA60 結構、長斜率與中長期位置",
            f"MA5 / MA10 / MA20 / MA60：{ma5:.2f} / {ma10:.2f} / {ma20:.2f} / {ma60:.2f}",
            f"MACD Hist / RSI14 / KD：{macd_hist:.4f} / {rsi14:.2f} / {k_val:.2f}-{d_val:.2f}",
            "",
            "【波浪分析模組】",
            f"波浪結構：{wave_structure}",
            f"可能位置：{wave_position}",
            f"交易訊號：{signal}｜交易型態：{trade_plan.get('trade_type','-')}",
            "用途：不是只看漲跌，而是判斷目前位在推動、修正或整理的哪一段。",
            "",
            "【費波南西目標位模組】",
            f"支撐位 / 壓力位：{float(trade_plan.get('support',0) or 0):.2f} / {float(trade_plan.get('resistance',0) or 0):.2f}",
            f"Fib 1.0：{float(trade_plan.get('resistance',0) or 0):.2f}",
            f"Fib 1.382：{float(trade_plan.get('target_1382',0) or 0):.2f}",
            f"Fib 1.618：{float(trade_plan.get('target_1618',0) or 0):.2f}",
            f"進場區 / 停損 / RR：{trade_plan.get('entry_zone','-')} / {trade_plan.get('stop_loss','-')} / {rr_value:.2f}",
            "用途：判斷目標價、追價風險與停利停損區間。",
            "",
            "【AI建議模組】",
            f"AI結論：{display_ai}",
            f"執行狀態：{display_ai}｜決策：{decision or '-'}｜分類：{trade_plan.get('bucket','-')}",
            f"盤中狀態：{trade_plan.get('liquidity_status','-')}｜活性分：{float(trade_plan.get('liquidity_score',0) or 0):.1f}｜淘汰原因：{trade_plan.get('elimination_reason','-')}",
            f"模型分數 / 交易分數：{float(trade_plan.get('model_score',0) or 0):.2f} / {float(trade_plan.get('wave_trade_score', trade_plan.get('trade_score',0)) or 0):.2f}",
            f"勝率 / 回測勝率：{model_win_rate:.1f}% / {backtest_win_rate:.1f}%",
            f"平均報酬 / CAGR / MDD / Sharpe：{float(bt.get('avg_return',0) or 0):.2f}% / {float(bt.get('cagr',0) or 0):.2f}% / {float(bt.get('mdd',0) or 0):.2f}% / {float(bt.get('sharpe',0) or 0):.2f}",
            f"一句話：{reason_text}",
        ]
        try:
            lines.extend(self.build_tactical_section_lines(stock_id, hist, stock, trade_plan))
        except Exception as e:
            lines.extend(["", f"【Phase 1 Tactical Layer】載入失敗：{e}"])
        if quick_only:
            lines.extend(["", "備註：目前為快速模式，完整 AI 分析與回測背景完成後會自動更新。"])
        return lines
    def _find_trade_row_snapshot(self, stock_id: str) -> dict:
        stock_id = str(stock_id or '').strip()
        frames = [
            getattr(self, 'last_top20_df', pd.DataFrame()),
            getattr(self, 'last_top5_df', pd.DataFrame()),
            getattr(self, 'last_today_buy_df', pd.DataFrame()),
            getattr(self, 'last_wait_df', pd.DataFrame()),
            getattr(self, 'last_execution_pool_df', pd.DataFrame()),
        ]
        for df in frames:
            try:
                if df is not None and not df.empty and 'stock_id' in df.columns:
                    row = df[df['stock_id'].astype(str) == stock_id]
                    if not row.empty:
                        return dict(row.iloc[0])
            except Exception:
                continue
        return {}

    def enrich_trade_dataframe_with_tactical(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame() if df is None else df.copy()
        x = df.copy()
        for idx, r in x.iterrows():
            try:
                stock_id = str(r.get('stock_id', '') or '')
                stock = self.db.get_stock_row(stock_id)
                hist = self.db.get_price_history(stock_id)
                if stock is None or hist is None or hist.empty:
                    continue
                hist = DataEngine.attach(hist.copy())
                last = hist.iloc[-1]
                realtime = self.data_engine.get_realtime_quote(stock_id, str(stock.get('market', '上市')), hist=hist)
                close = float(realtime.get('close', last.get('close', 0)) or 0)
                prev_close = float(realtime.get('prev_close', hist.iloc[-2]['close']) if len(hist) >= 2 else realtime.get('prev_close', close) or close)
                open_price = float(realtime.get('open', close) or close)
                high_price = float(realtime.get('high', close) or close)
                low_price = float(realtime.get('low', close) or close)
                change_pct = round(((close - prev_close) / prev_close) * 100, 2) if prev_close else 0.0
                ma20 = float(last.get('ma20', close) or close)
                ma60 = float(last.get('ma60', close) or close)
                rsi = float(last.get('rsi14', 50) or 50)
                trend_score = float(r.get('total_score', r.get('model_score', 0)) or 0)
                support = float(r.get('support', 0) or 0)
                resistance = float(r.get('resistance', 0) or 0)
                if support <= 0 or resistance <= 0 or resistance <= support:
                    recent = hist.tail(40)
                    support = float(recent['low'].min()) if not recent.empty else close
                    resistance = float(recent['high'].max()) if not recent.empty else close
                intraday_score = self.tactical_score_engine.calc_intraday_tactical_score(close, prev_close, open_price, high_price, low_price, support, resistance, realtime.get('orderbook_bias', '無有效五檔'), change_pct)
                signal, advice, state_bucket = self.tactical_score_engine.evaluate_intraday_trade_state(close, prev_close, open_price, support, resistance, change_pct, trend_score, intraday_score, trend_score, realtime.get('orderbook_bias', '無有效五檔'), ma20=ma20, ma60=ma60, rsi=rsi)
                leader = self.tactical_score_engine.classify_leader_candidate(trend_score, trend_score, intraday_score, close, ma20, ma60, resistance, rsi, signal, realtime.get('orderbook_bias', '無有效五檔'))
                light = self.ui_tactical_presenter.derive_signal_light(signal, trend_score, change_pct, intraday_score)
                ctx = {
                    'close': close, 'ma20': ma20, 'ma60': ma60, 'rsi': rsi, 'score': trend_score, 'trend_score': trend_score, 'intraday_score': intraday_score,
                    'support': support, 'resistance': resistance, 'signal': signal, 'advice': advice, 'orderbook_bias': realtime.get('orderbook_bias', '無有效五檔'),
                    'orderbook_ratio': realtime.get('orderbook_ratio', '-'), 'change_pct': change_pct, 'wave_structure': r.get('wave_structure', '-'), 'wave_position': r.get('wave_position', '-'),
                    'trade_type': r.get('trade_type', '-'), 'entry_zone': r.get('entry_zone', '-'), 'stop_loss': r.get('stop_loss', '-'), 'rr': r.get('rr', 0), 'target_1382': r.get('target_1382', 0), 'target_1618': r.get('target_1618', 0),
                    'state_bucket': state_bucket, 'light': light, 'leader_candidate': leader
                }
                scripts = self.single_stock_narrative_engine.build_trade_scripts_text(ctx)
                derived_bucket = self._derive_bucket_from_light(light, is_etf=bool(int(r.get('is_etf', 0) or 0) == 1 or str(r.get('market', '')) == 'ETF'))
                derived_grade = self._derive_operation_grade_from_light(light, is_etf=bool(int(r.get('is_etf', 0) or 0) == 1 or str(r.get('market', '')) == 'ETF'))
                x.at[idx, 'price'] = close
                x.at[idx, 'tactical_light'] = light
                x.at[idx, 'bucket'] = derived_bucket
                x.at[idx, 'decision_bucket'] = derived_bucket
                x.at[idx, 'operation_grade'] = derived_grade
                x.at[idx, 'trade_action'] = 'BUY' if light in ('🔵', '🟢') else ('HOLD' if light in ('🟡', '🟠') else 'AVOID')
                x.at[idx, 'orderbook_bias'] = realtime.get('orderbook_bias', '無有效五檔')
                x.at[idx, 'orderbook_ratio'] = realtime.get('orderbook_ratio', '-')
                x.at[idx, 'intraday_score'] = intraday_score
                x.at[idx, 'state_bucket'] = state_bucket
                x.at[idx, 'leader_candidate'] = leader
                x.at[idx, 'summary_block'] = self.single_stock_narrative_engine.build_summary_block(ctx)
                x.at[idx, 'ai_analysis_text'] = self.single_stock_narrative_engine.build_ai_summary_text(ctx)
                x.at[idx, 'wave_analysis_text'] = self.single_stock_narrative_engine.build_wave_summary_text(ctx)
                x.at[idx, 'fibo_analysis_text'] = self.single_stock_narrative_engine.build_fibo_summary_text(ctx)
                x.at[idx, 'path_analysis_text'] = self.single_stock_narrative_engine.build_bull_bear_path_text(ctx)
                x.at[idx, 'risk_note_tactical'] = self.single_stock_narrative_engine.build_tactical_risk_note(close, support, resistance, rsi, trend_score, change_pct)
                x.at[idx, 'script_a'] = scripts.get('script_a', '')
                x.at[idx, 'script_b'] = scripts.get('script_b', '')
                x.at[idx, 'script_c'] = scripts.get('script_c', '')
            except Exception as e:
                try:
                    self.append_log(f'戰術欄位補強失敗：{r.get("stock_id", "")}｜{e}', 'WARNING')
                except Exception:
                    pass
        return x

    def build_tactical_section_lines(self, stock_id: str, hist: pd.DataFrame, stock: pd.Series, trade_plan: dict) -> list[str]:
        cached = self._find_trade_row_snapshot(stock_id)
        hist = DataEngine.attach(hist.copy())
        last = hist.iloc[-1]
        realtime = self.data_engine.get_realtime_quote(stock_id, str(stock.get('market', '上市')), hist=hist)
        close = float(realtime.get('close', last.get('close', 0)) or 0)
        prev_close = float(realtime.get('prev_close', hist.iloc[-2]['close']) if len(hist) >= 2 else realtime.get('prev_close', close) or close)
        open_price = float(realtime.get('open', close) or close)
        high_price = float(realtime.get('high', close) or close)
        low_price = float(realtime.get('low', close) or close)
        ma20 = float(last.get('ma20', close) or close)
        ma60 = float(last.get('ma60', close) or close)
        rsi = float(last.get('rsi14', 50) or 50)
        support = float(trade_plan.get('support', cached.get('support', 0)) or 0)
        resistance = float(trade_plan.get('resistance', cached.get('resistance', 0)) or 0)
        if support <= 0 or resistance <= 0 or resistance <= support:
            recent = hist.tail(40)
            support = float(recent['low'].min()) if not recent.empty else close
            resistance = float(recent['high'].max()) if not recent.empty else close
        base_score = float(trade_plan.get('model_score', trade_plan.get('total_score', trade_plan.get('wave_trade_score', 0))) or 0)
        change_pct = round(((close - prev_close) / prev_close) * 100, 2) if prev_close else 0.0
        intraday_score = float(cached.get('intraday_score', 0) or 0)
        signal = str(cached.get('signal', trade_plan.get('signal', '-')) or '-')
        advice = str(cached.get('trade_action', trade_plan.get('trade_action', '-')) or '-')
        state_bucket = str(cached.get('state_bucket', 'range') or 'range')
        leader = str(cached.get('leader_candidate', '-') or '-')
        light = str(cached.get('tactical_light', '') or '')
        if intraday_score <= 0 or not light:
            intraday_score = self.tactical_score_engine.calc_intraday_tactical_score(close, prev_close, open_price, high_price, low_price, support, resistance, realtime.get('orderbook_bias', '無有效五檔'), change_pct)
            signal, advice, state_bucket = self.tactical_score_engine.evaluate_intraday_trade_state(close, prev_close, open_price, support, resistance, change_pct, base_score, intraday_score, base_score, realtime.get('orderbook_bias', '無有效五檔'), ma20=ma20, ma60=ma60, rsi=rsi)
            leader = self.tactical_score_engine.classify_leader_candidate(base_score, base_score, intraday_score, close, ma20, ma60, resistance, rsi, signal, realtime.get('orderbook_bias', '無有效五檔'))
            light = self.ui_tactical_presenter.derive_signal_light(signal, base_score, change_pct, intraday_score)
        ctx = {
            'close': close, 'ma20': ma20, 'ma60': ma60, 'rsi': rsi, 'score': base_score, 'trend_score': base_score, 'intraday_score': intraday_score,
            'support': support, 'resistance': resistance, 'signal': signal, 'advice': advice, 'orderbook_bias': realtime.get('orderbook_bias', '無有效五檔'),
            'orderbook_ratio': realtime.get('orderbook_ratio', '-'), 'change_pct': change_pct, 'wave_structure': trade_plan.get('wave_structure', '-'), 'wave_position': trade_plan.get('wave_position', '-'),
            'trade_type': trade_plan.get('trade_type', '-'), 'entry_zone': trade_plan.get('entry_zone', '-'), 'stop_loss': trade_plan.get('stop_loss', '-'), 'rr': trade_plan.get('rr', 0), 'target_1382': trade_plan.get('target_1382', 0), 'target_1618': trade_plan.get('target_1618', 0),
            'state_bucket': state_bucket, 'light': light, 'leader_candidate': leader
        }
        scripts = self.single_stock_narrative_engine.build_trade_scripts_text(ctx)
        return [
            '',
            '【Phase 1 Tactical Layer】',
            f"燈號 / 五檔 / 比率：{light} / {realtime.get('orderbook_bias','無有效五檔')} / {realtime.get('orderbook_ratio','-')}",
            f"盤中分 / 狀態 / 主升候選：{intraday_score:.1f} / {state_bucket} / {leader}",
            f"即時價 / 昨收 / 開高低：{close:.2f} / {prev_close:.2f} / {open_price:.2f}-{high_price:.2f}-{low_price:.2f}",
            f"風險提示：{self.single_stock_narrative_engine.build_tactical_risk_note(close, support, resistance, rsi, base_score, change_pct)}",
            '',
            self.single_stock_narrative_engine.build_ai_summary_text(ctx),
            '',
            self.single_stock_narrative_engine.build_wave_summary_text(ctx),
            '',
            self.single_stock_narrative_engine.build_fibo_summary_text(ctx),
            '',
            self.single_stock_narrative_engine.build_bull_bear_path_text(ctx),
            '',
            '【交易劇本】',
            scripts.get('script_a', ''),
            scripts.get('script_b', ''),
            scripts.get('script_c', ''),
        ]

    def update_detail_panel(self, stock_id: str, source: str = ""):
        lines = self.build_unified_detail_lines(stock_id, source=source or "多來源同步模式")
        self.detail.delete("1.0", tk.END)
        self.detail.insert("1.0", "\n".join(lines))

    def start_backtest_selection_job(self, stock_id: str):
        stock_id = str(stock_id or "").strip()
        if not stock_id:
            return
        self.backtest_selection_token += 1
        token = self.backtest_selection_token
        self.window_current_stock_id = stock_id
        self.sync_multi_windows_selectors(stock_id)

        lines = self.build_unified_detail_lines(stock_id, source="回測視覺化", quick_only=True)
        lines.extend([
            "",
            "回測圖表狀態：背景計算 Equity Curve 中，完成後自動更新右下圖表。",
        ])
        self.detail.delete("1.0", tk.END)
        self.detail.insert("1.0", "\n".join(lines))
        try:
            self.show_chart_loading(stock_id)
        except Exception:
            pass
        self.append_log(f"回測視覺化背景任務啟動：{stock_id}")
        t = threading.Thread(
            target=self._backtest_selection_worker,
            args=(stock_id, token),
            daemon=True,
            name=f"bt_select_{stock_id}"
        )
        t.start()

    def _backtest_selection_worker(self, stock_id: str, token: int):
        try:
            log_info(f"回測視覺化背景開始：{stock_id}｜token={token}")
            if token != self.backtest_selection_token:
                return

            stock = self.db.get_stock_row(stock_id)
            hist = self.db.get_price_history(stock_id)
            if stock is None or hist is None or hist.empty:
                self.ui_call(self.append_log, f"回測視覺化無資料：{stock_id}", "WARNING")
                return

            try:
                bt = self.backtest_engine.estimate_trade_quality(stock_id)
                self.backtest_cache[str(stock_id)] = bt
                log_info(f"回測視覺化回測完成：{stock_id}｜samples={bt.get('samples', 0)}")
            except Exception as e:
                log_exception(f"回測視覺化回測失敗：{stock_id}", e)
                self.ui_call(self.append_log, f"回測視覺化回測失敗：{stock_id}｜{e}", "ERROR")

            if token != self.backtest_selection_token:
                return

            eq_path = None
            try:
                log_info(f"回測視覺化圖表輸出開始：{stock_id}")
                eq_path = self.export_equity_curve_chart(stock_id, hist)
                log_info(f"回測視覺化圖表輸出完成：{stock_id}｜{eq_path}")
            except Exception as e:
                log_exception(f"回測視覺化圖表輸出失敗：{stock_id}", e)
                self.ui_call(self.append_log, f"回測視覺化圖表輸出失敗：{stock_id}｜{e}", "ERROR")

            def apply_result():
                if token != self.backtest_selection_token:
                    return
                try:
                    self.update_detail_panel(stock_id, source="回測視覺化")
                except Exception as e:
                    self.append_log(f"回測視覺化明細更新失敗：{stock_id}｜{e}", "WARNING")
                try:
                    if eq_path:
                        self.current_chart_path = eq_path
                        self._schedule_chart_file_update(stock_id, eq_path)
                    else:
                        self.show_chart_message("回測圖表產生失敗，請重新點選。")
                    try:
                        self.right_lower_notebook.select(self.chart_tab)
                    except Exception:
                        pass
                except Exception as e:
                    self.append_log(f"回測視覺化圖表更新失敗：{stock_id}｜{e}", "WARNING")

            self.ui_call(apply_result)
        except Exception as e:
            log_exception(f"回測視覺化背景失敗：{stock_id}", e)
            self.ui_call(self.append_log, f"回測視覺化背景失敗：{stock_id}｜{e}", "ERROR")

    def update_chart_panel(self, stock_id: str):
        self.window_current_stock_id = stock_id
        if self.current_chart_path and Path(self.current_chart_path).exists():
            self._schedule_chart_file_update(stock_id, self.current_chart_path)
            return
        self.show_chart_loading(stock_id)

    def safe_sync_stock_views(self, stock_id: str, source: str = ""):
        if not stock_id:
            return
        log_info(f"同步個股檢視：{stock_id}｜來源={source or '-'}")
        stock = self.db.get_stock_row(stock_id)
        hist = self.db.get_price_history(stock_id)
        if stock is None or hist.empty:
            return
        self.window_current_stock_id = stock_id
        self.sync_multi_windows_selectors(stock_id)
        try:
            self.start_selection_analysis(stock_id, source=source)
        except Exception as e:
            self.detail.delete("1.0", tk.END)
            self.detail.insert("1.0", f"股票：{stock_id}\n選股同步更新失敗：{e}")
            self.append_log(f"選股同步更新失敗：{stock_id}｜{e}")

    def sync_all_views(self, stock_id: str, source: str = ""):
        self.safe_sync_stock_views(stock_id, source=source)

    def _schedule_chart_update(self, stock_id: str):
        self.pending_stock_id = stock_id
        try:
            if self.chart_update_job is not None:
                self.root.after_cancel(self.chart_update_job)
        except Exception:
            pass
        self.chart_update_job = self.root.after(180, self._flush_chart_update)

    def _flush_chart_update(self):
        self.chart_update_job = None
        stock_id = self.pending_stock_id
        self.pending_stock_id = None
        if not stock_id:
            return
        self.update_multi_window_stock(stock_id)

    def _schedule_chart_file_update(self, stock_id: str, chart_path):
        self.pending_chart_image = (str(stock_id), str(chart_path))
        try:
            if self.chart_image_job is not None:
                self.root.after_cancel(self.chart_image_job)
        except Exception:
            pass
        self.chart_image_job = self.root.after(80, self._flush_chart_file_update)

    def _flush_chart_file_update(self):
        self.chart_image_job = None
        payload = self.pending_chart_image
        self.pending_chart_image = None
        if not payload:
            return
        stock_id, chart_path = payload
        self.update_multi_window_stock(stock_id, chart_path=chart_path)

    def show_chart_message(self, message: str):
        if self.chart_fig is None or self.chart_canvas is None:
            return
        self.chart_fig.clear()
        ax = self.chart_fig.add_subplot(111)
        ax.axis("off")
        ax.text(0.5, 0.5, safe_plot_text(message, fallback="圖表訊息"), ha="center", va="center", transform=ax.transAxes, fontfamily=SELECTED_PLOT_FONT, fontsize=12)
        self.chart_fig.tight_layout()
        self.chart_canvas.draw_idle()

    def show_chart_loading(self, stock_id: str):
        self.append_log(f"圖表載入中：{stock_id}")
        self.show_chart_message(f"{stock_id} 圖表背景載入中…")

    def show_chart_file(self, chart_path):
        self.append_log(f"載入圖表檔：{chart_path}")
        if self.chart_fig is None or self.chart_canvas is None:
            return
        p = Path(chart_path)
        if not p.exists():
            self.show_chart_message("找不到圖表檔案")
            return
        self.chart_fig.clear()
        ax = self.chart_fig.add_subplot(111)
        ax.axis("off")
        img = plt.imread(str(p))
        ax.imshow(img)
        self.chart_fig.tight_layout()
        self.chart_canvas.draw_idle()

    def _candlestick(self, ax, x_vals, opens, highs, lows, closes):
        width = 0.55
        for xi, op, hi, lo, cl in zip(x_vals, opens, highs, lows, closes):
            color = "#d62728" if cl >= op else "#2ca02c"
            ax.vlines(xi, lo, hi, color=color, linewidth=1)
            bottom = min(op, cl)
            height = abs(cl - op)
            if height < 1e-6:
                height = max((hi - lo) * 0.02, 0.05)
            rect = Rectangle((xi - width / 2, bottom), width, height, facecolor=color, edgecolor=color, alpha=0.65)
            ax.add_patch(rect)

    def build_window_plan_lines(self, stock_id: str):
        stock = self.db.get_stock_row(stock_id)
        hist = self.db.get_price_history(stock_id)
        if stock is None or hist.empty:
            return ["無資料"]
        hist = DataEngine.attach(hist)
        last = hist.iloc[-1]
        trade_plan = self.master_trading_engine.plan_engine.build_plan(stock_id)
        bt = self.backtest_engine.estimate_trade_quality(stock_id)
        wave = WaveEngine.detect_wave_label(hist)
        return [
            "《v9.2 交易計畫視窗》",
            f"股票：{stock['stock_name']} ({stock_id})",
            f"市場 / 產業 / 題材：{stock['market']} / {stock['industry']} / {stock['theme']}",
            f"最新收盤：{float(last['close']):.2f}",
            f"狀態：{trade_plan.get('ui_state','-')}｜決策：{trade_plan.get('trade_action','-')}｜盤中：{trade_plan.get('liquidity_status','-')} {float(trade_plan.get('liquidity_score',0) or 0):.1f}",
            f"波浪：{wave}｜訊號：{trade_plan.get('signal','-')}｜交易型態：{trade_plan.get('trade_type','-')}",
            f"進場區：{trade_plan.get('entry_zone','-')}",
            f"停損：{trade_plan.get('stop_loss','-')}",
            f"Support / Fib1.0：{float(trade_plan.get('support',0) or 0):.2f} / {float(trade_plan.get('resistance',0) or 0):.2f}",
            f"Fib 1.382 / 1.618：{float(trade_plan.get('target_1382',0) or 0):.2f} / {float(trade_plan.get('target_1618',0) or 0):.2f}",
            f"RR：{float(trade_plan.get('rr',0) or 0):.2f}｜勝率：{float(trade_plan.get('win_rate',0) or 0):.1f}%",
            f"六模組：K {trade_plan.get('kline_score',0):.1f}｜波 {trade_plan.get('wave_score',0):.1f}｜費 {trade_plan.get('fib_score',0):.1f}｜阪 {trade_plan.get('sakata_score',0):.1f}｜量 {trade_plan.get('volume_score',0):.1f}｜指 {trade_plan.get('indicator_score',0):.1f}",
            f"回測：勝率 {float(bt.get('backtest_win_rate',0) or 0):.1f}%｜CAGR {float(bt.get('cagr',0) or 0):.2f}%｜MDD {float(bt.get('mdd',0) or 0):.2f}%｜Sharpe {float(bt.get('sharpe',0) or 0):.2f}",
            f"理由：{trade_plan.get('reason','-')}",
        ]

    def update_multi_window_stock(self, stock_id: str, chart_path: str | None = None):
        self.ensure_multi_windows()
        self.window_current_stock_id = stock_id
        try:
            if chart_path and Path(chart_path).exists():
                self.show_chart_file(chart_path)
            elif self.current_chart_path and Path(self.current_chart_path).exists():
                self.show_chart_file(self.current_chart_path)
            else:
                self.show_chart_loading(stock_id)
            try:
                self.right_lower_notebook.select(self.chart_tab)
            except Exception:
                pass
        except Exception as e:
            self.append_log(f"圖表更新失敗：{stock_id}｜{e}")

    def draw_live_chart(self, stock_id: str):
        if self.chart_fig is None or self.chart_canvas is None:
            return
        if self.chart_updating:
            self.pending_stock_id = stock_id
            return
        self.chart_updating = True
        try:
            stock = self.db.get_stock_row(stock_id)
            hist = self.db.get_price_history(stock_id)
            if stock is None or hist.empty:
                return
            hist = DataEngine.attach(hist.copy()).tail(90).reset_index(drop=True)
            if hist.empty:
                return

            plan = self.get_cached_trade_plan(stock_id)
            if plan is None:
                plan = self.build_lightweight_plan(stock_id, hist, stock=stock)
                self.plan_cache[str(stock_id)] = plan
            wave = WaveEngine.detect_wave_label(hist)
            x = list(range(len(hist)))
            self.chart_fig.clear()
            ax = self.chart_fig.add_subplot(111)

            self._candlestick(ax, x, hist["open"], hist["high"], hist["low"], hist["close"])
            ax.plot(x, hist["ma20"], label="MA20", linewidth=1.2)
            ax.plot(x, hist["ma60"], label="MA60", linewidth=1.2)

            support = float(plan.get("support", 0) or 0)
            fib1 = float(plan.get("resistance", 0) or 0)
            fib1382 = float(plan.get("target_1382", 0) or 0)
            fib1618 = float(plan.get("target_1618", 0) or 0)
            try:
                stop = float(plan.get("stop_loss", 0) or 0)
            except Exception:
                stop = 0.0

            if support > 0:
                ax.axhline(support, linestyle="--", linewidth=1.0, label=f"Support {support:.2f}")
            if fib1 > 0:
                ax.axhline(fib1, linestyle="--", linewidth=1.0, label=f"Fib 1.0 {fib1:.2f}")
            if fib1382 > 0:
                ax.axhline(fib1382, linestyle=":", linewidth=1.0, label=f"Fib 1.382 {fib1382:.2f}")
            if fib1618 > 0:
                ax.axhline(fib1618, linestyle=":", linewidth=1.0, label=f"Fib 1.618 {fib1618:.2f}")

            recent = hist.tail(55)
            try:
                peak_idx = recent["high"].idxmax()
                trough_idx = recent["low"].idxmin()
                peak_y = float(hist.loc[peak_idx, "high"])
                trough_y = float(hist.loc[trough_idx, "low"])
                ax.scatter([peak_idx], [peak_y], s=45, marker="o")
                ax.scatter([trough_idx], [trough_y], s=45, marker="o")
                ax.annotate("Wave Peak", xy=(peak_idx, peak_y), xytext=(peak_idx, peak_y * 1.02), fontfamily=SELECTED_PLOT_FONT)
                ax.annotate("Wave Trough", xy=(trough_idx, trough_y), xytext=(trough_idx, trough_y * 0.98), fontfamily=SELECTED_PLOT_FONT)
            except Exception:
                pass

            last_close = float(hist.iloc[-1]["close"])
            last_x = x[-1]
            bull_target = fib1382 if fib1382 > 0 else last_close * 1.08
            bear_target = stop if stop > 0 else last_close * 0.95
            path_x = [last_x, last_x + 4, last_x + 9]
            bull_y = [last_close, (last_close + bull_target) / 2.0, bull_target]
            bear_y = [last_close, (last_close + bear_target) / 2.0, bear_target]
            ax.plot(path_x, bull_y, "--", linewidth=1.6, label="Bull Path")
            ax.plot(path_x, bear_y, "--", linewidth=1.6, label="Bear Path")

            ax.set_xlim(0, max(path_x) + 2)
            title_stock = safe_plot_text(stock.get("stock_name", stock_id), fallback=str(stock_id))
            title_wave = safe_plot_text(wave, fallback="Wave")
            title_signal = safe_plot_text(plan.get("signal", "-"), fallback="-")
            ax.set_title(f"{title_stock}({stock_id}) | {title_wave} | {title_signal}", fontfamily=SELECTED_PLOT_FONT)
            info_text = (
                f"Wave: {title_wave}\n"
                f"Entry: {safe_plot_text(plan.get('entry_zone','-'))}\n"
                f"Stop: {safe_plot_text(plan.get('stop_loss','-'))}\n"
                f"RR: {float(plan.get('rr',0) or 0):.2f}"
            )
            ax.text(
                0.01, 0.98,
                info_text,
                transform=ax.transAxes, va="top", ha="left", fontfamily=SELECTED_PLOT_FONT,
                bbox=dict(boxstyle="round", alpha=0.15)
            )
            ax.grid(alpha=0.2)
            ax.legend(loc="upper left", fontsize=8, prop={"family": SELECTED_PLOT_FONT, "size": 8})
            self.chart_fig.tight_layout()
            self.chart_canvas.draw_idle()
        finally:
            self.chart_updating = False
            if self.pending_stock_id and self.pending_stock_id != stock_id:
                next_stock = self.pending_stock_id
                self.pending_stock_id = None
                try:
                    self.root.after(50, lambda s=next_stock: self.update_multi_window_stock(s))
                except Exception:
                    pass

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
                "唯一決策": getattr(self, "last_final_decision_df", pd.DataFrame()),
                "下單清單": self.last_order_list_df,
                "機構交易計畫": getattr(self, "last_institutional_plan_df", pd.DataFrame()),
                "操作SOP": getattr(self, "last_operation_sop_df", pd.DataFrame()),
                "排行": self._filtered_ranking(),
                "類股": pd.DataFrame([(self.sector_tree.item(i, "values")) for i in self.sector_tree.get_children()], columns=["產業", "檔數", "平均總分", "平均AI分", "代表股"]) if self.sector_tree.get_children() else pd.DataFrame(),
                "題材": pd.DataFrame([(self.theme_tree.item(i, "values")) for i in self.theme_tree.get_children()], columns=["題材", "檔數", "平均總分", "平均AI分", "代表股"]) if self.theme_tree.get_children() else pd.DataFrame(),
                "未分類清單": pd.read_excel(CLASSIFICATION_V2_UNCLASSIFIED_PATH) if CLASSIFICATION_V2_UNCLASSIFIED_PATH.exists() else pd.DataFrame(),
                "分類V2摘要": pd.DataFrame([get_classification_v2_summary()]) if get_classification_v2_summary() else pd.DataFrame(),
            }
            df = mapping.get(target, pd.DataFrame())
            if df is None:
                df = pd.DataFrame()
            if df.empty:
                empty_columns = {
                    "TOP20": ["stock_id", "stock_name", "price", "bucket", "ui_state", "liquidity_status", "liquidity_score", "elimination_reason", "entry_zone", "stop_loss", "target_1382", "target_1618", "rr", "win_rate"],
                    "TOP5": ["stock_id", "stock_name", "price", "ui_state", "liquidity_status", "liquidity_score", "entry_zone", "stop_loss", "target_1382", "rr", "win_rate", "backtest_win_rate", "cagr", "mdd"],
                    "今日可買": ["stock_id", "stock_name", "price", "ui_state", "liquidity_status", "liquidity_score", "entry_zone", "stop_loss", "target_1382", "target_1618", "rr", "win_rate"],
                    "等待拉回": ["stock_id", "stock_name", "price", "ui_state", "liquidity_status", "liquidity_score", "entry_zone", "stop_loss", "target_1382", "target_1618", "rr", "win_rate"],
                    "唯一決策": ["stock_id", "stock_name", "decision_bucket", "trade_action", "ui_state", "execution_ready", "position_bucket", "distribution_flag", "distance_to_entry_pct", "inst_status", "suggested_qty", "decision_reason"],
                    "預掛單": ["stock_id", "stock_name", "price", "ui_state", "liquidity_status", "liquidity_score", "entry_zone", "stop_loss", "target_1382", "target_1618", "rr", "win_rate"],
                    "下單清單": ["優先級", "代號", "名稱", "現價", "分類", "狀態", "盤中狀態", "活性分", "進場區", "停損", "1.382", "1.618", "RR", "勝率", "ATR%", "Kelly%", "建議張數", "建議金額", "單檔曝險%", "投資組合狀態", "風險備註"],
                    "機構交易計畫": ["優先級", "代號", "名稱", "現價", "市場", "產業", "題材", "分類", "狀態", "盤中狀態", "活性分", "進場區", "停損", "1.382", "1.618", "RR", "勝率", "模型分數", "交易分數", "ATR%", "Kelly%", "建議張數", "建議金額", "單檔曝險%", "題材曝險%", "產業曝險%", "投資組合狀態", "風險備註"],
                    "操作SOP": ["step", "module", "focus", "rule", "purpose", "output"],
                    "未分類清單": ["stock_id", "stock_name", "market", "industry_final", "theme_final", "sub_theme_final", "classification_source", "classification_confidence", "classification_note"],
                    "分類V2摘要": ["total", "official", "manual", "rule_engine", "ai_infer", "unclassified", "covered", "coverage_pct", "report_time", "unclassified_report"],
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
        ranking_raw = self._filtered_ranking()
        if ranking_raw is None or ranking_raw.empty:
            return messagebox.showwarning("提醒", "目前沒有可匯出的分析資料。")

        def worker():
            try:
                sector = pd.DataFrame()
                theme = pd.DataFrame()
                self.ui_call(self.start_task, "匯出分析", 5)
                self.ui_call(self.update_task, "匯出分析", 1, 5, item="整理排行")

                ranking = self.build_enriched_ranking_dataframe(ranking_raw, scope="export")
                if ranking is None or ranking.empty:
                    raise RuntimeError("排行資料整理失敗，無法匯出。")

                if "display_price" in ranking.columns:
                    ranking["price"] = ranking["display_price"]
                elif "現價" in ranking.columns and "price" not in ranking.columns:
                    ranking["price"] = ranking["現價"]
                elif "price" not in ranking.columns:
                    ranking["price"] = ranking.apply(
                        lambda r: self._get_stock_display_price(str(r.get("stock_id", "") or ""), r), axis=1
                    )

                preferred_cols = [
                    "rank_all", "stock_id", "stock_name", "price",
                    "market", "industry", "theme",
                    "momentum_score", "trend_score", "reversal_score",
                    "volume_score", "risk_score", "ai_score", "total_score",
                    "signal", "action", "rank_industry", "display_price", "現價"
                ]
                existing_cols = [c for c in preferred_cols if c in ranking.columns]
                other_cols = [c for c in ranking.columns if c not in existing_cols]
                ranking = ranking[existing_cols + other_cols].copy()

                if not ranking.empty:
                    sector = ranking.groupby("industry", as_index=False).agg(
                        count=("stock_id", "count"),
                        avg_total=("total_score", "mean"),
                        avg_ai=("ai_score", "mean")
                    ).sort_values(["avg_total", "avg_ai"], ascending=False)
                    if not sector.empty:
                        sector["top_name"] = sector["industry"].astype(str).map(
                            ranking.sort_values(["industry", "total_score", "ai_score"], ascending=[True, False, False]).drop_duplicates("industry").set_index("industry")["stock_name"].to_dict()
                        ).fillna("")
                    theme = ranking.groupby("theme", as_index=False).agg(
                        count=("stock_id", "count"),
                        avg_total=("total_score", "mean"),
                        avg_ai=("ai_score", "mean")
                    ).sort_values(["avg_total", "avg_ai"], ascending=False)
                    if not theme.empty:
                        theme["top_name"] = theme["theme"].astype(str).map(
                            ranking.sort_values(["theme", "total_score", "ai_score"], ascending=[True, False, False]).drop_duplicates("theme").set_index("theme")["stock_name"].to_dict()
                        ).fillna("")

                detail_text = self.detail.get("1.0", tk.END).strip()
                tables = {"Ranking": ranking}
                if not sector.empty:
                    tables["Sector"] = sector
                if not theme.empty:
                    tables["Theme"] = theme
                if self.last_top20_df is not None and not self.last_top20_df.empty:
                    tables["Trade_TOP20"] = self.last_top20_df
                if getattr(self, "last_mainstream_top20_df", pd.DataFrame()) is not None and not getattr(self, "last_mainstream_top20_df", pd.DataFrame()).empty:
                    tables["Mainstream_TOP20"] = self.last_mainstream_top20_df
                if getattr(self, "last_breakout_top20_df", pd.DataFrame()) is not None and not getattr(self, "last_breakout_top20_df", pd.DataFrame()).empty:
                    tables["Breakout_TOP20"] = self.last_breakout_top20_df
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
                if getattr(self, "last_execution_pool_df", pd.DataFrame()) is not None and not getattr(self, "last_execution_pool_df", pd.DataFrame()).empty:
                    tables["Execution_Pool"] = self.last_execution_pool_df
                if getattr(self, "last_final_decision_df", pd.DataFrame()) is not None and not getattr(self, "last_final_decision_df", pd.DataFrame()).empty:
                    tables["Final_Decision"] = self.last_final_decision_df
                tables["Order_List"] = self.last_order_list_df.copy() if self.last_order_list_df is not None and not self.last_order_list_df.empty else pd.DataFrame([{"message":"今日無可執行標的","reason":"請檢查 Final_Decision / execution_ready / suggested_qty"}])
                tables["Institutional_Plan"] = self.last_institutional_plan_df.copy() if getattr(self, "last_institutional_plan_df", pd.DataFrame()) is not None and not getattr(self, "last_institutional_plan_df", pd.DataFrame()).empty else pd.DataFrame([{"message":"今日無機構交易計畫","reason":"execution_pool 為空或過濾後無資料"}])
                cls_v2 = get_classification_v2_summary()
                if cls_v2:
                    tables["Classification_V2_Summary"] = pd.DataFrame([cls_v2])
                try:
                    if CLASSIFICATION_V2_UNCLASSIFIED_PATH.exists():
                        tables["Unclassified_Report"] = pd.read_excel(CLASSIFICATION_V2_UNCLASSIFIED_PATH)
                except Exception:
                    pass
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

    def build_order_list(self, execution_pool_df: pd.DataFrame | None = None, plan_df: pd.DataFrame | None = None) -> pd.DataFrame:
        execution_pool = execution_pool_df.copy() if execution_pool_df is not None else pd.DataFrame()
        self.last_execution_pool_df = execution_pool.copy()
        plan = plan_df.copy() if plan_df is not None else self.portfolio_engine.build_institutional_plan(execution_pool)
        empty_columns = ["優先級","代號","名稱","現價","分類","狀態","盤中狀態","活性分","進場區","停損","1.382","1.618","RR","勝率","ATR%","Kelly%","建議張數","建議金額","單檔曝險%","投資組合狀態","風險備註","唯一決策","決策原因","可執行","空表說明"]
        if plan.empty:
            self.last_institutional_plan_df = pd.DataFrame()
            return pd.DataFrame([{"優先級":0,"代號":"-","名稱":"今日無可執行標的","現價":"","分類":"-","狀態":"說明","盤中狀態":"-","活性分":"0","進場區":"-","停損":"-","1.382":"-","1.618":"-","RR":"0","勝率":"0","ATR%":"0","Kelly%":"0","建議張數":"0","建議金額":"0","單檔曝險%":"0","投資組合狀態":"空表","風險備註":"請先執行AI選股TOP20 / 機構交易計畫","唯一決策":"-","決策原因":"-","可執行":0,"空表說明":"機構交易計畫為空，因此下單清單沒有資料"}], columns=empty_columns)

        final_df = getattr(self, "last_final_decision_df", pd.DataFrame()).copy()
        if final_df is not None and not final_df.empty and "stock_id" in final_df.columns and "代號" in plan.columns:
            fd_cols = [c for c in ["stock_id", "decision_bucket", "decision_reason", "execution_ready", "risk_reason", "empty_reason", "strategy_layer_source"] if c in final_df.columns]
            fd = final_df[fd_cols].copy()
            fd["stock_id"] = fd["stock_id"].astype(str)
            plan = plan.copy()
            plan["代號"] = plan["代號"].astype(str)
            plan = plan.merge(fd, left_on="代號", right_on="stock_id", how="left")
            plan["decision_bucket"] = plan.get("decision_bucket", "排除").fillna("排除")
            plan["execution_ready"] = pd.to_numeric(plan.get("execution_ready", 0), errors="coerce").fillna(0).astype(int)
            filtered = plan[plan["execution_ready"] == 1].copy()
            if filtered.empty:
                self.last_institutional_plan_df = plan.copy()
                empty_reason = "所有股票未同時滿足 主攻 + execution_ready=1"
                if "empty_reason" in plan.columns:
                    candidates = [str(x).strip() for x in plan["empty_reason"].fillna("").tolist() if str(x).strip()]
                    if candidates:
                        empty_reason = "；".join(candidates[:5])
                return pd.DataFrame([{"優先級":0,"代號":"-","名稱":"今日無可執行標的","現價":"","分類":"-","狀態":"說明","盤中狀態":"-","活性分":"0","進場區":"-","停損":"-","1.382":"-","1.618":"-","RR":"0","勝率":"0","ATR%":"0","Kelly%":"0","建議張數":"0","建議金額":"0","單檔曝險%":"0","投資組合狀態":"空表","風險備註":empty_reason,"唯一決策":"-","決策原因":"請檢查 Final_Decision / Institutional_Plan","可執行":0,"空表說明":empty_reason}], columns=empty_columns)
            plan = filtered
        elif "盤中狀態" in plan.columns:
            plan = plan[plan["盤中狀態"].astype(str).eq("PASS")].copy()
        if plan.empty:
            self.last_institutional_plan_df = pd.DataFrame()
            return pd.DataFrame([{"優先級":0,"代號":"-","名稱":"今日無可執行標的","現價":"","分類":"-","狀態":"說明","盤中狀態":"-","活性分":"0","進場區":"-","停損":"-","1.382":"-","1.618":"-","RR":"0","勝率":"0","ATR%":"0","Kelly%":"0","建議張數":"0","建議金額":"0","單檔曝險%":"0","投資組合狀態":"空表","風險備註":"過濾後無 PASS 資料","唯一決策":"-","決策原因":"-","可執行":0,"空表說明":"請檢查盤中狀態 / suggested_qty"}], columns=empty_columns)
        order_df = pd.DataFrame({
            "優先級": plan["優先級"],
            "代號": plan["代號"],
            "名稱": plan["名稱"],
            "現價": plan["現價"] if "現價" in plan.columns else "",
            "分類": plan["分類"],
            "狀態": plan["狀態"],
            "盤中狀態": plan["盤中狀態"] if "盤中狀態" in plan.columns else "",
            "活性分": plan["活性分"] if "活性分" in plan.columns else 0,
            "進場區": plan["進場區"],
            "停損": plan["停損"],
            "1.382": plan["1.382"],
            "1.618": plan["1.618"],
            "RR": plan["RR"],
            "勝率": plan["勝率"],
            "ATR%": plan["ATR%"],
            "Kelly%": plan["Kelly%"],
            "建議張數": plan["建議張數"],
            "建議金額": plan["建議金額"],
            "單檔曝險%": plan["單檔曝險%"],
            "投資組合狀態": plan["投資組合狀態"],
            "風險備註": plan["風險備註"],
            "唯一決策": plan["decision_bucket"] if "decision_bucket" in plan.columns else "主攻",
            "決策原因": plan["decision_reason"] if "decision_reason" in plan.columns else "",
            "可執行": plan["execution_ready"] if "execution_ready" in plan.columns else 1,
            "空表說明": "",
        })
        self.last_institutional_plan_df = plan.copy()
        return order_df


    def refresh_top20_and_order_views(self):
        for tree in (self.top20_tree, self.top5_tree, self.order_tree, self.inst_tree, self.backtest_tree):
            for item in tree.get_children():
                tree.delete(item)

        if self.last_top20_df is not None and not self.last_top20_df.empty:
            for i, (_, r) in enumerate(self.last_top20_df.iterrows(), start=1):
                ui_action = str(r.get("ui_state", "不可買"))
                light = self._get_display_light(r.to_dict())
                self._insert_colored_row(self.top20_tree, values=(
                    i, r.get("stock_id", ""), r.get("stock_name", ""), self._display_light_symbol(light), r.get("candidate_engine", "混合"), self._get_stock_display_price(r.get("stock_id", ""), r), r.get("bucket", ""), r.get("operation_grade", "-"), ui_action,
                    r.get('orderbook_bias', '-'), f"{float(r.get('intraday_score', 0) or 0):.1f}", r.get("liquidity_status", ""), f"{float(r.get('liquidity_score', 0) or 0):.1f}",
                    r.get("entry_zone", "-"), r.get("stop_loss", "-"),
                    f"{float(r.get('target_1382', 0) or 0):.2f}", f"{float(r.get('target_1618', 0) or 0):.2f}",
                    f"{float(r.get('rr', 0) or 0):.2f}", f"{float(r.get('win_rate', 0) or 0):.1f}",
                    r.get("elimination_reason", "")
                ), light=light)

        if self.last_top5_df is not None and not self.last_top5_df.empty:
            for i, (_, r) in enumerate(self.last_top5_df.iterrows(), start=1):
                _light = self._get_display_light(r.to_dict())
                self._insert_colored_row(self.top5_tree, values=(
                    i, r.get("stock_id", ""), self._display_light_symbol(_light), r.get("stock_name", ""), self._get_stock_display_price(r.get("stock_id", ""), r), r.get("ui_state", "-"),
                    r.get('orderbook_bias', '-'), f"{float(r.get('intraday_score', 0) or 0):.1f}", r.get("liquidity_status", ""), f"{float(r.get('liquidity_score', 0) or 0):.1f}",
                    r.get("entry_zone", "-"), r.get("stop_loss", "-"),
                    f"{float(r.get('target_1382', 0) or 0):.2f}",
                    f"{float(r.get('rr', 0) or 0):.2f}",
                    f"{float(r.get('win_rate', 0) or 0):.1f}",
                    f"{float(r.get('backtest_win_rate', 0) or 0):.1f}",
                    f"{float(r.get('cagr', 0) or 0):.2f}",
                    f"{float(r.get('mdd', 0) or 0):.2f}"
                ), light=_light)


        if self.last_institutional_plan_df is not None and not self.last_institutional_plan_df.empty:
            for _, r in self.last_institutional_plan_df.iterrows():
                _inst_light = UITacticalPresenter.derive_signal_light(r.get("盤中狀態", ""), score=float(r.get('活性分', 0) or 0), intraday_score=float(r.get('活性分', 0) or 0))
                self._insert_colored_row(self.inst_tree, values=(
                    int(r.get("優先級", 0) or 0), r.get("代號", ""), r.get("名稱", ""), self._fmt_price(r.get("現價", "")), r.get("市場", ""),
                    r.get("產業", ""), r.get("題材", ""), r.get("分類", ""), r.get("狀態", ""),
                    r.get("盤中狀態", ""), f"{float(r.get('活性分', 0) or 0):.1f}",
                    r.get("進場區", "-"), r.get("停損", "-"),
                    f"{float(r.get('RR', 0) or 0):.2f}", f"{float(r.get('勝率', 0) or 0):.1f}",
                    f"{float(r.get('模型分數', 0) or 0):.2f}",
                    f"{float(r.get('交易分數', 0) or 0):.2f}",
                    f"{float(r.get('ATR%', 0) or 0):.2f}",
                    f"{float(r.get('Kelly%', 0) or 0):.2f}",
                    (f"{float(r.get('建議張數', 0) or 0):.1f}".rstrip('0').rstrip('.') if pd.notna(r.get('建議張數', 0)) else "0"),
                    f"{float(r.get('建議金額', 0) or 0):.0f}",
                    f"{float(r.get('單檔曝險%', 0) or 0):.2f}",
                    f"{float(r.get('題材曝險%', 0) or 0):.2f}",
                    f"{float(r.get('產業曝險%', 0) or 0):.2f}",
                    r.get("投資組合狀態", "")
                ), light=_inst_light)

        if self.last_order_list_df is not None and not self.last_order_list_df.empty:
            for _, r in self.last_order_list_df.iterrows():
                _order_light = UITacticalPresenter.derive_signal_light(r.get("盤中狀態", ""), score=float(r.get('活性分', 0) or 0), intraday_score=float(r.get('活性分', 0) or 0))
                self._insert_colored_row(self.order_tree, values=(
                    int(r.get("優先級", 0) or 0), r.get("代號", ""), r.get("名稱", ""), self._fmt_price(r.get("現價", "")), r.get("分類", ""),
                    r.get("狀態", ""), r.get("盤中狀態", ""), f"{float(r.get('活性分', 0) or 0):.1f}",
                    r.get("進場區", "-"), r.get("停損", "-"),
                    r.get("1.382", "-"), r.get("1.618", "-"),
                    f"{float(r.get('RR', 0) or 0):.2f}", f"{float(r.get('勝率', 0) or 0):.1f}",
                    f"{float(r.get('ATR%', 0) or 0):.2f}", f"{float(r.get('Kelly%', 0) or 0):.2f}",
                    (f"{float(r.get('建議張數', 0) or 0):.1f}".rstrip('0').rstrip('.') if pd.notna(r.get('建議張數', 0)) else "0"),
                    f"{float(r.get('建議金額', 0) or 0):.0f}", f"{float(r.get('單檔曝險%', 0) or 0):.2f}",
                    r.get("投資組合狀態", ""), r.get("風險備註", ""), r.get("空表說明", r.get("決策原因", ""))
                ), light=_order_light)

        self.sync_multi_windows()
        if self.window_current_stock_id and self.current_chart_path and Path(self.current_chart_path).exists():
            self.update_multi_window_stock(self.window_current_stock_id, chart_path=str(self.current_chart_path))

    def on_select_top20(self, event=None):
        sel = self.top20_tree.selection()
        if not sel:
            return
        vals = self.top20_tree.item(sel[0], "values")
        stock_id = self._resolve_stock_id_from_tree_values(vals, "AI交易TOP20")
        if not stock_id:
            self.append_log(f"雙引擎TOP20 選取失敗：無法解析 stock_id｜values={vals}", "WARNING")
            return
        if self._should_ignore_select_event(stock_id, "AI交易TOP20"):
            return
        self.sync_all_views(stock_id, source="AI交易TOP20")
    def on_select_order(self, event=None):
        sel = self.order_tree.selection()
        if not sel:
            return
        vals = self.order_tree.item(sel[0], "values")
        stock_id = self._resolve_stock_id_from_tree_values(vals, "下單清單")
        if not stock_id:
            return
        if self._should_ignore_select_event(stock_id, "下單清單"):
            return
        self.sync_all_views(stock_id, source="下單清單")


    def _show_theme_detail_summary(self):
        theme_df = getattr(self, "last_theme_summary_df", pd.DataFrame())
        lines = ["《題材輪動摘要》", ""]
        if theme_df is None or theme_df.empty:
            lines.append("目前沒有題材摘要資料。請先執行 AI選股TOP20 或重建排行。")
        else:
            lines.append("用途：先看題材，再看個股，不要只看單一股票分數。")
            lines.append("")
            for i, (_, r) in enumerate(theme_df.head(10).iterrows(), start=1):
                lines.append(f"{i}. {r.get('theme','-')}｜檔數 {int(r.get('count',0) or 0)}｜熱度 {float(r.get('hot_score',0) or 0):.2f}｜平均總分 {float(r.get('avg_total',0) or 0):.2f}")
        self.detail.delete("1.0", tk.END)
        self.detail.insert("1.0", "\n".join(lines))

    def _show_order_detail_summary(self):
        order_df = getattr(self, "last_order_list_df", pd.DataFrame())
        lines = ["《下單清單摘要》", ""]
        if order_df is None or order_df.empty:
            lines.append("今日無下單清單資料。")
            lines.append("原因：尚未執行機構交易計畫，或 Final_Decision 尚未完成收斂。")
        else:
            first_name = str(order_df.iloc[0].get("名稱", "") or "")
            first_sid = normalize_stock_id(order_df.iloc[0].get("代號", ""))
            if first_name == "今日無可執行標的" or not first_sid:
                lines.append("今日無 execution_ready=1 的標的。")
                lines.append(f"原因：{order_df.iloc[0].get('空表說明','-')}")
            else:
                lines.append("用途：這裡只保留 execution_ready=1 的最終可下單名單。")
                lines.append("")
                for i, (_, r) in enumerate(order_df.head(10).iterrows(), start=1):
                    lines.append(f"{i}. {r.get('代號','-')} {r.get('名稱','-')}｜狀態 {r.get('狀態','-')}｜進場 {r.get('進場區','-')}｜RR {float(r.get('RR',0) or 0):.2f}")
        self.detail.delete("1.0", tk.END)
        self.detail.insert("1.0", "\n".join(lines))

    def on_left_tab_changed(self, event=None):
        try:
            current = self.left_notebook.select()
            current_text = self.left_notebook.tab(current, "text")
        except Exception:
            return
        if current_text in ("題材輪動",):
            self._show_theme_detail_summary()
        elif current_text in ("下單清單",):
            self._show_order_detail_summary()

    def show_top5(self):
        def render_top5():
            self.refresh_top20_and_order_views()
            self.left_notebook.select(self.tab_top5)
            lines = ["《v9.2 FINAL-RELEASE V3.5｜AI選股TOP5》", "用途：這裡是核心觀察名單，不代表全部都要買；先看狀態與進場區，再決定是否列入下單清單。", ""]
            for i, (_, r) in enumerate(self.last_top5_df.iterrows(), start=1):
                lines.append(
                    f"{i}. {r['stock_id']} {r['stock_name']}｜{r.get('ui_state','-')}｜進場 {r.get('entry_zone','-')}｜RR {float(r.get('rr',0) or 0):.2f}｜勝率 {float(r.get('win_rate',0) or 0):.1f}%｜回測 {float(r.get('backtest_win_rate',0) or 0):.1f}%"
                )
            self.detail.delete("1.0", tk.END)
            self.detail.insert("1.0", "\n".join(lines))

        if self.last_top5_df is not None and not self.last_top5_df.empty:
            return render_top5()

        # 若 TOP20 尚未建立，先觸發背景分析，再等待結果
        if self.worker is not None and self.worker.is_alive():
            self.set_status("背景分析進行中，等待 TOP5 結果…")
            self.root.after(600, self.show_top5)
            return

        self.show_top20()

        def wait_for_top5(retry=0):
            if self.last_top5_df is not None and not self.last_top5_df.empty:
                render_top5()
                return
            if self.worker is not None and self.worker.is_alive() and retry < 20:
                self.set_status(f"等待 TOP5 結果…({retry+1}/20)")
                self.root.after(600, lambda: wait_for_top5(retry + 1))
                return
            messagebox.showwarning("提醒", "目前尚無可用 TOP5 資料，請先執行 AI選股TOP20。")

        self.root.after(600, lambda: wait_for_top5(0))

    def on_select_top5(self, event=None):
        sel = self.top5_tree.selection()
        if not sel:
            return
        vals = self.top5_tree.item(sel[0], "values")
        stock_id = self._resolve_stock_id_from_tree_values(vals, "AI選股TOP5")
        if not stock_id:
            return
        if self._should_ignore_select_event(stock_id, "AI選股TOP5"):
            return
        self.sync_all_views(stock_id, source="AI選股TOP5")


    def on_select_institutional(self, event=None):
        sel = self.inst_tree.selection()
        if not sel:
            return
        vals = self.inst_tree.item(sel[0], "values")
        stock_id = self._resolve_stock_id_from_tree_values(vals, "機構交易計畫")
        if not stock_id:
            return
        if self._should_ignore_select_event(stock_id, "機構交易計畫"):
            return
        self.sync_all_views(stock_id, source="機構交易計畫")

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
                progress_every = 25
                status_every = 25
                log_every = 25

                def throttled_log(msg: str):
                    text_msg = str(msg or "")
                    emit = False
                    if "分批節點" in text_msg or "完成" in text_msg or "錯誤" in text_msg or "失敗" in text_msg:
                        emit = True
                    else:
                        m = re.search(r"\[(\d+)/(\d+)\]", text_msg)
                        if m:
                            idx_num = int(m.group(1))
                            total_num = int(m.group(2))
                            emit = (idx_num == 1) or (idx_num == total_num) or (idx_num % log_every == 0)
                    if emit:
                        self.ui_call(self.append_log, text_msg)

                def progress(idx, total_count, sid, existing_count, flag):
                    if flag in ("fail", "error"):
                        counters["fail"] += 1
                    elif flag == "ok":
                        counters["ok"] += 1

                    if idx == 1 or idx == total_count or idx % progress_every == 0 or flag in ("fail", "error"):
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
                    if idx == 1 or idx == total_count or idx % status_every == 0 or flag in ("fail", "error"):
                        self.ui_call(self.set_status, f"建立歷史中 {idx}/{total_count}｜{sid}｜成功 {counters['ok']}｜失敗 {counters['fail']}")

                success, failed, rows = self.data_engine.build_full_history(
                    batch_size=self.history_batch_size,
                    sleep_sec=self.history_sleep_sec,
                    progress_cb=progress,
                    log_cb=throttled_log,
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
                    self.ui_call(self.refresh_classification_summary_ui)
                    self.ui_call(self.update_task, "初始化全市場", 4, 4, success=1, item="完成")
                    self.ui_call(self.set_status, f"已改用本地主檔，共 {len(master2)} 檔。")
                    self.ui_call(messagebox.showinfo, "完成", f"全市場抓取失敗，已改用本地主檔\n共 {len(master2)} 檔\n\n使用主檔：{csv_path}")
                    return
                self.db.import_master_df(universe)
                master2 = self.db.get_master()
                self.ui_call(self.refresh_filters)
                self.ui_call(self.refresh_all_tables)
                self.ui_call(self.refresh_classification_summary_ui)
                self.ui_call(self.update_task, "初始化全市場", 4, 4, success=1, item="完成")
                self.ui_call(self.set_status, f"全市場初始化完成，共 {len(master2)} 檔。")
                self.ui_call(messagebox.showinfo, "完成", f"全市場股票清單初始化完成\n共 {len(master2)} 檔")
            except Exception as e:
                traceback.print_exc()
                self.ui_call(messagebox.showerror, "錯誤", f"初始化失敗：\n{e}")

        self._run_in_thread(worker, "init_market")

    def populate_operation_sop(self, market: dict, trade_top20: pd.DataFrame, today_buy: pd.DataFrame, wait_pullback: pd.DataFrame, attack: pd.DataFrame, defense: pd.DataFrame):
        for item in self.sop_tree.get_children():
            self.sop_tree.delete(item)
        sop_df = OperationGuideEngine.build_playbook(market, trade_top20, today_buy, wait_pullback, attack, defense)
        self.last_operation_sop_df = sop_df.copy()
        if sop_df.empty:
            return
        for _, r in sop_df.iterrows():
            self.sop_tree.insert("", "end", values=(r["step"], r["module"], r["focus"], r["rule"], r["purpose"], r["output"]))

    def show_operation_guide(self):
        ranking = self._filtered_ranking()
        trade_top20 = getattr(self, "last_top20_df", pd.DataFrame())
        today_buy = getattr(self, "last_today_buy_df", pd.DataFrame())
        wait_pullback = getattr(self, "last_wait_df", pd.DataFrame())
        attack = getattr(self, "last_attack_df", pd.DataFrame())
        defense = getattr(self, "last_defense_df", pd.DataFrame())
        market = self.master_trading_engine.market_engine.get_market_regime()
        self.populate_operation_sop(market, trade_top20, today_buy, wait_pullback, attack, defense)
        lines = [
            "《V3.5 操作版｜功能與用途》",
            "",
            f"市場狀態：{market['regime']}（{market['score']:.2f}）",
            f"市場說明：{market['memo']}",
            "",
            "核心流程：先看市場 → 再看輪動 → 再看今日可買 / 預掛單 → 再看下單清單 → 最後用回測驗證。",
            "",
            "五種狀態怎麼用：",
        ]
        for state in ["可買", "準備買", "預掛單", "觀察", "不可買"]:
            lines.append(f"- {state}：{OperationGuideEngine.explain_state(state)}")
        if trade_top20 is not None and not trade_top20.empty:
            lines.extend(["", "目前 TOP20 前3檔："])
            for i, (_, r) in enumerate(trade_top20.head(3).iterrows(), start=1):
                lines.append(f"{i}. {r['stock_id']} {r['stock_name']}｜{r.get('ui_state','-')}｜{r.get('entry_zone','-')}｜RR {float(r.get('rr',0) or 0):.2f}")
        elif ranking is not None and not ranking.empty:
            lines.extend(["", "尚未建立 TOP20，請先執行 AI選股TOP20。", f"目前排行第一：{ranking.iloc[0]['stock_id']} {ranking.iloc[0]['stock_name']}"])
        self.detail.delete("1.0", tk.END)
        self.detail.insert("1.0", "\n".join(lines))
        self.left_notebook.select(self.tab_sop)

    def refresh_filters(self, force: bool = False):
        master = self.db.get_master()
        if master.empty:
            self.market_cb["values"] = ["全部"]
            self.industry_cb["values"] = ["全部"]
            self.theme_cb["values"] = ["全部"]
            self.last_filter_signature = ((), (), ())
            return
        markets = tuple(sorted([x for x in master["market"].dropna().unique().tolist() if str(x).strip() != ""]))
        industries = tuple(sorted([x for x in master["industry"].dropna().unique().tolist() if str(x).strip() != ""]))
        themes = tuple(sorted([x for x in master["theme"].dropna().unique().tolist() if str(x).strip() != ""]))
        signature = (markets, industries, themes)
        if (not force) and signature == self.last_filter_signature:
            return
        self.market_cb["values"] = ["全部"] + list(markets)
        self.industry_cb["values"] = ["全部"] + list(industries)
        self.theme_cb["values"] = ["全部"] + list(themes)
        self.last_filter_signature = signature

    def _parse_search_tokens(self, raw_query: str):
        raw = str(raw_query or "").strip()
        if not raw:
            return []
        normalized = raw.replace("，", ",").replace("、", ",").replace("；", ",").replace(";", ",")
        parts = [p.strip() for p in normalized.split(",")]
        return [p for p in parts if p]

    def _apply_search_filter(self, df: pd.DataFrame, raw_query: str) -> pd.DataFrame:
        tokens = self._parse_search_tokens(raw_query)
        if not tokens or df is None or df.empty:
            return df

        stock_id_series = df["stock_id"].astype(str)
        stock_name_series = df["stock_name"].astype(str)
        mask = pd.Series(False, index=df.index)

        for token in tokens:
            token_mask = pd.Series(False, index=df.index)
            escaped = re.escape(token)
            if token.isdigit():
                token_mask = token_mask | stock_id_series.eq(token)
                token_mask = token_mask | stock_id_series.str.contains(escaped, case=False, na=False, regex=True)
            else:
                token_mask = token_mask | stock_id_series.str.contains(escaped, case=False, na=False, regex=True)
            token_mask = token_mask | stock_name_series.str.contains(escaped, case=False, na=False, regex=True)
            mask = mask | token_mask

        return df[mask]

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
            df = self._apply_search_filter(df, q)
        return df.sort_values(["rank_all"]).reset_index(drop=True)

    def refresh_ranking_table_only(self):
        for tree in (self.rank_tree, self.sector_tree, self.theme_tree):
            for item in tree.get_children():
                tree.delete(item)
        df = self._filtered_ranking()
        if df is None or df.empty:
            return pd.DataFrame()
        enriched = self.build_enriched_ranking_dataframe(df, scope="ui")
        for i, row in enriched.iterrows():
            _rank_light = UITacticalPresenter.derive_signal_light(row.get("signal", ""), score=float(row.get('total_score', 0) or 0), intraday_score=float(row.get('intraday_score', 0) or 0))
            self._insert_colored_row(self.rank_tree, values=(
                i + 1, row["stock_id"], row["stock_name"], row.get("display_price", self._get_stock_display_price(row["stock_id"], row)), row["industry"], row["theme"],
                f"{row['total_score']:.2f}", f"{row['ai_score']:.2f}", row["signal"], row["action"]
            ), light=_rank_light)
        sector = (
            enriched.groupby("industry", as_index=False)
            .agg(count=("stock_id", "count"), avg_total=("total_score", "mean"), avg_ai=("ai_score", "mean"))
            .sort_values(["avg_total", "avg_ai"], ascending=False)
        )
        for _, r in sector.iterrows():
            top_name = enriched[enriched["industry"] == r["industry"]].sort_values("total_score", ascending=False).iloc[0]["stock_name"]
            self.sector_tree.insert("", "end", values=(r["industry"], int(r["count"]), f"{r['avg_total']:.2f}", f"{r['avg_ai']:.2f}", top_name))
        theme = (
            enriched.groupby("theme", as_index=False)
            .agg(count=("stock_id", "count"), avg_total=("total_score", "mean"), avg_ai=("ai_score", "mean"))
            .sort_values(["avg_total", "avg_ai"], ascending=False)
        )
        for _, r in theme.iterrows():
            top_name = enriched[enriched["theme"] == r["theme"]].sort_values("total_score", ascending=False).iloc[0]["stock_name"]
            self.theme_tree.insert("", "end", values=(r["theme"], int(r["count"]), f"{r['avg_total']:.2f}", f"{r['avg_ai']:.2f}", top_name))
        return enriched

    def refresh_trade_summary_only(self):
        for tree in (self.dashboard_tree, self.rotation_tree):
            for item in tree.get_children():
                tree.delete(item)
        df = self._filtered_ranking()
        if df is None or df.empty:
            return
        market_engine = self.master_trading_engine.market_engine
        regime = market_engine.get_market_regime()
        rotation = IndustryRotationEngine.summarize(df)
        dash_rows = [
            ("市場狀態", regime["regime"], f"Regime score {regime['score']:.2f}"),
            ("市場廣度", f"{regime['breadth']:.1f}", "強勢訊號占比"),
            ("排行檔數", str(len(df)), "目前篩選後股票數"),
            ("最強題材", str(df.groupby("theme")["total_score"].mean().sort_values(ascending=False).index[0]) if not df.empty else "-", "依平均總分"),
            ("最強產業", str(rotation.iloc[0]["industry"]) if not rotation.empty else "-", "依輪動分"),
        ]
        for m, v, d in dash_rows:
            self.dashboard_tree.insert("", "end", values=(m, v, d))
        for _, r in rotation.iterrows():
            self.rotation_tree.insert("", "end", values=(r["industry"], int(r["count"]), f"{r['avg_total']:.2f}", f"{r['avg_ai']:.2f}", int(r["trend_count"]), f"{r['hot_score']:.2f}", r["rotation"]))

    def refresh_optional_views_lazy(self):
        try:
            if (self.last_top20_df is not None and not self.last_top20_df.empty) or (self.last_order_list_df is not None and not self.last_order_list_df.empty):
                self.refresh_top20_and_order_views()
        except Exception:
            pass
        try:
            self.refresh_classification_summary_ui()
        except Exception:
            pass

    def _finish_rebuild_rank_fast(self, count: int, stage_name: str = "重建排行"):
        try:
            self.refresh_ranking_table_only()
        except Exception:
            pass
        try:
            self.refresh_trade_summary_only()
        except Exception:
            pass
        self.finish_task(stage_name, f"排行已完成，共 {count} 檔")
        self.append_log(f"{stage_name}已完成，共 {count} 檔；其餘頁面將延後刷新。")
        self.root.after(200, self._post_refresh_after_rank)

    def _post_refresh_after_rank(self):
        self.refresh_trade_summary_only()
        self.root.after(200, self.refresh_optional_views_lazy)

    def refresh_all_tables(self):
        if not self.ensure_ranking_ready(auto_rebuild=True):
            price_rows = self.db.get_total_price_rows()
            if price_rows > 0:
                self.set_status("已有歷史資料，但尚未形成有效排行；請先補足歷史或重建排行。")
            else:
                self.set_status("目前尚無排行資料，請先初始化、建立歷史，再重建排行。")
            self.show_welcome_message()
            return
        df = self.refresh_ranking_table_only()
        if df is None or df.empty:
            self.set_status("目前篩選條件下沒有資料。")
            return
        self.refresh_trade_summary_only()
        try:
            trade = self.master_trading_engine.get_trade_pool(df.head(200))
            self.populate_operation_sop(trade["market"], trade["trade_top20"], trade["today_buy"], trade["wait_pullback"], trade["attack"], trade["defense"])
            attack_cnt = len(trade["attack"])
            defense_cnt = len(trade["defense"])
            self.set_status(f"已載入資料，共 {len(df)} 檔｜市場 {trade['market']['regime']}｜主攻 {attack_cnt}｜防守 {defense_cnt}")
        except Exception:
            self.set_status(f"已載入資料，共 {len(df)} 檔")
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
                self.ui_call(self.refresh_filters, False)
                self.ui_call(self._finish_rebuild_rank_fast, rank_count, "每日增量更新")
                self.ui_call(self.show_welcome_message)
                self.ui_call(self.append_log, f"每日增量更新完成｜成功 {success} 檔｜寫入 {rows} 筆｜排行 {rank_count} 檔")
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
                self.ui_call(self.refresh_filters, False)
                self.ui_call(self._finish_rebuild_rank_fast, count, "重建排行")
                if count <= 0:
                    self.ui_call(messagebox.showwarning, "提醒", "排行重建完成，但目前可計算檔數為 0。\n請先建立至少 70 根以上歷史K線資料。")
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
                candidate_df = df.head(300).copy()
                total = len(candidate_df)
                self.ui_call(self.clear_log)
                self.ui_call(self.start_task, "AI選股TOP20", total)

                def progress(idx, total_count, sid):
                    self.ui_call(self.update_task, "AI選股TOP20", idx, total_count, idx, 0, 0, sid)

                trade = self.master_trading_engine.get_trade_pool(
                    candidate_df,
                    progress_cb=progress,
                    log_cb=lambda msg: self.ui_call(self.append_log, msg),
                    cancel_cb=lambda: self.cancel_event.is_set(),
                )
                market = trade["market"]
                trade_top20 = self.enrich_trade_dataframe_with_tactical(trade["trade_top20"])
                if trade_top20 is not None and not trade_top20.empty and 'tactical_light' in trade_top20.columns:
                    allowed = trade_top20[trade_top20['tactical_light'].astype(str).isin(['🔵','🟢','🟡','🟠'])].copy()
                    if not allowed.empty:
                        light_order = {'🔵':0, '🟢':1, '🟡':2, '🟠':3}
                        allowed['_light_order'] = allowed['tactical_light'].astype(str).map(light_order).fillna(9)
                        allowed = allowed.sort_values(['_light_order','selection_score','trade_score'], ascending=[True,False,False]).drop(columns=['_light_order'])
                        trade_top20 = allowed.head(20).copy()
                mainstream_top20 = self.enrich_trade_dataframe_with_tactical(trade.get("mainstream_top20", pd.DataFrame()))
                breakout_top20 = self.enrich_trade_dataframe_with_tactical(trade.get("breakout_top20", pd.DataFrame()))
                attack = self.enrich_trade_dataframe_with_tactical(trade["attack"])
                today_buy = self.enrich_trade_dataframe_with_tactical(trade["today_buy"])
                wait_pullback = self.enrich_trade_dataframe_with_tactical(trade["wait_pullback"])
                execution_pool = self.enrich_trade_dataframe_with_tactical(trade.get("execution_pool", pd.DataFrame()))
                watch = trade["watch"].copy()
                defense = trade["defense"].copy()
                theme_summary = trade["theme_summary"]
                eliminated = trade.get("eliminated", pd.DataFrame())

                self.last_top20_df = trade_top20.copy()
                self.last_mainstream_top20_df = mainstream_top20.copy()
                self.last_breakout_top20_df = breakout_top20.copy()
                self.cache_trade_dataframe(self.last_top20_df)
                self.last_top5_df = pd.DataFrame()
                self.last_attack_df = attack.copy()
                self.cache_trade_dataframe(self.last_attack_df)
                self.last_watch_df = watch.copy()
                self.cache_trade_dataframe(self.last_watch_df)
                self.last_defense_df = defense.copy()
                self.cache_trade_dataframe(self.last_defense_df)
                self.last_theme_summary_df = theme_summary.copy()
                self.last_today_buy_df = today_buy.copy()
                self.cache_trade_dataframe(self.last_today_buy_df)
                self.last_wait_df = wait_pullback.copy()
                self.cache_trade_dataframe(self.last_wait_df)
                self.last_execution_pool_df = execution_pool.copy()
                institutional_plan = self.portfolio_engine.build_institutional_plan(execution_pool)
                self.last_institutional_plan_df = institutional_plan.copy()
                final_decision = FinalDecisionEngine.finalize(trade_top20, today_buy, wait_pullback, defense, institutional_plan, theme_summary)
                self.last_final_decision_df = final_decision.copy()

                top5_source = final_decision[(final_decision["decision_bucket"] == "主攻") & (final_decision["trade_action"].astype(str).str.upper().isin(["BUY", "WEAK BUY"]))].copy() if final_decision is not None and not final_decision.empty else pd.DataFrame()
                if not top5_source.empty:
                    top5_source["top5_source"] = "Final主攻"
                if len(top5_source) < 5 and final_decision is not None and not final_decision.empty:
                    supplement = final_decision[final_decision["decision_bucket"] == "等待拉回"].copy()
                    if not supplement.empty:
                        supplement["top5_source"] = "Wait fallback"
                        top5_source = pd.concat([top5_source, supplement], ignore_index=True)
                top5_source = top5_source.head(5).copy() if not top5_source.empty else top5_source
                if not top5_source.empty:
                    bt_rows = []
                    for _, rr in top5_source.iterrows():
                        bt = self.backtest_engine.estimate_trade_quality(str(rr["stock_id"]))
                        bt_rows.append(bt)
                    bt_df = pd.DataFrame(bt_rows)
                    top5_source = pd.concat([top5_source.reset_index(drop=True), bt_df.reset_index(drop=True)], axis=1)
                self.last_top5_df = top5_source.copy()
                self.cache_trade_dataframe(self.last_top5_df)
                self.cache_backtest_dataframe(self.last_top5_df)

                self.last_order_list_df = self.build_order_list(execution_pool, institutional_plan)

                self.ui_call(self.populate_operation_sop, market, trade_top20, today_buy, wait_pullback, attack, defense)
                self.ui_call(self.refresh_top20_and_order_views)
                self.ui_call(self.left_notebook.select, self.tab_top20)
                self.ui_call(self.open_three_windows)

                defend_cnt = int(trade_top20["bucket"].eq("防守").sum()) if not trade_top20.empty else 0
                eliminated_cnt = len(eliminated) if eliminated is not None else 0
                lines = [
                    "《v9.2 FINAL-RELEASE V3.5 操作版》",
                    f"市場判斷：{market['regime']}（{market['score']:.2f}）｜市場廣度 {market['breadth']:.1f}",
                    f"市場說明：{market['memo']}",
                    f"TOP20 觀察池：{len(trade_top20)} 檔｜今日可買：{len(today_buy)}｜預掛單：{len(wait_pullback)}｜防守：{defend_cnt}｜淘汰：{eliminated_cnt}",
                    f"交易門檻：決策 BUY / WEAK BUY｜支撐 > 0｜壓力 > 支撐｜再依六模組總分排序",
                    "操作用途：先看今日可買，再看預掛單，沒有進場區就不下單。",
                    "",
                    "【TOP20 觀察池 前5檔】",
                ]
                if trade_top20.empty:
                    lines.append("目前無符合條件標的")
                else:
                    for i, (_, r) in enumerate(trade_top20.head(5).iterrows(), start=1):
                        lines.append(f"{i}. {r['stock_id']} {r['stock_name']}｜{r.get('tactical_light','⚪')} {r.get('orderbook_bias','-')}｜盤中分 {float(r.get('intraday_score',0) or 0):.1f}｜{r['bucket']}｜{r.get('liquidity_status','-')} {float(r.get('liquidity_score',0) or 0):.1f}｜{r['trade_action']}｜RR {r['rr']:.2f}｜勝率 {r['win_rate']:.1f}%")

                lines.extend(["", "【今日可買】"])
                if today_buy.empty:
                    lines.append("今日無符合 SOP 的可買名單（允許空白，不為湊數放寬）。")
                else:
                    for i, (_, r) in enumerate(today_buy.iterrows(), start=1):
                        lines.append(f"{i}. {r['stock_id']} {r['stock_name']}｜{r.get('tactical_light','⚪')} {r.get('orderbook_bias','-')}｜盤中分 {float(r.get('intraday_score',0) or 0):.1f}｜{r['trade_action']}｜RR {r['rr']:.2f}｜勝率 {r['win_rate']:.1f}%｜{r['entry_zone']}")

                lines.extend(["", "【預掛單】"])
                if wait_pullback.empty:
                    lines.append("無預掛單")
                else:
                    for i, (_, r) in enumerate(wait_pullback.iterrows(), start=1):
                        lines.append(f"{i}. {r['stock_id']} {r['stock_name']}｜{r.get('tactical_light','⚪')} {r.get('orderbook_bias','-')}｜盤中分 {float(r.get('intraday_score',0) or 0):.1f}｜預掛單｜RR {r['rr']:.2f}｜勝率 {r['win_rate']:.1f}%｜{r['entry_zone']}")

                self.ui_call(self.detail.delete, "1.0", tk.END)
                self.ui_call(self.detail.insert, "1.0", "\n".join(lines))
                self.ui_call(self.finish_task, "AI選股TOP20", f"AI選股完成：候選 {len(candidate_df)}｜TOP20 {len(trade_top20)}｜今日可買 {len(today_buy)}｜等待 {len(wait_pullback)}")
            except OperationCancelled:
                self.ui_call(self.finish_task, "AI選股TOP20", "AI選股TOP20 已中斷")
            except Exception as e:
                traceback.print_exc()
                self.ui_call(messagebox.showerror, "錯誤", str(e))

        self._run_in_thread(worker, "show_top20")


    def show_strategy_backtest(self):
        if self.last_top20_df is None or self.last_top20_df.empty:
            return messagebox.showwarning("提醒", "請先執行 AI選股TOP20。")

        rows = []
        for _, r in self.last_top20_df.head(10).iterrows():
            stock_id = str(r["stock_id"])
            bt = self.backtest_engine.estimate_trade_quality(stock_id)
            price = self._get_stock_display_price(stock_id, r)
            rows.append({
                "stock_id": stock_id,
                "stock_name": r["stock_name"],
                "price": price,
                "backtest_win_rate": bt.get("backtest_win_rate", 0),
                "avg_return": bt.get("avg_return", 0),
                "cagr": bt.get("cagr", 0),
                "mdd": bt.get("mdd", 0),
                "sharpe": bt.get("sharpe", 0),
                "samples": bt.get("samples", 0),
            })

        out = pd.DataFrame(rows).sort_values(["backtest_win_rate", "cagr", "sharpe"], ascending=False).reset_index(drop=True)

        for item in self.backtest_tree.get_children():
            self.backtest_tree.delete(item)

        expected_cols = 10
        for i, (_, r) in enumerate(out.iterrows(), start=1):
            values = (
                i,
                r["stock_id"],
                r["stock_name"],
                r["price"],
                f"{float(r['backtest_win_rate']):.1f}",
                f"{float(r['avg_return']):.2f}",
                f"{float(r['cagr']):.2f}",
                f"{float(r['mdd']):.2f}",
                f"{float(r['sharpe']):.2f}",
                int(r["samples"]),
            )
            assert len(values) == expected_cols, "backtest_tree 欄位數與寫入值數量不一致"
            self.backtest_tree.insert("", "end", values=values)

        lines = ["《v9.2 FINAL-RELEASE 策略回測摘要》", ""]
        for i, (_, r) in enumerate(out.iterrows(), start=1):
            lines.append(
                f"{i}. {r['stock_id']} {r['stock_name']}｜現價 {r['price']}｜勝率 {float(r['backtest_win_rate']):.1f}%｜CAGR {float(r['cagr']):.2f}%｜MDD {float(r['mdd']):.2f}%｜Sharpe {float(r['sharpe']):.2f}｜樣本 {int(r['samples'])}"
            )
        self.detail.delete("1.0", tk.END)
        self.detail.insert("1.0", "\n".join(lines))
        self.left_notebook.select(self.tab_backtest)


    def export_equity_curve_chart(self, stock_id: str, hist: pd.DataFrame):
        bt = self.backtest_engine.estimate_trade_quality(stock_id)
        trades = self.backtest_engine.simulate_trades(stock_id)
        if trades.empty:
            return None
        eq = (1 + trades["ret"].astype(float)).cumprod()
        fig = plt.figure(figsize=(10, 5))
        ax = fig.add_subplot(111)
        ax.plot(eq.index + 1, eq.values)
        ax.set_title(f"{stock_id} Equity Curve | CAGR {bt.get('cagr',0):.2f}% | MDD {bt.get('mdd',0):.2f}%")
        ax.set_xlabel("Trade #")
        ax.set_ylabel("Equity")
        out = CHART_DIR / f"{stock_id}_equity_curve.png"
        fig.savefig(out, dpi=140, bbox_inches="tight")
        plt.close(fig)
        log_info(f"export_chart done：{stock_id}｜{out}")
        return out

    def on_select_backtest(self, event=None):
        sel = self.backtest_tree.selection()
        if not sel:
            return
        vals = self.backtest_tree.item(sel[0], "values")
        stock_id = str(vals[1]).strip()
        if not stock_id:
            return
        if self._should_ignore_select_event(stock_id, "回測視覺化"):
            return
        self.start_backtest_selection_job(stock_id)

    def on_select_stock(self, event=None):
        sel = self.rank_tree.selection()
        if not sel:
            return
        vals = self.rank_tree.item(sel[0], "values")
        stock_id = self._resolve_stock_id_from_tree_values(vals, "排行榜")
        if not stock_id:
            return
        if self._should_ignore_select_event(stock_id, "排行榜"):
            return
        self.sync_all_views(stock_id, source="排行榜")


    def export_chart(self, stock_id: str, hist: pd.DataFrame):
        log_info(f"export_chart start：{stock_id}")
        x = DataEngine.attach(hist.copy()).tail(120).reset_index(drop=True)
        fig = plt.figure(figsize=(11, 5.8))
        ax = fig.add_subplot(111)
        xs = list(range(len(x)))
        self._candlestick(ax, xs, x["open"], x["high"], x["low"], x["close"])
        ax.plot(xs, x["ma20"], label="MA20", linewidth=1.2)
        ax.plot(xs, x["ma60"], label="MA60", linewidth=1.2)

        plan = self.get_cached_trade_plan(stock_id)
        if plan is None:
            stock = self.db.get_stock_row(stock_id)
            plan = self.build_lightweight_plan(stock_id, x, stock=stock)
            self.plan_cache[str(stock_id)] = plan
        support = float(plan.get("support", 0) or 0)
        fib1 = float(plan.get("resistance", 0) or 0)
        fib1382 = float(plan.get("target_1382", 0) or 0)
        fib1618 = float(plan.get("target_1618", 0) or 0)
        try:
            stop = float(plan.get("stop_loss", 0) or 0)
        except Exception:
            stop = 0.0

        if support > 0:
            ax.axhline(support, linestyle="--", linewidth=1, label=f"Support {support:.2f}")
        if fib1 > 0:
            ax.axhline(fib1, linestyle="--", linewidth=1, label=f"Fib 1.0 {fib1:.2f}")
        if fib1382 > 0:
            ax.axhline(fib1382, linestyle=":", linewidth=1, label=f"Fib 1.382 {fib1382:.2f}")
        if fib1618 > 0:
            ax.axhline(fib1618, linestyle=":", linewidth=1, label=f"Fib 1.618 {fib1618:.2f}")

        wave = WaveEngine.detect_wave_label(x)
        last_close = float(x.iloc[-1]["close"])
        last_x = xs[-1]
        bull_target = fib1382 if fib1382 > 0 else last_close * 1.08
        bear_target = stop if stop > 0 else last_close * 0.95
        path_x = [last_x, last_x + 4, last_x + 9]
        ax.plot(path_x, [last_close, (last_close + bull_target) / 2.0, bull_target], "--", linewidth=1.5, label="Bull Path")
        ax.plot(path_x, [last_close, (last_close + bear_target) / 2.0, bear_target], "--", linewidth=1.5, label="Bear Path")

        recent = x.tail(55)
        try:
            peak_idx = recent["high"].idxmax()
            trough_idx = recent["low"].idxmin()
            peak_y = float(x.loc[peak_idx, "high"])
            trough_y = float(x.loc[trough_idx, "low"])
            ax.scatter([peak_idx], [peak_y], s=36)
            ax.scatter([trough_idx], [trough_y], s=36)
            ax.annotate("Wave Peak", xy=(peak_idx, peak_y), xytext=(peak_idx, peak_y * 1.02), fontfamily=SELECTED_PLOT_FONT)
            ax.annotate("Wave Trough", xy=(trough_idx, trough_y), xytext=(trough_idx, trough_y * 0.98), fontfamily=SELECTED_PLOT_FONT)
        except Exception:
            pass

        ax.set_xlim(0, max(path_x) + 2)
        title_wave = safe_plot_text(wave, fallback="Wave")
        title_signal = safe_plot_text(plan.get("signal", "-"), fallback="-")
        ax.set_title(f"{stock_id} | {title_wave} | {title_signal}", fontfamily=SELECTED_PLOT_FONT)
        info_text = (
            f"Wave: {title_wave}\n"
            f"Entry: {safe_plot_text(plan.get('entry_zone','-'))}\n"
            f"Stop: {safe_plot_text(plan.get('stop_loss','-'))}\n"
            f"RR: {float(plan.get('rr',0) or 0):.2f}"
        )
        ax.text(
            0.01, 0.98, info_text,
            transform=ax.transAxes, va="top", ha="left", fontfamily=SELECTED_PLOT_FONT,
            bbox=dict(boxstyle="round", alpha=0.15)
        )
        ax.grid(alpha=0.2)
        ax.legend(loc="upper left", fontsize=8, prop={"family": SELECTED_PLOT_FONT, "size": 8})
        fig.tight_layout()
        out = CHART_DIR / f"{stock_id}_chart.png"
        fig.savefig(out, dpi=140, bbox_inches="tight")
        plt.close(fig)
        return out



def bootstrap():
    log_info("bootstrap start")
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

    log_info(f"bootstrap done｜{init_message}")
    return db, init_message


def main():
    log_info("main start")
    db = DBManager(DB_PATH)
    db.init_db()
    root = tk.Tk()
    app = AppUI(root, db)
    app.set_status("UI 已啟動，背景初始化中...")

    def _close():
        log_info("application closing")
        try:
            db.close()
        except Exception:
            pass
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", _close)
    root.mainloop()


if __name__ == "__main__":
    main()
