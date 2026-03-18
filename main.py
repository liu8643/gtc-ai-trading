import os
import ssl
import math
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import List, Tuple

import numpy as np
import pandas as pd
import yfinance as yf
import tkinter as tk
from tkinter import ttk, messagebox

# PDF 輸出
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont


APP_TITLE = "GTC AI Trading System"
DEFAULT_TICKERS = "2330,2382,3231,2308,3017,3324,4979"
OUTPUT_DIR = os.path.join(os.path.expanduser("~"), "GTC_AI_Reports")


@dataclass
class AnalyzeResult:
    ticker: str
    close: float
    signal: str
    score: int
    support: float
    resistance: float
    rsi14: float
    comment: str


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def to_float(value) -> float:
    if value is None:
        return np.nan
    s = str(value).strip().replace(",", "")
    if s in {"", "--", "----", "X", "除權息", "除息", "除權"}:
        return np.nan
    try:
        return float(s)
    except Exception:
        return np.nan


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(period).mean()
    avg_loss = loss.rolling(period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def macd(series: pd.Series) -> Tuple[pd.Series, pd.Series]:
    macd_line = ema(series, 12) - ema(series, 26)
    signal = ema(macd_line, 9)
    return macd_line, signal


def kd(df: pd.DataFrame, period: int = 9) -> Tuple[pd.Series, pd.Series]:
    low_min = df["Low"].rolling(period).min()
    high_max = df["High"].rolling(period).max()
    rsv = (df["Close"] - low_min) / (high_max - low_min).replace(0, np.nan) * 100
    k = rsv.ewm(com=2, adjust=False).mean()
    d = k.ewm(com=2, adjust=False).mean()
    return k, d


def month_list_back(n_months: int = 8) -> List[Tuple[int, int]]:
    today = datetime.today()
    y, m = today.year, today.month
    result = []
    for _ in range(n_months):
        result.append((y, m))
        m -= 1
        if m == 0:
            y -= 1
            m = 12
    result.reverse()
    return result


def fetch_twse_month(stock_no: str, year: int, month: int) -> pd.DataFrame:
    """
    只抓 TWSE 上市股票月資料（官方來源）
    """
    date_str = f"{year}{month:02d}01"
    url = (
        "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
        f"?response=json&date={date_str}&stockNo={urllib.parse.quote(stock_no)}"
    )

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json,text/plain,*/*",
        },
    )

    context = ssl.create_default_context()
    with urllib.request.urlopen(req, timeout=20, context=context) as resp:
        raw = resp.read().decode("utf-8", errors="ignore")

    import json

    data = json.loads(raw)
    rows = data.get("data", [])
    if not rows:
        return pd.DataFrame()

    parsed = []
    for row in rows:
        # TWSE 欄位：
        # 日期, 成交股數, 成交金額, 開盤價, 最高價, 最低價, 收盤價, 漲跌價差, 成交筆數
        roc_date = row[0].strip()
        try:
            roc_y, mm, dd = roc_date.split("/")
            ad_y = int(roc_y) + 1911
            dt = pd.Timestamp(f"{ad_y}-{int(mm):02d}-{int(dd):02d}")
        except Exception:
            continue

        parsed.append(
            {
                "Date": dt,
                "Open": to_float(row[3]),
                "High": to_float(row[4]),
                "Low": to_float(row[5]),
                "Close": to_float(row[6]),
                "Volume": to_float(row[1]),
            }
        )

    df = pd.DataFrame(parsed)
    if df.empty:
        return df

    df = df.dropna(subset=["Open", "High", "Low", "Close"]).copy()
    df = df.sort_values("Date").set_index("Date")
    return df


def fetch_twse_stock(stock_no: str, months: int = 8) -> pd.DataFrame:
    frames = []
    for year, month in month_list_back(months):
        try:
            df = fetch_twse_month(stock_no, year, month)
            if not df.empty:
                frames.append(df)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames)
    out = out[~out.index.duplicated(keep="last")].sort_index()
    return out


def fetch_yfinance(symbol: str) -> pd.DataFrame:
    df = yf.download(symbol, period="8mo", interval="1d", auto_adjust=False, progress=False)
    if df is None or df.empty:
        return pd.DataFrame()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    # 統一欄位
    cols = {}
    for c in ["Open", "High", "Low", "Close", "Volume"]:
        if c in df.columns:
            cols[c] = c

    if "Adj Close" in df.columns and "Close" not in df.columns:
        cols["Adj Close"] = "Close"

    df = df.rename(columns=cols)
    needed = ["Open", "High", "Low", "Close", "Volume"]
    for c in needed:
        if c not in df.columns:
            df[c] = np.nan

    df = df[needed].dropna(subset=["Open", "High", "Low", "Close"]).copy()
    return df.sort_index()


def fetch_data(ticker: str) -> pd.DataFrame:
    ticker = str(ticker).strip().upper()

    # 台股數字代號：先走 TWSE 官方資料
    if ticker.isdigit():
        df_twse = fetch_twse_stock(ticker, months=8)
        if not df_twse.empty and len(df_twse) >= 30:
            return df_twse

        # 若官方抓不到，再 fallback 到 Yahoo
        for symbol in [f"{ticker}.TW", f"{ticker}.TWO"]:
            df_yf = fetch_yfinance(symbol)
            if not df_yf.empty and len(df_yf) >= 30:
                return df_yf

        raise ValueError(f"{ticker} 無法取得台股資料")

    # 非數字：走 yfinance
    df = fetch_yfinance(ticker)
    if df.empty or len(df) < 30:
        raise ValueError(f"{ticker} 無法取得資料")
    return df


def build_result(df: pd.DataFrame, ticker: str) -> AnalyzeResult:
    df = df.copy()
    df["MA20"] = df["Close"].rolling(20).mean()
    df["MA60"] = df["Close"].rolling(60).mean()
    df["RSI14"] = rsi(df["Close"], 14)
    df["MACD"], df["MACDSignal"] = macd(df["Close"])
    df["K"], df["D"] = kd(df)

    latest = df.iloc[-1]
    recent = df.tail(60)

    support = float(recent["Low"].min()) if not recent.empty else float(latest["Close"])
    resistance = float(recent["High"].max()) if not recent.empty else float(latest["Close"])

    score = 0
    notes: List[str] = []

    if not pd.isna(latest["MA20"]) and latest["Close"] > latest["MA20"]:
        score += 25
        notes.append("站上20日線")
    else:
        notes.append("跌破20日線")

    if not pd.isna(latest["MA60"]) and latest["Close"] > latest["MA60"]:
        score += 20
        notes.append("站上60日線")
    else:
        notes.append("跌破60日線")

    if not pd.isna(latest["MACD"]) and not pd.isna(latest["MACDSignal"]) and latest["MACD"] > latest["MACDSignal"]:
        score += 20
        notes.append("MACD偏多")
    else:
        notes.append("MACD偏弱")

    if not pd.isna(latest["K"]) and not pd.isna(latest["D"]) and latest["K"] > latest["D"]:
        score += 15
        notes.append("KD偏多")
    else:
        notes.append("KD偏空")

    if not pd.isna(latest["RSI14"]):
        if 45 <= latest["RSI14"] <= 70:
            score += 20
            notes.append("RSI健康")
        elif latest["RSI14"] < 30:
            score += 10
            notes.append("RSI超跌")
        elif latest["RSI14"] > 80:
            notes.append("RSI過熱")

    signal = "強勢買進" if score >= 70 else "偏多觀察" if score >= 50 else "保守/減碼"

    return AnalyzeResult(
        ticker=ticker,
        close=round(float(latest["Close"]), 2),
        signal=signal,
        score=int(score),
        support=round(support, 2),
        resistance=round(resistance, 2),
        rsi14=round(float(latest["RSI14"]), 2) if not pd.isna(latest["RSI14"]) else np.nan,
        comment="；".join(notes),
    )


def register_pdf_font() -> str:
    candidates = [
        r"C:\Windows\Fonts\msjh.ttc",
        r"C:\Windows\Fonts\msjh.ttf",
        r"C:\Windows\Fonts\mingliu.ttc",
        r"C:\Windows\Fonts\mingliu.ttf",
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                pdfmetrics.registerFont(TTFont("APPFONT", path))
                return "APPFONT"
            except Exception:
                continue
    return "Helvetica"


def export_pdf(rows: List[AnalyzeResult]) -> str:
    ensure_dir(OUTPUT_DIR)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(OUTPUT_DIR, f"GTC_AI_Report_{ts}.pdf")

    font_name = register_pdf_font()
    c = canvas.Canvas(out_path, pagesize=A4)
    width, height = A4

    c.setFont(font_name, 16)
    c.drawString(40, height - 40, "GTC AI Trading System Report")

    c.setFont(font_name, 10)
    c.drawString(40, height - 60, f"產生時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    y = height - 90
    headers = ["代號", "收盤", "訊號", "分數", "支撐", "壓力", "RSI", "說明"]
    xs = [40, 95, 150, 225, 280, 340, 400, 455]

    for x, h in zip(xs, headers):
        c.drawString(x, y, h)

    y -= 18
    c.line(40, y + 8, width - 40, y + 8)

    for r in rows:
        values = [
            r.ticker,
            f"{r.close:.2f}",
            r.signal,
            str(r.score),
            f"{r.support:.2f}",
            f"{r.resistance:.2f}",
            f"{r.rsi14:.2f}" if not pd.isna(r.rsi14) else "-",
            r.comment[:40],
        ]
        for x, val in zip(xs, values):
            c.drawString(x, y, val)
        y -= 16
        if y < 60:
            c.showPage()
            c.setFont(font_name, 10)
            y = height - 40

    c.save()
    return out_path


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1450x800")
        self.results: List[AnalyzeResult] = []
        self._build_ui()

    def _build_ui(self):
        top = ttk.Frame(self)
        top.pack(fill="x", padx=12, pady=12)

        ttk.Label(top, text="股票代號（逗號分隔）").pack(side="left")

        self.ticker_var = tk.StringVar(value=DEFAULT_TICKERS)
        ttk.Entry(top, textvariable=self.ticker_var, width=65).pack(side="left", padx=8)

        ttk.Button(top, text="執行分析", command=self.run_analysis).pack(side="left", padx=8)
        ttk.Button(top, text="匯出 PDF", command=self.on_export_pdf).pack(side="left", padx=8)

        cols = ("ticker", "close", "signal", "score", "support", "resistance", "rsi14", "comment")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", height=30)

        headers = {
            "ticker": "代號",
            "close": "收盤",
            "signal": "訊號",
            "score": "分數",
            "support": "支撐",
            "resistance": "壓力",
            "rsi14": "RSI",
            "comment": "說明",
        }
        widths = {
            "ticker": 90,
            "close": 110,
            "signal": 140,
            "score": 90,
            "support": 110,
            "resistance": 110,
            "rsi14": 100,
            "comment": 700,
        }

        for c in cols:
            self.tree.heading(c, text=headers[c])
            self.tree.column(c, width=widths[c], anchor="center")

        self.tree.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        note = ttk.Label(
            self,
            text="台股數字代號會優先使用 TWSE 官方資料；美股/其他代號使用 yfinance。",
        )
        note.pack(anchor="w", padx=12, pady=(0, 12))

    def run_analysis(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.results.clear()

        raw = self.ticker_var.get().strip()
        tickers = [t.strip() for t in raw.split(",") if t.strip()]
        if not tickers:
            messagebox.showwarning("提醒", "請輸入股票代號")
            return

        self.config(cursor="watch")
        self.update_idletasks()

        errors = []
        for ticker in tickers:
            try:
                df = fetch_data(ticker)
                r = build_result(df, ticker)
                self.results.append(r)
                self.tree.insert(
                    "",
                    "end",
                    values=(
                        r.ticker,
                        r.close,
                        r.signal,
                        r.score,
                        r.support,
                        r.resistance,
                        r.rsi14,
                        r.comment,
                    ),
                )
            except Exception as e:
                errors.append(f"{ticker}: {e}")

        self.config(cursor="")
        self.update_idletasks()

        if errors and not self.results:
            messagebox.showerror("錯誤", "\n".join(errors))
        elif errors:
            messagebox.showwarning("部分股票失敗", "\n".join(errors))

    def on_export_pdf(self):
        if not self.results:
            messagebox.showwarning("提醒", "請先執行分析")
            return
        try:
            path = export_pdf(self.results)
            messagebox.showinfo("完成", f"PDF 已輸出：\n{path}")
        except Exception as e:
            messagebox.showerror("錯誤", str(e))


if __name__ == "__main__":
    ensure_dir(OUTPUT_DIR)
    App().mainloop()