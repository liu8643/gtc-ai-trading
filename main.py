import os
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import yfinance as yf
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import tkinter as tk
from tkinter import ttk, messagebox

from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas

APP_TITLE = "GTC AI Trading System"
DEFAULT_TICKERS = "2330,2382,3231,2308,3017,3324,4979"
OUTPUT_DIR = os.path.join(os.path.expanduser("~"), "GTC_AI_Reports")


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def tw_symbol(ticker: str) -> str:
    ticker = str(ticker).strip().upper().replace(".TW", "").replace(".TWO", "")
    if ticker.isdigit():
        return f"{ticker}.TW"
    return ticker


def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def macd(series: pd.Series):
    macd_line = ema(series, 12) - ema(series, 26)
    signal = ema(macd_line, 9)
    hist = macd_line - signal
    return macd_line, signal, hist


def kd(df: pd.DataFrame, period: int = 9):
    low_min = df["Low"].rolling(period).min()
    high_max = df["High"].rolling(period).max()
    rsv = (df["Close"] - low_min) / (high_max - low_min).replace(0, np.nan) * 100
    k = rsv.ewm(com=2).mean()
    d = k.ewm(com=2).mean()
    return k, d


@dataclass
class SignalResult:
    ticker: str
    latest_close: float
    signal: str
    score: int
    support: float
    resistance: float
    fib_0382: float
    fib_05: float
    fib_0618: float
    rsi14: float
    macd_value: float
    macd_signal: float
    k_value: float
    d_value: float
    comment: str


def fetch_data(ticker: str, period: str = "6mo") -> pd.DataFrame:
    symbol = tw_symbol(ticker)
    df = yf.download(symbol, period=period, interval="1d", auto_adjust=True, progress=False)
    if df is None or df.empty:
        raise ValueError(f"{ticker} 無法取得資料")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]
    return df.dropna()


def build_signal(df: pd.DataFrame, ticker: str) -> SignalResult:
    df = df.copy()
    df["MA20"] = df["Close"].rolling(20).mean()
    df["MA60"] = df["Close"].rolling(60).mean()
    df["RSI14"] = rsi(df["Close"], 14)
    df["MACD"], df["MACDSignal"], _ = macd(df["Close"])
    df["K"], df["D"] = kd(df)

    latest = df.iloc[-1]
    recent = df.tail(90)
    low_90 = float(recent["Low"].min())
    high_90 = float(recent["High"].max())
    diff = max(high_90 - low_90, 1.0)

    fib_0382 = high_90 - diff * 0.382
    fib_05 = high_90 - diff * 0.5
    fib_0618 = high_90 - diff * 0.618

    support = float(df["MA20"].iloc[-1]) if not pd.isna(df["MA20"].iloc[-1]) else float(latest["Close"])
    resistance = high_90

    score = 0
    notes = []

    if latest["Close"] > latest["MA20"]:
        score += 20
        notes.append("站上20日線")
    else:
        notes.append("跌破20日線")

    if latest["Close"] > latest["MA60"]:
        score += 20
        notes.append("站上60日線")
    else:
        notes.append("跌破60日線")

    if latest["MACD"] > latest["MACDSignal"]:
        score += 20
        notes.append("MACD偏多")
    else:
        notes.append("MACD偏弱")

    if latest["K"] > latest["D"]:
        score += 15
        notes.append("KD偏多")
    else:
        notes.append("KD偏空")

    if 45 <= latest["RSI14"] <= 70:
        score += 15
        notes.append("RSI健康")
    elif latest["RSI14"] < 30:
        score += 10
        notes.append("RSI超跌")

    avg_vol = df["Volume"].tail(20).mean()
    if latest["Volume"] > avg_vol * 1.2:
        score += 10
        notes.append("量能放大")

    if score >= 75:
        signal = "強勢買進"
    elif score >= 60:
        signal = "偏多觀察"
    elif score >= 45:
        signal = "區間整理"
    else:
        signal = "保守/減碼"

    return SignalResult(
        ticker=ticker,
        latest_close=round(float(latest["Close"]), 2),
        signal=signal,
        score=int(score),
        support=round(support, 2),
        resistance=round(resistance, 2),
        fib_0382=round(fib_0382, 2),
        fib_05=round(fib_05, 2),
        fib_0618=round(fib_0618, 2),
        rsi14=round(float(latest["RSI14"]), 2),
        macd_value=round(float(latest["MACD"]), 4),
        macd_signal=round(float(latest["MACDSignal"]), 4),
        k_value=round(float(latest["K"]), 2),
        d_value=round(float(latest["D"]), 2),
        comment="；".join(notes),
    )


def try_register_font():
    candidates = [
        r"C:\Windows\Fonts\msjh.ttc",
        r"C:\Windows\Fonts\mingliu.ttc",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                pdfmetrics.registerFont(TTFont("APPFONT", path))
                return "APPFONT"
            except Exception:
                continue
    return "Helvetica"


def export_pdf(results: List[SignalResult], output_pdf: str) -> None:
    ensure_dir(os.path.dirname(output_pdf))
    font_name = try_register_font()
    c = canvas.Canvas(output_pdf, pagesize=A4)
    width, height = A4

    c.setFont(font_name, 18)
    c.drawString(40, height - 40, "GTC AI Trading System")
    c.setFont(font_name, 10)
    c.drawString(40, height - 58, f"Report Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    y = height - 90
    c.setFont(font_name, 10)
    for r in results:
        lines = [
            f"{r.ticker} | {r.signal} | Score {r.score}",
            f"Close {r.latest_close} | Support {r.support} | Resistance {r.resistance}",
            f"Fib 0.382={r.fib_0382} / 0.5={r.fib_05} / 0.618={r.fib_0618}",
            f"RSI={r.rsi14} | MACD={r.macd_value} / {r.macd_signal} | KD={r.k_value}/{r.d_value}",
            f"Comment: {r.comment}",
        ]
        for line in lines:
            c.drawString(40, y, line[:110])
            y -= 15
        y -= 8
        if y < 80:
            c.showPage()
            y = height - 40
            c.setFont(font_name, 10)
    c.save()


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(APP_TITLE)
        self.geometry("1100x700")
        self.results = []
        self.data_cache: Dict[str, pd.DataFrame] = {}
        ensure_dir(OUTPUT_DIR)
        self._build_ui()

    def _build_ui(self):
        top = ttk.Frame(self)
        top.pack(fill="x", padx=12, pady=12)

        ttk.Label(top, text="股票代號（逗號分隔）").pack(side="left")
        self.ticker_var = tk.StringVar(value=DEFAULT_TICKERS)
        ttk.Entry(top, textvariable=self.ticker_var, width=60).pack(side="left", padx=8)

        ttk.Button(top, text="執行分析", command=self.run_analysis).pack(side="left", padx=4)
        ttk.Button(top, text="匯出 PDF", command=self.export_report).pack(side="left", padx=4)

        columns = ("ticker", "close", "signal", "score", "support", "resistance", "rsi", "comment")
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=26)
        headers = {
            "ticker": "代號", "close": "收盤", "signal": "訊號", "score": "分數",
            "support": "支撐", "resistance": "壓力", "rsi": "RSI", "comment": "說明"
        }
        widths = {"ticker": 70, "close": 80, "signal": 90, "score": 60, "support": 80, "resistance": 80, "rsi": 70, "comment": 420}
        for col in columns:
            self.tree.heading(col, text=headers[col])
            self.tree.column(col, width=widths[col], anchor="center")
        self.tree.pack(fill="both", expand=True, padx=12, pady=(0, 12))

    def run_analysis(self):
        tickers = [t.strip() for t in self.ticker_var.get().split(",") if t.strip()]
        if not tickers:
            messagebox.showwarning("提醒", "請輸入股票代號")
            return

        self.results.clear()
        self.data_cache.clear()
        for item in self.tree.get_children():
            self.tree.delete(item)

        try:
            for ticker in tickers:
                df = fetch_data(ticker)
                self.data_cache[ticker] = df
                result = build_signal(df, ticker)
                self.results.append(result)

            self.results.sort(key=lambda x: x.score, reverse=True)
            for r in self.results:
                self.tree.insert("", "end", values=(
                    r.ticker, r.latest_close, r.signal, r.score, r.support, r.resistance, r.rsi14, r.comment
                ))
        except Exception as e:
            messagebox.showerror("錯誤", str(e))

    def export_report(self):
        if not self.results:
            messagebox.showwarning("提醒", "請先執行分析")
            return

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        pdf_path = os.path.join(OUTPUT_DIR, f"GTC_AI_Report_{ts}.pdf")
        export_pdf(self.results, pdf_path)
        messagebox.showinfo("完成", f"已輸出：\n{pdf_path}")


if __name__ == "__main__":
    app = App()
    app.mainloop()
