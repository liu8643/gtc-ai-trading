import numpy as np
import pandas as pd
import yfinance as yf
import tkinter as tk
from tkinter import ttk, messagebox

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))

def ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def macd(series: pd.Series):
    macd_line = ema(series, 12) - ema(series, 26)
    signal = ema(macd_line, 9)
    return macd_line, signal

def tw_symbol(ticker: str) -> str:
    ticker = str(ticker).strip().upper().replace(".TW", "")
    return f"{ticker}.TW" if ticker.isdigit() else ticker

def fetch_data(ticker: str) -> pd.DataFrame:
    df = yf.download(tw_symbol(ticker), period="6mo", interval="1d", auto_adjust=True, progress=False)
    if df is None or df.empty:
        raise ValueError(f"{ticker} 無法取得資料")
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]
    return df.dropna()

def analyze(df: pd.DataFrame):
    df = df.copy()
    df["MA20"] = df["Close"].rolling(20).mean()
    df["RSI14"] = rsi(df["Close"], 14)
    df["MACD"], df["MACDSignal"] = macd(df["Close"])
    latest = df.iloc[-1]
    score = 0
    notes = []
    if latest["Close"] > latest["MA20"]:
        score += 40
        notes.append("站上20日線")
    else:
        notes.append("跌破20日線")
    if latest["MACD"] > latest["MACDSignal"]:
        score += 30
        notes.append("MACD偏多")
    else:
        notes.append("MACD偏弱")
    if 45 <= latest["RSI14"] <= 70:
        score += 20
        notes.append("RSI健康")
    elif latest["RSI14"] < 30:
        score += 10
        notes.append("RSI超跌")
    signal = "強勢買進" if score >= 70 else "偏多觀察" if score >= 50 else "保守"
    return round(float(latest["Close"]), 2), signal, int(score), round(float(latest["RSI14"]), 2), "；".join(notes)

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("GTC AI Trading System")
        self.geometry("900x600")
        top = ttk.Frame(self)
        top.pack(fill="x", padx=10, pady=10)
        ttk.Label(top, text="股票代號（逗號分隔）").pack(side="left")
        self.ticker_var = tk.StringVar(value="2330,2382,3231,2308")
        ttk.Entry(top, textvariable=self.ticker_var, width=50).pack(side="left", padx=8)
        ttk.Button(top, text="執行分析", command=self.run_analysis).pack(side="left")
        cols = ("ticker", "close", "signal", "score", "rsi14", "comment")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", height=22)
        headers = {"ticker":"代號", "close":"收盤", "signal":"訊號", "score":"分數", "rsi14":"RSI", "comment":"說明"}
        widths = {"ticker":80, "close":90, "signal":100, "score":70, "rsi14":80, "comment":420}
        for c in cols:
            self.tree.heading(c, text=headers[c])
            self.tree.column(c, width=widths[c], anchor="center")
        self.tree.pack(fill="both", expand=True, padx=10, pady=(0,10))

    def run_analysis(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        tickers = [t.strip() for t in self.ticker_var.get().split(",") if t.strip()]
        try:
            for t in tickers:
                close, signal, score, rsi14, comment = analyze(fetch_data(t))
                self.tree.insert("", "end", values=(t, close, signal, score, rsi14, comment))
        except Exception as e:
            messagebox.showerror("錯誤", str(e))

if __name__ == "__main__":
    App().mainloop()
