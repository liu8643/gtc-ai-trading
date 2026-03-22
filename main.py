# -*- coding: utf-8 -*-
"""
GTC AI Trading System v5.9 PRO
✔ 全市場歷史建庫（分批）
✔ 每日增量更新
✔ 技術指標（MA/MACD/RSI）
✔ TOP50 + AI選股（可買名單）
"""

import threading
import time
import sqlite3
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import tkinter as tk
from tkinter import ttk, messagebox

try:
    import yfinance as yf
except:
    yf = None

APP_NAME = "GTC AI Trading System v5.9 PRO"

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "stock_v5_9.db"

# ===================== DB =====================
class DB:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)

        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS master(
            stock_id TEXT PRIMARY KEY,
            stock_name TEXT
        )
        """)

        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS price(
            stock_id TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            PRIMARY KEY(stock_id,date)
        )
        """)

    def replace_master(self, df):
        df.to_sql("master", self.conn, if_exists="replace", index=False)

    def get_master(self):
        return pd.read_sql("SELECT * FROM master", self.conn)

    def insert_price(self, df):
        df.to_sql("price", self.conn, if_exists="append", index=False)

    def get_history(self, stock_id):
        return pd.read_sql(
            "SELECT * FROM price WHERE stock_id=? ORDER BY date",
            self.conn,
            params=[stock_id]
        )

# ===================== 抓市場 =====================
def fetch_all_stocks():
    url = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
    data = requests.get(url).json()
    df = pd.DataFrame(data)
    df = df.rename(columns={"Code":"stock_id","Name":"stock_name"})
    return df[["stock_id","stock_name"]]

# ===================== 抓歷史 =====================
def fetch_history(stock_id):
    try:
        df = yf.Ticker(stock_id+".TW").history(period="2y")
        if df.empty:
            return pd.DataFrame()

        df = df.rename(columns={
            "Open":"open","High":"high","Low":"low",
            "Close":"close","Volume":"volume"
        }).reset_index()

        df["date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        df["stock_id"] = stock_id

        return df[["stock_id","date","open","high","low","close","volume"]]
    except:
        return pd.DataFrame()

# ===================== 技術指標 =====================
def calc_indicators(df):
    df["ma20"] = df["close"].rolling(20).mean()
    df["ma60"] = df["close"].rolling(60).mean()

    ema12 = df["close"].ewm(span=12).mean()
    ema26 = df["close"].ewm(span=26).mean()
    df["macd"] = ema12 - ema26

    delta = df["close"].diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    rs = up.rolling(14).mean() / down.rolling(14).mean()
    df["rsi"] = 100 - (100/(1+rs))

    return df

# ===================== AI評分 =====================
def ai_score(df):
    if len(df) < 60:
        return 0, "資料不足"

    last = df.iloc[-1]

    score = 0

    if last["close"] > last["ma20"] > last["ma60"]:
        score += 30

    if last["macd"] > 0:
        score += 20

    if 40 < last["rsi"] < 70:
        score += 20

    momentum = (last["close"]/df.iloc[-20]["close"] - 1)*100
    score += min(max(momentum,0),30)

    # === 決策 ===
    if score >= 70:
        action = "🔥可買"
    elif score >= 50:
        action = "觀察"
    else:
        action = "避開"

    return score, action

# ===================== UI =====================
class App:
    def __init__(self, root):
        self.db = DB()

        self.root = root
        self.root.title(APP_NAME)
        self.root.geometry("1200x800")

        self.status = tk.StringVar(value="Ready")

        top = ttk.Frame(root)
        top.pack()

        ttk.Button(top,text="初始化全市場",command=self.init_market).pack(side="left")
        ttk.Button(top,text="建立歷史資料（一次）",command=self.build_history).pack(side="left")
        ttk.Button(top,text="每日更新",command=self.update_today).pack(side="left")
        ttk.Button(top,text="AI選股（TOP50）",command=self.run_ai).pack(side="left")

        ttk.Label(top,textvariable=self.status).pack(side="right")

        self.tree = ttk.Treeview(root,columns=("id","score","action"),show="headings")
        self.tree.heading("id",text="股票")
        self.tree.heading("score",text="AI分")
        self.tree.heading("action",text="建議")
        self.tree.pack(fill="both",expand=True)

    # =====================
    def init_market(self):
        df = fetch_all_stocks()
        self.db.replace_master(df)
        self.status.set(f"完成 {len(df)} 檔")

    # =====================
    def build_history(self):
        threading.Thread(target=self._build_bg).start()

    def _build_bg(self):
        master = self.db.get_master()

        count = 0
        for i, row in master.iterrows():
            stock_id = row["stock_id"]

            df = fetch_history(stock_id)
            if not df.empty:
                self.db.insert_price(df)
                count += 1

            if i % 20 == 0:
                self.status.set(f"建庫中 {i}/{len(master)}")
                time.sleep(1)

        self.status.set(f"完成建庫 {count} 檔")

    # =====================
    def update_today(self):
        self.status.set("更新今日...")

        url = "https://www.twse.com.tw/exchangeReport/MI_INDEX?response=json&type=ALL"
        data = requests.get(url).json()

        rows = data["data9"]

        result = []
        for r in rows:
            try:
                result.append({
                    "stock_id": r[0],
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "open": float(r[5].replace(",","")),
                    "high": float(r[6].replace(",","")),
                    "low": float(r[7].replace(",","")),
                    "close": float(r[8].replace(",","")),
                    "volume": float(r[2].replace(",",""))
                })
            except:
                pass

        df = pd.DataFrame(result)
        self.db.insert_price(df)

        self.status.set(f"更新完成 {len(df)} 檔")

    # =====================
    def run_ai(self):
        self.status.set("AI分析中...")
        master = self.db.get_master()

        results = []

        for _, row in master.iterrows():
            stock_id = row["stock_id"]
            hist = self.db.get_history(stock_id)

            if len(hist) < 60:
                continue

            hist = calc_indicators(hist)
            score, action = ai_score(hist)

            results.append((stock_id, score, action))

        df = pd.DataFrame(results,columns=["id","score","action"])
        df = df.sort_values("score",ascending=False).head(50)

        # UI
        for i in self.tree.get_children():
            self.tree.delete(i)

        for _, r in df.iterrows():
            self.tree.insert("", "end", values=(r["id"], round(r["score"],1), r["action"]))

        self.status.set("完成 AI選股")

# =====================
def main():
    root = tk.Tk()
    app = App(root)
    root.mainloop()

if __name__ == "__main__":
    main()
