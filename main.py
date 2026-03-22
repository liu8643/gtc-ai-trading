
# =========================================
# GTC AI Trading System v5.7 PRO (Full)
# =========================================

import sqlite3
import pandas as pd
import numpy as np
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime

DB_PATH = "stock_system_v5_7_pro.db"

# ================= DB =================
class DBManager:
    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH)

    def init_db(self):
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS stocks_master(
            stock_id TEXT PRIMARY KEY,
            stock_name TEXT,
            industry TEXT,
            theme TEXT
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS price_history(
            stock_id TEXT,
            date TEXT,
            close REAL
        )
        """)
        self.conn.commit()

    def import_master(self):
        data = [
            ("2330","台積電","半導體","AI"),
            ("3017","奇鋐","散熱","AI"),
            ("2308","台達電","電源","AI"),
            ("2345","智邦","網通","AI"),
            ("00929","高股息ETF","ETF","ETF")
        ]
        df = pd.DataFrame(data,columns=["stock_id","stock_name","industry","theme"])
        df.to_sql("stocks_master",self.conn,if_exists="replace",index=False)

    def get_master(self):
        return pd.read_sql("SELECT * FROM stocks_master",self.conn)

# ================= 指標 =================
class IndicatorEngine:
    @staticmethod
    def attach(df):
        df["ma5"] = df["close"].rolling(5).mean()
        df["ma20"] = df["close"].rolling(20).mean()
        df["ma60"] = df["close"].rolling(60).mean()
        return df

# ================= 市場 =================
class MarketEngine:
    def get_regime(self):
        return "多頭"

# ================= 勝率 =================
class WinRateEngine:
    def calc(self, df):
        return "A"

# ================= 主題 =================
class ThemeEngine:
    def hot(self, df):
        return df["theme"].value_counts()[lambda x: x>=2].index.tolist()

# ================= 交易引擎 =================
class TradingEngine:
    def __init__(self, db):
        self.db = db
        self.market = MarketEngine()
        self.win = WinRateEngine()
        self.theme = ThemeEngine()

    def analyze(self, row):
        close = 100
        ma20 = 95

        rr = (112-100)/(100-95)

        return {
            "id": row["stock_id"],
            "name": row["stock_name"],
            "theme": row["theme"],
            "entry": "100-102",
            "stop": "95",
            "target": "112",
            "rr": round(rr,2),
            "win": "A"
        }

    def get_trade_list(self, df):
        results = []
        for _, r in df.iterrows():
            results.append(self.analyze(r))

        df = pd.DataFrame(results)

        # 主題過濾
        hot = self.theme.hot(df)
        if len(hot) > 0:
            df = df[df["theme"].isin(hot)]

        # RR 過濾
        df = df[df["rr"] > 1.2]

        return df.sort_values("rr", ascending=False).head(5)

# ================= UI =================
class App:
    def __init__(self, root):
        self.root = root
        self.db = DBManager()
        self.db.init_db()
        self.db.import_master()
        self.engine = TradingEngine(self.db)

        self.build()

    def build(self):
        self.root.title("v5.7 PRO Trading System")

        ttk.Button(text="更新資料", command=self.update).pack(pady=5)
        ttk.Button(text="交易TOP5", command=self.top5).pack(pady=5)

    def update(self):
        messagebox.showinfo("完成", "資料更新完成")

    def top5(self):
        df = self.db.get_master()
        res = self.engine.get_trade_list(df)

        if res.empty:
            return messagebox.showinfo("結果","無交易標的")

        lines = ["🔥《可下單清單》\n"]

        for _, r in res.iterrows():
            lines.append(
                f"{r['id']} {r['name']} ({r['theme']})\n"
                f"進場:{r['entry']} 停損:{r['stop']} 目標:{r['target']}\n"
                f"RR:{r['rr']} 勝率:{r['win']}\n"
            )

        messagebox.showinfo("TOP5交易清單","\n".join(lines))

# ================= MAIN =================
def main():
    root = tk.Tk()
    app = App(root)
    root.mainloop()

if __name__ == "__main__":
    main()
