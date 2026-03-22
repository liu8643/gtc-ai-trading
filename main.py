# -*- coding: utf-8 -*-
"""
GTC AI Trading System v5.3.4 PRO
GitHub ready build version

功能：
- 股票主檔分類（市場 / 產業 / 題材）
- 本地 SQLite 歷史資料庫
- Yahoo Finance 收盤資料更新
- 技術指標：MA / MACD / RSI / KD
- 排行榜 / 類股熱度 / 題材輪動
- AI 選股 TOP5
- Tkinter 桌面 UI
"""

import sqlite3
import traceback
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

APP_NAME = "GTC AI Trading System v5.3.4 PRO"
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CHART_DIR = BASE_DIR / "charts"
DATA_DIR.mkdir(exist_ok=True)
CHART_DIR.mkdir(exist_ok=True)

DB_PATH = DATA_DIR / "stock_system_v5_3_4.db"
MASTER_CSV = DATA_DIR / "stocks_master.csv"


class DBManager:
    def __init__(self, db_path: Path):
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row

    def close(self):
        self.conn.close()

    def init_db(self):
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
        df = pd.read_csv(csv_path, dtype={"stock_id": str})
        if "update_date" not in df.columns:
            df["update_date"] = datetime.now().strftime("%Y-%m-%d")
        df.to_sql("stocks_master", self.conn, if_exists="replace", index=False)
        self.conn.commit()

    def get_master(self) -> pd.DataFrame:
        return pd.read_sql_query(
            "SELECT * FROM stocks_master WHERE is_active=1 ORDER BY market, industry, stock_id",
            self.conn
        )

    def get_stock_row(self, stock_id: str) -> Optional[pd.Series]:
        df = pd.read_sql_query("SELECT * FROM stocks_master WHERE stock_id=?", self.conn, params=[stock_id])
        if df.empty:
            return None
        return df.iloc[0]

    def upsert_price_history(self, stock_id: str, df: pd.DataFrame):
        if df.empty:
            return
        cur = self.conn.cursor()
        for _, r in df.iterrows():
            cur.execute("""
            INSERT INTO price_history(stock_id, date, open, high, low, close, volume, turnover)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(stock_id, date) DO UPDATE SET
                open=excluded.open,
                high=excluded.high,
                low=excluded.low,
                close=excluded.close,
                volume=excluded.volume,
                turnover=excluded.turnover
            """, (
                stock_id, r["date"], float(r["open"]), float(r["high"]), float(r["low"]),
                float(r["close"]), float(r["volume"]), float(r["turnover"])
            ))
        self.conn.commit()

    def get_price_history(self, stock_id: str) -> pd.DataFrame:
        return pd.read_sql_query(
            "SELECT * FROM price_history WHERE stock_id=? ORDER BY date",
            self.conn, params=[stock_id]
        )

    def replace_ranking(self, df: pd.DataFrame):
        today = datetime.now().strftime("%Y-%m-%d")
        cur = self.conn.cursor()
        cur.execute("DELETE FROM ranking_result WHERE date=?", (today,))
        self.conn.commit()
        df.to_sql("ranking_result", self.conn, if_exists="append", index=False)

    def get_latest_ranking(self) -> pd.DataFrame:
        q = """
        SELECT rr.*, sm.stock_name, sm.market, sm.industry, sm.theme
        FROM ranking_result rr
        JOIN stocks_master sm ON rr.stock_id = sm.stock_id
        WHERE rr.date = (SELECT MAX(date) FROM ranking_result)
        ORDER BY rr.rank_all ASC
        """
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

    def download_history(self, stock_id: str, market: str, period: str = "2y") -> pd.DataFrame:
        if yf is None:
            return pd.DataFrame()
        try:
            symbol = self.yahoo_symbol(stock_id, market)
            hist = yf.Ticker(symbol).history(period=period, auto_adjust=False)
            if hist.empty:
                return pd.DataFrame()
            hist = hist.rename(columns={
                "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"
            }).reset_index()
            date_col = "Date" if "Date" in hist.columns else "Datetime"
            hist["date"] = pd.to_datetime(hist[date_col]).dt.strftime("%Y-%m-%d")
            hist["turnover"] = hist["close"] * hist["volume"]
            return hist[["date", "open", "high", "low", "close", "volume", "turnover"]]
        except Exception:
            return pd.DataFrame()

    def update_all(self) -> Tuple[int, int]:
        master = self.db.get_master()
        success = 0
        rows = 0
        for _, row in master.iterrows():
            df = self.download_history(str(row["stock_id"]), row["market"])
            if not df.empty:
                self.db.upsert_price_history(str(row["stock_id"]), df)
                success += 1
                rows += len(df)
        return success, rows


class IndicatorEngine:
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
        x["k"] = rsv.ewm(alpha=1/3, adjust=False).mean()
        x["d"] = x["k"].ewm(alpha=1/3, adjust=False).mean()
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

        ai = StrategyEngine._clamp(momentum*0.2 + trend*0.25 + reversal*0.15 + volume*0.15 + risk*0.25)
        total = StrategyEngine._clamp(momentum*0.22 + trend*0.28 + reversal*0.15 + volume*0.15 + risk*0.1 + ai*0.1)

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


class AppUI:
    def __init__(self, root, db: DBManager):
        self.root = root
        self.db = db
        self.data_engine = DataEngine(db)
        self.rank_engine = RankingEngine(db)

        self.root.title(APP_NAME)
        self.root.geometry("1580x920")

        self.market_var = tk.StringVar(value="全部")
        self.industry_var = tk.StringVar(value="全部")
        self.theme_var = tk.StringVar(value="全部")
        self.search_var = tk.StringVar(value="")

        self._build_ui()
        self.refresh_filters()
        self.refresh_all_tables()

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

        ttk.Button(top, text="套用篩選", command=self.refresh_all_tables).pack(side="left", padx=4)
        ttk.Button(top, text="更新資料", command=self.update_data).pack(side="left", padx=4)
        ttk.Button(top, text="重建排行", command=self.rebuild_ranking).pack(side="left", padx=4)
        ttk.Button(top, text="AI選股TOP5", command=self.show_top5).pack(side="left", padx=4)

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

        self.rank_tree = self._make_tree(self.tab_rank, ("rank","id","name","industry","theme","total","ai","signal","action"), {
            "rank":"排名","id":"代號","name":"名稱","industry":"產業","theme":"題材","total":"總分","ai":"AI分","signal":"訊號","action":"建議"
        })
        self.rank_tree.bind("<<TreeviewSelect>>", self.on_select_stock)

        self.sector_tree = self._make_tree(self.tab_sector, ("industry","count","avg_total","avg_ai","top_name"), {
            "industry":"產業","count":"檔數","avg_total":"平均總分","avg_ai":"平均AI分","top_name":"代表股"
        })

        self.theme_tree = self._make_tree(self.tab_theme, ("theme","count","avg_total","avg_ai","top_name"), {
            "theme":"題材","count":"檔數","avg_total":"平均總分","avg_ai":"平均AI分","top_name":"代表股"
        })

        self.detail = tk.Text(right, wrap="word", font=("Consolas", 11))
        self.detail.pack(fill="both", expand=True)

    def _make_tree(self, parent, cols, headers):
        tree = ttk.Treeview(parent, columns=cols, show="headings", height=28)
        for c in cols:
            tree.heading(c, text=headers[c])
            tree.column(c, width=140 if c not in ("rank","count","avg_total","avg_ai","id","total","ai") else 90, anchor="center")
        tree.pack(fill="both", expand=True)
        return tree

    def set_status(self, text):
        self.status_label.config(text=text)
        self.root.update_idletasks()

    def refresh_filters(self):
        master = self.db.get_master()
        self.market_cb["values"] = ["全部"] + sorted(master["market"].dropna().unique().tolist())
        self.industry_cb["values"] = ["全部"] + sorted(master["industry"].dropna().unique().tolist())
        self.theme_cb["values"] = ["全部"] + sorted(master["theme"].dropna().unique().tolist())

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
            .agg(count=("stock_id","count"), avg_total=("total_score","mean"), avg_ai=("ai_score","mean"))
            .sort_values(["avg_total","avg_ai"], ascending=False)
        )
        for _, r in sector.iterrows():
            top_name = df[df["industry"] == r["industry"]].sort_values("total_score", ascending=False).iloc[0]["stock_name"]
            self.sector_tree.insert("", "end", values=(
                r["industry"], int(r["count"]), f"{r['avg_total']:.2f}", f"{r['avg_ai']:.2f}", top_name
            ))

        theme = (
            df.groupby("theme", as_index=False)
            .agg(count=("stock_id","count"), avg_total=("total_score","mean"), avg_ai=("ai_score","mean"))
            .sort_values(["avg_total","avg_ai"], ascending=False)
        )
        for _, r in theme.iterrows():
            top_name = df[df["theme"] == r["theme"]].sort_values("total_score", ascending=False).iloc[0]["stock_name"]
            self.theme_tree.insert("", "end", values=(
                r["theme"], int(r["count"]), f"{r['avg_total']:.2f}", f"{r['avg_ai']:.2f}", top_name
            ))

        self.set_status(f"已載入資料，共 {len(df)} 檔。")

    def update_data(self):
        try:
            self.set_status("開始下載歷史資料...")
            success, rows = self.data_engine.update_all()
            self.set_status(f"完成：成功 {success} 檔，寫入 {rows} 筆。")
            messagebox.showinfo("完成", f"成功 {success} 檔\n寫入 {rows} 筆")
        except Exception as e:
            traceback.print_exc()
            messagebox.showerror("錯誤", str(e))

    def rebuild_ranking(self):
        try:
            self.set_status("開始重建排行...")
            count = self.rank_engine.rebuild()
            self.refresh_all_tables()
            messagebox.showinfo("完成", f"排行已完成，共 {count} 檔")
        except Exception as e:
            traceback.print_exc()
            messagebox.showerror("錯誤", str(e))

    def show_top5(self):
        df = self._filtered_ranking()
        if df.empty:
            return messagebox.showwarning("提醒", "尚無資料")
        top5 = df.sort_values(["ai_score","total_score"], ascending=False).head(5)
        text = ["AI 選股 TOP5\n"]
        for _, r in top5.iterrows():
            text.append(f"{r['stock_id']} {r['stock_name']} | {r['industry']} | AI={r['ai_score']:.2f} | {r['action']}")
        messagebox.showinfo("AI 選股 TOP5", "\n".join(text))

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

        lines = [
            f"股票：{stock['stock_name']} ({stock_id})",
            f"市場 / 產業 / 題材：{stock['market']} / {stock['industry']} / {stock['theme']}",
            f"最新收盤：{last['close']:.2f}",
            f"MA20 / MA60：{last['ma20']:.2f if pd.notna(last['ma20']) else float('nan')} / {last['ma60']:.2f if pd.notna(last['ma60']) else float('nan')}",
            f"RSI14：{last['rsi14']:.2f if pd.notna(last['rsi14']) else float('nan')}",
            f"MACD Hist：{last['macd_hist']:.4f if pd.notna(last['macd_hist']) else float('nan')}",
            f"K / D：{last['k']:.2f if pd.notna(last['k']) else float('nan')} / {last['d']:.2f if pd.notna(last['d']) else float('nan')}",
            "",
            f"波浪階段：{wave}",
            f"Fib 1.0 / 1.382 / 1.618：{fib1:.2f} / {fib2:.2f} / {fib3:.2f}",
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
    master = db.get_master()
    if master.empty and MASTER_CSV.exists():
        db.import_master_csv(MASTER_CSV)
    return db

def main():
    db = bootstrap()
    root = tk.Tk()
    AppUI(root, db)

    def _close():
        db.close()
        root.destroy()

    root.protocol("WM_DELETE_WINDOW", _close)
    root.mainloop()

if __name__ == "__main__":
    main()
