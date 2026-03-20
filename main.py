# ===============================
# GTC 股票專業版看盤分析系統
# v5.0.3-2（完整可拷貝版）
# ===============================

import tkinter as tk
from tkinter import ttk
from datetime import datetime
import random

APP_VERSION = "v5.0.3-2"

# ===============================
# 燈號邏輯（已修正）
# ===============================
def get_light(signal, score, change_pct):

    if change_pct <= -4:
        return "🟣"   # 崩跌

    if change_pct >= 3:
        return "🔵"   # 強勢突破

    if score >= 80 and change_pct > 0:
        return "🟢"

    if score >= 45:
        return "🟡"

    return "🔴"


# ===============================
# 主程式
# ===============================
class StockApp:

    def __init__(self, root):
        self.root = root
        self.root.title(f"GTC 看盤系統 {APP_VERSION}")
        self.root.geometry("1400x800")

        self.results = []
        self.auto_refresh_enabled = True
        self.next_refresh_sec = 30
        self.last_update_time = None

        self._build_ui()

        # 🔥 關鍵（修正倒數問題）
        self.enable_auto_refresh()

    # ===============================
    # UI
    # ===============================
    def _build_ui(self):

        columns = (
            "排名","燈號","市場","代號","名稱","價格",
            "漲跌","漲跌幅","分數","訊號"
        )

        self.tree = ttk.Treeview(self.root, columns=columns, show="headings")

        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=100, anchor="center")

        self.tree.pack(fill="both", expand=True)

        self.status_var = tk.StringVar()
        ttk.Label(self.root, textvariable=self.status_var).pack(anchor="w")

    # ===============================
    # 模擬資料（你可換成API）
    # ===============================
    def mock_data(self):

        data = []

        for i in range(5):
            change = round(random.uniform(-5, 5), 2)
            change_pct = change
            score = random.randint(30, 95)

            if change_pct <= -4:
                signal = "急跌風險"
            elif change_pct >= 3:
                signal = "突破"
            elif score > 80:
                signal = "強勢"
            else:
                signal = "整理"

            data.append({
                "market": "台股",
                "symbol": f"30{i}0",
                "name": "測試股",
                "price": round(100 + change, 2),
                "change": change,
                "change_pct": change_pct,
                "score": score,
                "signal": signal
            })

        return data

    # ===============================
    # 排名（已修正）
    # ===============================
    def rank_results(self):

        self.results.sort(
            key=lambda x: x["score"] * 0.6 + x["change_pct"] * 0.4,
            reverse=True
        )

    # ===============================
    # 渲染
    # ===============================
    def render(self):

        for i in self.tree.get_children():
            self.tree.delete(i)

        for idx, r in enumerate(self.results, start=1):

            light = get_light(r["signal"], r["score"], r["change_pct"])

            self.tree.insert("", "end", values=(
                idx,
                light,
                r["market"],
                r["symbol"],
                r["name"],
                r["price"],
                f"{r['change']:+.2f}",
                f"{r['change_pct']:+.2f}%",
                r["score"],
                r["signal"]
            ))

    # ===============================
    # 主分析
    # ===============================
    def run_analysis(self):

        self.results = self.mock_data()

        # 🔥 排名優化
        self.rank_results()

        self.render()

        self.last_update_time = datetime.now()
        self.next_refresh_sec = 30

    # ===============================
    # 狀態列（倒數）
    # ===============================
    def update_status(self):

        last = self.last_update_time.strftime("%H:%M:%S") if self.last_update_time else "-"

        self.status_var.set(
            f"最後更新：{last} ｜ 下次刷新：{self.next_refresh_sec} 秒"
        )

    # ===============================
    # 自動刷新（已修正）
    # ===============================
    def auto_refresh_job(self):

        if not self.auto_refresh_enabled:
            return

        self.next_refresh_sec -= 1

        if self.next_refresh_sec <= 0:
            self.run_analysis()

        self.update_status()

        self.root.after(1000, self.auto_refresh_job)

    def enable_auto_refresh(self):
        self.auto_refresh_enabled = True
        self.run_analysis()
        self.auto_refresh_job()


# ===============================
# 啟動
# ===============================
if __name__ == "__main__":
    root = tk.Tk()
    app = StockApp(root)
    root.mainloop()
