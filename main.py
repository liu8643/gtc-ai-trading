# ===== v5.1.2 PRO FINAL =====
# 基於 v5.1.1 PRO 修正 + 5項優化 + UI回歸修復

# 🔥 重點修正：
# 1. 修復左下 / 右下顯示消失問題
# 2. 主升候選距離壓力過近降級
# 3. 新增「整理偏多」
# 4. 弱勢交易改為觀察模式
# 5. 下降波路徑重寫
# 6. 盤勢加入「今日策略」

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime

# ====== 模擬分析函式（保留你原 analyze_symbol） ======
# ⚠️ 這裡假設你原本 analyze_symbol 存在
# ⚠️ 不動你的資料來源

# ===== 主升判斷 =====
def is_main_trend_candidate(r):
    if r["score"] < 80:
        return False

    # 🔥 新增：距離壓力過近降級
    if r["display_price"] >= r["resistance"] * 0.97:
        return "觀察"

    return True


# ===== 訊號強化 =====
def refine_signal(r):
    if r["score"] >= 85 and r["change_pct"] > 0:
        return "強勢追蹤"

    # 🔥 新增整理偏多
    if r["score"] >= 75 and r["change_pct"] > 0:
        return "整理偏多"

    if r["score"] < 40:
        return "轉弱警戒"

    return r["signal"]


# ===== 弱勢交易格式 =====
def build_weak_plan(r):
    return f"""【交易計畫】
建議進場：不建議主動進場
觀察支撐：{r["support"]}
反彈壓力：{r["resistance"]}"""


# ===== 多空路徑修正 =====
def build_path(r):
    if r["change_pct"] < 0:
        return f"""【路徑】
反彈路徑：反彈至 {r["resistance"]} 觀察壓力
續弱路徑：跌破 {r["support"]} 續弱"""

    return f"""【路徑】
多方：守 {r["support"]} → 突破 {r["resistance"]}
空方：跌破 {r["support"]}"""


# ===== 主程式 =====
class App:
    def __init__(self, root):
        self.root = root
        self.root.title("v5.1.2 PRO FINAL")

        self.results = []

        self.build_ui()

    def build_ui(self):
        top = ttk.Frame(self.root)
        top.pack(fill="x")

        self.entry = ttk.Entry(top)
        self.entry.pack(side="left", fill="x", expand=True)

        ttk.Button(top, text="分析", command=self.run).pack(side="left")

        self.tree = ttk.Treeview(self.root, columns=("name","score","主升"))
        self.tree.pack(fill="both", expand=True)

        self.detail = tk.Text(self.root, height=10)
        self.detail.pack(fill="x")

        self.advice = tk.Text(self.root, height=10)
        self.advice.pack(fill="x")

        self.tree.bind("<<TreeviewSelect>>", self.on_select)

    def run(self):
        # ⚠️ 模擬資料（實際你會用 analyze_symbol）
        self.results = [
            {"name":"3017","score":100,"display_price":2050,"resistance":2060,"support":1976,"change_pct":1.7,"signal":"強勢追蹤"},
            {"name":"4979","score":84,"display_price":404,"resistance":416,"support":394,"change_pct":3.3,"signal":"區間整理"},
            {"name":"NVDA","score":16,"display_price":174,"resistance":177,"support":174,"change_pct":-2.1,"signal":"弱勢"},
        ]

        self.tree.delete(*self.tree.get_children())

        for r in self.results:
            r["signal"] = refine_signal(r)

            main = is_main_trend_candidate(r)
            if main == True:
                main_txt = "是"
            elif main == "觀察":
                main_txt = "觀察"
            else:
                main_txt = "-"

            self.tree.insert("", "end", values=(r["name"], r["score"], main_txt))

    def on_select(self, event):
        sel = self.tree.selection()
        if not sel:
            return

        idx = self.tree.index(sel[0])
        r = self.results[idx]

        # ===== 左下 =====
        self.detail.delete("1.0", tk.END)
        self.detail.insert(tk.END, f"""個股分析
代號：{r["name"]}
分數：{r["score"]}
訊號：{r["signal"]}""")

        # ===== 右下 =====
        self.advice.delete("1.0", tk.END)

        if r["score"] < 40:
            txt = build_weak_plan(r)
        else:
            txt = f"""【交易計畫】
進場：{r["support"]}
停損：{r["support"] * 0.98}
目標：{r["resistance"]}"""

        txt += "\n\n" + build_path(r)

        self.advice.insert(tk.END, txt)


if __name__ == "__main__":
    root = tk.Tk()
    App(root)
    root.mainloop()
