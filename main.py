import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import math
import random
from datetime import datetime


APP_TITLE = "GTC AI Trading System Pro"


def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def calc_mock_analysis(symbol: str):
    """
    這是離線可跑的示範分析版，不依賴網路 API。
    先讓 EXE、UI、PDF 匯出流程完整可用。
    之後你要接 Yahoo / 券商 / 真實技術指標，我再幫你升級。
    """
    base = sum(ord(c) for c in symbol.upper())
    rnd = random.Random(base)

    close = round(rnd.uniform(35, 2200), 2)
    ma20 = close * rnd.uniform(0.94, 1.06)
    ma60 = close * rnd.uniform(0.90, 1.10)
    support = round(min(ma20, ma60, close) * rnd.uniform(0.96, 0.995), 2)
    resistance = round(max(ma20, ma60, close) * rnd.uniform(1.01, 1.08), 2)
    rsi = round(rnd.uniform(22, 78), 2)

    score = 50

    comments = []

    if close > ma20:
        score += 15
        comments.append("站上20日線")
    else:
        score -= 10
        comments.append("跌破20日線")

    if close > ma60:
        score += 15
        comments.append("站上60日線")
    else:
        score -= 10
        comments.append("跌破60日線")

    macd_bull = rnd.choice([True, False])
    kd_bull = rnd.choice([True, False])
    volume_expand = rnd.choice([True, False])

    if macd_bull:
        score += 10
        comments.append("MACD偏多")
    else:
        score -= 5
        comments.append("MACD偏弱")

    if kd_bull:
        score += 8
        comments.append("KD偏多")
    else:
        score -= 4
        comments.append("KD偏空")

    if rsi < 30:
        score += 8
        comments.append("RSI超跌")
    elif rsi > 70:
        score -= 6
        comments.append("RSI過熱")

    if volume_expand:
        score += 4
        comments.append("量能放大")

    score = max(0, min(100, int(score)))

    if score >= 80:
        signal = "強勢買進"
    elif score >= 65:
        signal = "偏多觀察"
    elif score >= 45:
        signal = "區間整理"
    elif score >= 30:
        signal = "保守/減碼"
    else:
        signal = "弱勢觀望"

    return {
        "symbol": symbol.upper(),
        "close": close,
        "signal": signal,
        "score": score,
        "support": round(support, 2),
        "resistance": round(resistance, 2),
        "rsi": rsi,
        "comment": "；".join(comments),
    }


class GTCProApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("1320x760")
        self.root.minsize(1080, 640)

        self.results = []

        self._build_ui()

    def _build_ui(self):
        top = ttk.Frame(self.root, padding=12)
        top.pack(fill="x")

        ttk.Label(top, text="股票代號（逗號分隔）").pack(side="left", padx=(0, 8))

        self.symbol_entry = ttk.Entry(top, width=70)
        self.symbol_entry.pack(side="left", padx=(0, 8), fill="x", expand=True)
        self.symbol_entry.insert(0, "2330,2382,3231,2308,3017")

        self.analyze_btn = ttk.Button(top, text="執行分析", command=self.run_analysis)
        self.analyze_btn.pack(side="left", padx=(0, 8))

        self.export_btn = ttk.Button(top, text="匯出報告", command=self.export_report)
        self.export_btn.pack(side="left", padx=(0, 8))

        self.clear_btn = ttk.Button(top, text="清空", command=self.clear_results)
        self.clear_btn.pack(side="left")

        middle = ttk.Frame(self.root, padding=(12, 0, 12, 12))
        middle.pack(fill="both", expand=True)

        columns = (
            "代號", "收盤", "訊號", "分數", "支撐", "壓力", "RSI", "說明"
        )

        self.tree = ttk.Treeview(
            middle,
            columns=columns,
            show="headings",
            height=22
        )

        widths = {
            "代號": 90,
            "收盤": 110,
            "訊號": 130,
            "分數": 80,
            "支撐": 110,
            "壓力": 110,
            "RSI": 90,
            "說明": 560,
        }

        for c in columns:
            self.tree.heading(c, text=c)
            self.tree.column(c, width=widths[c], anchor="center")

        yscroll = ttk.Scrollbar(middle, orient="vertical", command=self.tree.yview)
        xscroll = ttk.Scrollbar(middle, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")

        middle.rowconfigure(0, weight=1)
        middle.columnconfigure(0, weight=1)

        bottom = ttk.LabelFrame(self.root, text="系統訊息", padding=10)
        bottom.pack(fill="x", padx=12, pady=(0, 12))

        self.status_var = tk.StringVar(value="系統已就緒。")
        self.status_label = ttk.Label(bottom, textvariable=self.status_var)
        self.status_label.pack(anchor="w")

    def set_status(self, text: str):
        now = datetime.now().strftime("%H:%M:%S")
        self.status_var.set(f"[{now}] {text}")
        self.root.update_idletasks()

    def clear_results(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.results = []
        self.set_status("已清空結果。")

    def parse_symbols(self):
        raw = self.symbol_entry.get().strip()
        if not raw:
            return []
        parts = [x.strip() for x in raw.replace("，", ",").split(",")]
        return [p for p in parts if p]

    def run_analysis(self):
        symbols = self.parse_symbols()
        if not symbols:
            messagebox.showwarning("提醒", "請輸入至少一個股票代號。")
            return

        self.clear_results()
        self.set_status("開始分析中...")

        self.results = []
        for sym in symbols:
            result = calc_mock_analysis(sym)
            self.results.append(result)

        self.results.sort(key=lambda x: x["score"], reverse=True)

        for r in self.results:
            self.tree.insert(
                "",
                "end",
                values=(
                    r["symbol"],
                    r["close"],
                    r["signal"],
                    r["score"],
                    r["support"],
                    r["resistance"],
                    r["rsi"],
                    r["comment"],
                ),
            )

        self.set_status(f"分析完成，共 {len(self.results)} 檔。")

    def export_report(self):
        if not self.results:
            messagebox.showwarning("提醒", "目前沒有分析結果可匯出。")
            return

        file_path = filedialog.asksaveasfilename(
            title="匯出分析報告",
            defaultextension=".txt",
            filetypes=[("Text file", "*.txt"), ("All files", "*.*")]
        )
        if not file_path:
            return

        lines = []
        lines.append(APP_TITLE)
        lines.append(f"報告時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 90)

        for idx, r in enumerate(self.results, start=1):
            lines.append(f"{idx}. 股票代號：{r['symbol']}")
            lines.append(f"   收盤：{r['close']}")
            lines.append(f"   訊號：{r['signal']}")
            lines.append(f"   分數：{r['score']}")
            lines.append(f"   支撐：{r['support']}")
            lines.append(f"   壓力：{r['resistance']}")
            lines.append(f"   RSI：{r['rsi']}")
            lines.append(f"   說明：{r['comment']}")
            lines.append("-" * 90)

        try:
            with open(file_path, "w", encoding="utf-8-sig") as f:
                f.write("\n".join(lines))
            self.set_status(f"已匯出報告：{file_path}")
            messagebox.showinfo("完成", "報告匯出成功。")
        except Exception as e:
            messagebox.showerror("錯誤", f"匯出失敗：{e}")

def main():
    root = tk.Tk()
    try:
        style = ttk.Style()
        if "vista" in style.theme_names():
            style.theme_use("vista")
    except Exception:
        pass

    app = GTCProApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
