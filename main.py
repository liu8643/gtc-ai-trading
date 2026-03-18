import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime
import pandas as pd
import yfinance as yf
from reportlab.lib.pagesizes import A4, landscape
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas
import os


APP_TITLE = "GTC AI Trading System Pro Plus"


def setup_pdf_font():
    candidates = [
        r"C:\Windows\Fonts\msjh.ttc",
        r"C:\Windows\Fonts\msjh.ttf",
        r"C:\Windows\Fonts\mingliu.ttc",
        r"C:\Windows\Fonts\kaiu.ttf",
        r"C:\Windows\Fonts\simhei.ttf",
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                pdfmetrics.registerFont(TTFont("CH_FONT", path))
                return "CH_FONT"
            except Exception:
                pass
    return "Helvetica"


def normalize_symbol(symbol: str) -> list[str]:
    s = symbol.strip().upper()
    if not s:
        return []

    if "." in s:
        return [s]

    if s.isdigit():
        if len(s) == 4:
            return [f"{s}.TW", f"{s}.TWO"]
        return [s]

    return [s]


def download_symbol_data(symbol: str, period: str = "6mo") -> tuple[str, pd.DataFrame]:
    candidates = normalize_symbol(symbol)
    last_error = None

    for yf_symbol in candidates:
        try:
            df = yf.download(
                yf_symbol,
                period=period,
                interval="1d",
                auto_adjust=False,
                progress=False,
                threads=False,
            )

            if df is None or df.empty:
                continue

            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] for c in df.columns]

            needed = ["Open", "High", "Low", "Close", "Volume"]
            ok = all(c in df.columns for c in needed)
            if not ok:
                continue

            df = df.dropna(subset=["Close"]).copy()
            if df.empty:
                continue

            return yf_symbol, df

        except Exception as e:
            last_error = e

    if last_error:
        raise ValueError(f"查無資料：{symbol} / {last_error}")
    raise ValueError(f"查無資料：{symbol}")


def calc_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["MA20"] = df["Close"].rolling(20).mean()
    df["MA60"] = df["Close"].rolling(60).mean()

    delta = df["Close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()

    rs = avg_gain / avg_loss.replace(0, pd.NA)
    df["RSI"] = 100 - (100 / (1 + rs))
    df["RSI"] = df["RSI"].fillna(50)

    ema12 = df["Close"].ewm(span=12, adjust=False).mean()
    ema26 = df["Close"].ewm(span=26, adjust=False).mean()
    df["MACD"] = ema12 - ema26
    df["MACD_SIGNAL"] = df["MACD"].ewm(span=9, adjust=False).mean()

    low9 = df["Low"].rolling(9).min()
    high9 = df["High"].rolling(9).max()
    rsv = (df["Close"] - low9) / (high9 - low9) * 100
    df["K"] = rsv.ewm(com=2).mean()
    df["D"] = df["K"].ewm(com=2).mean()

    return df


def detect_market(input_symbol: str, yf_symbol: str) -> str:
    if yf_symbol.endswith(".TW"):
        return "台股上市"
    if yf_symbol.endswith(".TWO"):
        return "台股上櫃"
    if input_symbol.isalpha():
        return "美股/海外"
    return "其他"


def analyze_symbol(symbol: str) -> dict:
    yf_symbol, df = download_symbol_data(symbol)
    df = calc_indicators(df)

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else last

    close = round(float(last["Close"]), 2)
    prev_close = round(float(prev["Close"]), 2)

    change = round(close - prev_close, 2)
    change_pct = round((change / prev_close) * 100, 2) if prev_close != 0 else 0.0

    ma20 = round(float(last["MA20"]), 2) if pd.notna(last["MA20"]) else None
    ma60 = round(float(last["MA60"]), 2) if pd.notna(last["MA60"]) else None
    rsi = round(float(last["RSI"]), 2) if pd.notna(last["RSI"]) else 50.0

    support = round(float(df.tail(20)["Low"].min()), 2)
    resistance = round(float(df.tail(20)["High"].max()), 2)

    score = 50
    comments = []

    if ma20 is not None:
        if close >= ma20:
            score += 10
            comments.append("站上20日線")
        else:
            score -= 10
            comments.append("跌破20日線")

    if ma60 is not None:
        if close >= ma60:
            score += 15
            comments.append("站上60日線")
        else:
            score -= 12
            comments.append("跌破60日線")

    if float(last["MACD"]) >= float(last["MACD_SIGNAL"]):
        score += 8
        comments.append("MACD偏多")
    else:
        score -= 6
        comments.append("MACD偏弱")

    if pd.notna(last["K"]) and pd.notna(last["D"]):
        if float(last["K"]) >= float(last["D"]):
            score += 6
            comments.append("KD偏多")
        else:
            score -= 4
            comments.append("KD偏空")

    if rsi < 30:
        score += 8
        comments.append("RSI超跌")
    elif rsi > 70:
        score -= 8
        comments.append("RSI過熱")

    if len(df) >= 20:
        vol5 = df["Volume"].tail(5).mean()
        vol20 = df["Volume"].tail(20).mean()
        if pd.notna(vol5) and pd.notna(vol20) and vol5 > vol20:
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

    direction_text = "上漲" if change > 0 else "下跌" if change < 0 else "平盤"

    return {
        "input_symbol": symbol,
        "yf_symbol": yf_symbol,
        "market": detect_market(symbol, yf_symbol),
        "close": close,
        "prev_close": prev_close,
        "change": change,
        "change_pct": change_pct,
        "direction": direction_text,
        "signal": signal,
        "score": score,
        "support": support,
        "resistance": resistance,
        "rsi": rsi,
        "comment": "；".join(comments),
    }


class GTCProApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry("1750x860")
        self.root.minsize(1350, 720)
        self.results = []
        self.current_sort_column = None
        self.sort_reverse = True
        self._build_ui()

    def _build_ui(self):
        top = ttk.Frame(self.root, padding=12)
        top.pack(fill="x")

        ttk.Label(top, text="股票代號（逗號分隔）").pack(side="left", padx=(0, 8))

        self.symbol_entry = ttk.Entry(top, width=100)
        self.symbol_entry.pack(side="left", padx=(0, 8), fill="x", expand=True)
        self.symbol_entry.insert(0, "2330,2382,3231,2308,3017,AAPL,NVDA,MSFT")

        ttk.Button(top, text="執行分析", command=self.run_analysis).pack(side="left", padx=(0, 8))
        ttk.Button(top, text="匯出 PDF", command=self.export_pdf).pack(side="left", padx=(0, 8))
        ttk.Button(top, text="匯出 TXT", command=self.export_txt).pack(side="left", padx=(0, 8))
        ttk.Button(top, text="清空", command=self.clear_results).pack(side="left")

        middle = ttk.Frame(self.root, padding=(12, 0, 12, 12))
        middle.pack(fill="both", expand=True)

        columns = (
            "排名", "市場", "代號", "收盤", "昨收", "漲跌", "漲跌幅%", "訊號",
            "分數", "支撐", "壓力", "RSI", "說明"
        )

        self.tree = ttk.Treeview(middle, columns=columns, show="headings", height=25)

        widths = {
            "排名": 70,
            "市場": 110,
            "代號": 90,
            "收盤": 100,
            "昨收": 100,
            "漲跌": 100,
            "漲跌幅%": 100,
            "訊號": 120,
            "分數": 80,
            "支撐": 110,
            "壓力": 110,
            "RSI": 90,
            "說明": 500,
        }

        for c in columns:
            self.tree.heading(c, text=c, command=lambda col=c: self.sort_by_column(col))
            self.tree.column(c, width=widths[c], anchor="center")

        self.tree.tag_configure("up", foreground="red", background="#ffecec")
        self.tree.tag_configure("down", foreground="green", background="#ecffec")
        self.tree.tag_configure("flat", foreground="black", background="white")
        self.tree.tag_configure("strong", background="#fff5cc")

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
        ttk.Label(bottom, textvariable=self.status_var).pack(anchor="w")

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

    def render_results(self):
        for item in self.tree.get_children():
            self.tree.delete(item)

        for idx, r in enumerate(self.results, start=1):
            tags = []
            if r["change"] > 0:
                tags.append("up")
            elif r["change"] < 0:
                tags.append("down")
            else:
                tags.append("flat")

            if r["score"] >= 80:
                tags.append("strong")

            change_str = f"{r['change']:+.2f}"
            change_pct_str = f"{r['change_pct']:+.2f}%"

            self.tree.insert(
                "",
                "end",
                values=(
                    idx,
                    r["market"],
                    r["input_symbol"],
                    r["close"],
                    r["prev_close"],
                    change_str,
                    change_pct_str,
                    r["signal"],
                    r["score"],
                    r["support"],
                    r["resistance"],
                    r["rsi"],
                    r["comment"],
                ),
                tags=tuple(tags),
            )

    def run_analysis(self):
        symbols = self.parse_symbols()
        if not symbols:
            messagebox.showwarning("提醒", "請輸入至少一個股票代號。")
            return

        self.clear_results()
        self.set_status("開始抓取即時股票資料...")

        ok_results = []
        errors = []

        for sym in symbols:
            try:
                result = analyze_symbol(sym)
                ok_results.append(result)
                self.set_status(f"完成：{sym}")
            except Exception as e:
                errors.append(f"{sym}: {e}")

        self.results = sorted(ok_results, key=lambda x: x["score"], reverse=True)
        self.render_results()

        if errors:
            self.set_status(f"完成 {len(self.results)} 檔，失敗 {len(errors)} 檔。")
            messagebox.showwarning("部分股票失敗", "\n".join(errors[:10]))
        else:
            self.set_status(f"分析完成，共 {len(self.results)} 檔。")

    def sort_by_column(self, col_name):
        if not self.results:
            return

        key_map = {
            "排名": None,
            "市場": "market",
            "代號": "input_symbol",
            "收盤": "close",
            "昨收": "prev_close",
            "漲跌": "change",
            "漲跌幅%": "change_pct",
            "訊號": "signal",
            "分數": "score",
            "支撐": "support",
            "壓力": "resistance",
            "RSI": "rsi",
            "說明": "comment",
        }

        real_key = key_map.get(col_name)
        if real_key is None:
            self.results = sorted(self.results, key=lambda x: x["score"], reverse=True)
            self.render_results()
            return

        if self.current_sort_column == col_name:
            self.sort_reverse = not self.sort_reverse
        else:
            self.current_sort_column = col_name
            self.sort_reverse = True

        self.results = sorted(self.results, key=lambda x: x[real_key], reverse=self.sort_reverse)
        self.render_results()

    def export_txt(self):
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
        lines.append("=" * 120)

        for idx, r in enumerate(self.results, start=1):
            lines.append(f"{idx}. 市場：{r['market']} / 股票代號：{r['input_symbol']}")
            lines.append(f"   收盤：{r['close']}")
            lines.append(f"   昨收：{r['prev_close']}")
            lines.append(f"   漲跌：{r['change']:+.2f}")
            lines.append(f"   漲跌幅：{r['change_pct']:+.2f}%")
            lines.append(f"   訊號：{r['signal']}")
            lines.append(f"   分數：{r['score']}")
            lines.append(f"   支撐：{r['support']}")
            lines.append(f"   壓力：{r['resistance']}")
            lines.append(f"   RSI：{r['rsi']}")
            lines.append(f"   說明：{r['comment']}")
            lines.append("-" * 120)

        try:
            with open(file_path, "w", encoding="utf-8-sig") as f:
                f.write("\n".join(lines))
            self.set_status(f"已匯出 TXT：{file_path}")
            messagebox.showinfo("完成", "TXT 匯出成功。")
        except Exception as e:
            messagebox.showerror("錯誤", f"匯出失敗：{e}")

    def export_pdf(self):
        if not self.results:
            messagebox.showwarning("提醒", "目前沒有分析結果可匯出。")
            return

        file_path = filedialog.asksaveasfilename(
            title="匯出 PDF 報告",
            defaultextension=".pdf",
            filetypes=[("PDF file", "*.pdf"), ("All files", "*.*")]
        )
        if not file_path:
            return

        try:
            font_name = setup_pdf_font()
            c = canvas.Canvas(file_path, pagesize=landscape(A4))
            width, height = landscape(A4)

            c.setFont(font_name, 16)
            c.drawString(30, height - 30, APP_TITLE)

            c.setFont(font_name, 10)
            c.drawString(30, height - 50, f"報告時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            y = height - 80
            headers = ["排名", "市場", "代號", "收盤", "漲跌", "漲跌幅%", "訊號", "分數", "支撐", "壓力", "RSI"]
            x_positions = [20, 60, 140, 200, 260, 320, 390, 470, 530, 610, 690]

            c.setFont(font_name, 9)
            for h, x in zip(headers, x_positions):
                c.drawString(x, y, h)

            y -= 18
            c.line(20, y + 8, width - 20, y + 8)

            for idx, r in enumerate(self.results, start=1):
                if y < 40:
                    c.showPage()
                    c.setFont(font_name, 9)
                    y = height - 40

                row = [
                    str(idx),
                    r["market"],
                    r["input_symbol"],
                    str(r["close"]),
                    f"{r['change']:+.2f}",
                    f"{r['change_pct']:+.2f}%",
                    r["signal"],
                    str(r["score"]),
                    str(r["support"]),
                    str(r["resistance"]),
                    str(r["rsi"]),
                ]

                for text, x in zip(row, x_positions):
                    c.drawString(x, y, text)

                y -= 16

            c.save()
            self.set_status(f"已匯出 PDF：{file_path}")
            messagebox.showinfo("完成", "PDF 匯出成功。")

        except Exception as e:
            messagebox.showerror("錯誤", f"PDF 匯出失敗：{e}")


def main():
    root = tk.Tk()
    try:
        style = ttk.Style()
        if "vista" in style.theme_names():
            style.theme_use("vista")
    except Exception:
        pass

    GTCProApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
