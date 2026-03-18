import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime
import pandas as pd
import yfinance as yf


APP_TITLE = "GTC AI Trading System Pro"


def normalize_symbol(symbol: str) -> str:
    s = symbol.strip().upper()
    if not s:
        return s

    # 已有市場尾碼就直接用
    if "." in s:
        return s

    # 台股常見代號自動補 Yahoo 市場尾碼
    # 4 碼先預設上市 .TW，抓不到再試上櫃 .TWO
    if s.isdigit() and len(s) == 4:
        return s + ".TW"

    return s


def fetch_history(symbol: str, period: str = "6mo") -> pd.DataFrame:
    """
    先抓原始 symbol；
    若是台股 4 碼代號且 .TW 失敗，改抓 .TWO。
    """
    yf_symbol = normalize_symbol(symbol)
    df = yf.download(
        yf_symbol,
        period=period,
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )

    if (df is None or df.empty) and symbol.isdigit() and len(symbol) == 4:
        alt_symbol = symbol + ".TWO"
        df = yf.download(
            alt_symbol,
            period=period,
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        yf_symbol = alt_symbol

    if df is None or df.empty:
        raise ValueError(f"查無資料：{symbol}")

    # 某些版本/情況可能出現多層欄位，這裡壓平
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    needed = ["Open", "High", "Low", "Close", "Volume"]
    for c in needed:
        if c not in df.columns:
            raise ValueError(f"{symbol} 缺少欄位：{c}")

    df = df.dropna(subset=["Close"]).copy()
    if df.empty:
        raise ValueError(f"無有效收盤資料：{symbol}")

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

    return df


def analyze_symbol(symbol: str) -> dict:
    df = fetch_history(symbol)
    last = df.iloc[-1]

    close = round(float(last["Close"]), 2)
    ma20 = round(float(last["MA20"])) if pd.notna(last["MA20"]) else None
    ma60 = round(float(last["MA60"])) if pd.notna(last["MA60"]) else None
    rsi = round(float(last["RSI"]), 2) if pd.notna(last["RSI"]) else 50.0

    recent_20 = df.tail(20)
    support = round(float(recent_20["Low"].min()), 2)
    resistance = round(float(recent_20["High"].max()), 2)

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

    # 簡化版 MACD 趨勢判斷
    ema12 = df["Close"].ewm(span=12, adjust=False).mean()
    ema26 = df["Close"].ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal_line = macd.ewm(span=9, adjust=False).mean()
    if float(macd.iloc[-1]) >= float(signal_line.iloc[-1]):
        score += 8
        comments.append("MACD偏多")
    else:
        score -= 6
        comments.append("MACD偏弱")

    # 簡化 KD 趨勢
    low9 = df["Low"].rolling(9).min()
    high9 = df["High"].rolling(9).max()
    rsv = (df["Close"] - low9) / (high9 - low9) * 100
    k = rsv.ewm(com=2).mean()
    d = k.ewm(com=2).mean()
    if pd.notna(k.iloc[-1]) and pd.notna(d.iloc[-1]):
        if float(k.iloc[-1]) >= float(d.iloc[-1]):
            score += 6
            comments.append("KD偏多")
        else:
            score -= 4
            comments.append("KD偏空")

    # RSI
    if rsi < 30:
        score += 8
        comments.append("RSI超跌")
    elif rsi > 70:
        score -= 8
        comments.append("RSI過熱")

    # 量能
    if len(df) >= 6:
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

    return {
        "symbol": symbol,
        "close": close,
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
        self.root.geometry("1450x780")
        self.root.minsize(1180, 680)
        self.results = []
        self._build_ui()

    def _build_ui(self):
        top = ttk.Frame(self.root, padding=12)
        top.pack(fill="x")

        ttk.Label(top, text="股票代號（逗號分隔）").pack(side="left", padx=(0, 8))

        self.symbol_entry = ttk.Entry(top, width=80)
        self.symbol_entry.pack(side="left", padx=(0, 8), fill="x", expand=True)
        self.symbol_entry.insert(0, "2330,2382,3231,2308,3017")

        ttk.Button(top, text="執行分析", command=self.run_analysis).pack(side="left", padx=(0, 8))
        ttk.Button(top, text="匯出報告", command=self.export_report).pack(side="left", padx=(0, 8))
        ttk.Button(top, text="清空", command=self.clear_results).pack(side="left")

        middle = ttk.Frame(self.root, padding=(12, 0, 12, 12))
        middle.pack(fill="both", expand=True)

        columns = ("代號", "收盤", "訊號", "分數", "支撐", "壓力", "RSI", "說明")
        self.tree = ttk.Treeview(middle, columns=columns, show="headings", height=24)

        widths = {
            "代號": 90,
            "收盤": 110,
            "訊號": 130,
            "分數": 80,
            "支撐": 110,
            "壓力": 110,
            "RSI": 90,
            "說明": 720,
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

    def run_analysis(self):
        symbols = self.parse_symbols()
        if not symbols:
            messagebox.showwarning("提醒", "請輸入至少一個股票代號。")
            return

        self.clear_results()
        self.set_status("開始抓取真實股票資料...")

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

        if errors:
            self.set_status(f"完成 {len(self.results)} 檔，失敗 {len(errors)} 檔。")
            messagebox.showwarning("部分股票失敗", "\n".join(errors[:10]))
        else:
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
        lines.append("資料來源：Yahoo Finance / yfinance")
        lines.append("=" * 100)

        for idx, r in enumerate(self.results, start=1):
            lines.append(f"{idx}. 股票代號：{r['symbol']}")
            lines.append(f"   收盤：{r['close']}")
            lines.append(f"   訊號：{r['signal']}")
            lines.append(f"   分數：{r['score']}")
            lines.append(f"   支撐：{r['support']}")
            lines.append(f"   壓力：{r['resistance']}")
            lines.append(f"   RSI：{r['rsi']}")
            lines.append(f"   說明：{r['comment']}")
            lines.append("-" * 100)

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

    GTCProApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
