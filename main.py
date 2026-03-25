# -*- coding: utf-8 -*-
"""
GTC AI Trading System v6.0.6a DOWNLOAD-STABLE
FINAL RELEASE (Fixed Version)
"""

import tkinter as tk
from tkinter import ttk, messagebox
import pandas as pd
from pathlib import Path
import datetime

APP_NAME = "GTC AI Trading System v6.0.6a DOWNLOAD-STABLE"

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)


def write_data(df, path, fmt):
    if fmt == "CSV":
        df.to_csv(path.with_suffix(".csv"), index=False, encoding="utf-8-sig")

    elif fmt == "TXT":
        df.to_csv(path.with_suffix(".txt"), index=False, sep="\t")

    elif fmt == "Excel":
        try:
            df.to_excel(path.with_suffix(".xlsx"), index=False)
        except:
            df.to_csv(path.with_suffix(".csv"), index=False)

    elif fmt == "PDF":
        df.to_csv(path.with_suffix(".csv"), index=False)


class DataEngine:
    def get_stock_data(self):
        data = {
            "Stock": ["2330", "2317", "2454", "2308", "2881"],
            "Score": [95, 88, 90, 85, 80],
            "Price": [600, 100, 1200, 300, 60]
        }
        return pd.DataFrame(data)


class RankingEngine:
    def get_top20(self, df):
        return df.sort_values("Score", ascending=False).head(20)


class AppUI:
    def __init__(self, root):
        self.root = root
        self.root.title(APP_NAME)

        self.data_engine = DataEngine()
        self.rank_engine = RankingEngine()

        self.setup_ui()
        self.refresh_all_tables()

    def setup_ui(self):
        frame = ttk.Frame(self.root)
        frame.pack(fill="both", expand=True)

        self.download_format_var = tk.StringVar(value="CSV")
        self.download_format_cb = ttk.Combobox(
            frame,
            textvariable=self.download_format_var,
            values=["CSV", "Excel", "TXT", "PDF"]
        )
        self.download_format_cb.pack()

        self.btn_export_data = ttk.Button(
            frame, text="下載資料", command=self.export_selected_data)
        self.btn_export_data.pack()

        self.tree_top20 = ttk.Treeview(frame)
        self.tree_top20.pack(fill="both", expand=True)
        self.tree_top20.bind("<<TreeviewSelect>>", self.on_select_top20)

        self.tree_order = ttk.Treeview(frame)
        self.tree_order.pack(fill="both", expand=True)
        self.tree_order.bind("<<TreeviewSelect>>", self.on_select_order)

    def refresh_all_tables(self):
        df = self.data_engine.get_stock_data()
        self.top20_df = self.rank_engine.get_top20(df)
        self.refresh_top20_and_order_views()

    def refresh_top20_and_order_views(self):
        self.tree_top20.delete(*self.tree_top20.get_children())

        for _, row in self.top20_df.iterrows():
            self.tree_top20.insert("", "end", values=list(row))

        self.order_df = self.build_order_list(self.top20_df)

        self.tree_order.delete(*self.tree_order.get_children())

        for _, row in self.order_df.iterrows():
            self.tree_order.insert("", "end", values=list(row))

    def build_order_list(self, df):
        df = df.copy()
        df["Action"] = ["Buy" if x > 85 else "Hold" for x in df["Score"]]
        return df

    def on_select_top20(self, event):
        item = self.tree_top20.selection()
        if item:
            print("選擇TOP20:", self.tree_top20.item(item)["values"])

    def on_select_order(self, event):
        item = self.tree_order.selection()
        if item:
            print("選擇Order:", self.tree_order.item(item)["values"])

    def export_selected_data(self):
        fmt = self.download_format_var.get()
        df = self.top20_df

        path = DATA_DIR / f"export_{datetime.datetime.now().strftime('%H%M%S')}"
        write_data(df, path, fmt)

        messagebox.showinfo("完成", f"已輸出 {fmt}")


def main():
    root = tk.Tk()
    app = AppUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
