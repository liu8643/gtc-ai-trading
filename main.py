
# main_v5_0_8_1.py
# v5.0.8.1 - Added dropdown export menu + full report export structure

import tkinter as tk
from tkinter import ttk, filedialog, messagebox

class StockApp:

    def __init__(self, root):
        self.root = root
        self.root.title("GTC 股票專業版看盤分析系統 v5.0.8.1")

        self.current_data = []
        self.selected_stock = None

        self.build_ui()

    def build_ui(self):
        top_frame = tk.Frame(self.root)
        top_frame.pack(fill="x", padx=5, pady=5)

        # 左側輸入
        self.entry = tk.Entry(top_frame, width=60)
        self.entry.pack(side="left", padx=5)

        # 中間按鈕
        tk.Button(top_frame, text="執行分析", command=self.run_analysis).pack(side="left", padx=3)
        tk.Button(top_frame, text="啟用自動刷新").pack(side="left", padx=3)
        tk.Button(top_frame, text="停止自動刷新").pack(side="left", padx=3)

        # 右側下載下拉選單
        self.download_btn = tk.Menubutton(top_frame, text="下載報告 ▼", relief="raised")
        self.download_menu = tk.Menu(self.download_btn, tearoff=0)
        self.download_btn.config(menu=self.download_menu)

        self.download_menu.add_command(label="PDF：總表摘要", command=self.export_pdf_summary)
        self.download_menu.add_command(label="PDF：目前選取個股", command=self.export_pdf_selected)
        self.download_menu.add_command(label="PDF：全部完整報告", command=self.export_pdf_full)
        self.download_menu.add_separator()
        self.download_menu.add_command(label="TXT：全部完整報告", command=self.export_txt_full)
        self.download_menu.add_command(label="CSV：主表資料", command=self.export_csv)

        self.download_btn.pack(side="right", padx=5)

        # 主表
        self.tree = ttk.Treeview(self.root, columns=("code", "name", "score"), show="headings")
        self.tree.heading("code", text="代號")
        self.tree.heading("name", text="名稱")
        self.tree.heading("score", text="分數")
        self.tree.pack(fill="both", expand=True)

        self.tree.bind("<<TreeviewSelect>>", self.on_select)

        # 下方區域
        bottom_frame = tk.Frame(self.root)
        bottom_frame.pack(fill="both", expand=True)

        self.left_text = tk.Text(bottom_frame, width=50)
        self.left_text.pack(side="left", fill="both", expand=True)

        self.right_text = tk.Text(bottom_frame, width=50)
        self.right_text.pack(side="right", fill="both", expand=True)

    def run_analysis(self):
        # mock data
        self.current_data = [
            {"code": "2308", "name": "台達電", "score": 93},
            {"code": "3017", "name": "奇鋐", "score": 100},
        ]

        for i in self.tree.get_children():
            self.tree.delete(i)

        for row in self.current_data:
            self.tree.insert("", "end", values=(row["code"], row["name"], row["score"]))

    def on_select(self, event):
        item = self.tree.selection()
        if not item:
            return
        values = self.tree.item(item[0], "values")
        self.selected_stock = values

        self.left_text.delete("1.0", tk.END)
        self.left_text.insert(tk.END, f"{values[0]} 個股明細...\n")

        self.right_text.delete("1.0", tk.END)
        self.right_text.insert(tk.END, f"{values[0]} 操作建議...\n")

    # ===== 匯出功能 =====

    def export_pdf_summary(self):
        messagebox.showinfo("匯出", "PDF：總表摘要")

    def export_pdf_selected(self):
        if not self.selected_stock:
            messagebox.warning("提示", "請先選擇個股")
            return
        messagebox.showinfo("匯出", f"PDF：個股 {self.selected_stock[0]}")

    def export_pdf_full(self):
        messagebox.showinfo("匯出", "PDF：全部完整報告")

    def export_txt_full(self):
        messagebox.showinfo("匯出", "TXT：全部完整報告")

    def export_csv(self):
        file = filedialog.asksaveasfilename(defaultextension=".csv")
        if not file:
            return
        with open(file, "w", encoding="utf-8") as f:
            f.write("code,name,score\n")
            for row in self.current_data:
                f.write(f"{row['code']},{row['name']},{row['score']}\n")

        messagebox.showinfo("完成", "CSV 已匯出")

if __name__ == "__main__":
    root = tk.Tk()
    app = StockApp(root)
    root.mainloop()
