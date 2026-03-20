
# v5.1.1 交易級邏輯重構版（核心升級版）
# 重點：
# 1. 主升候選（結構模型）
# 2. 建議引擎統一
# 3. 進出場計算
# 4. PDF格式升級（交易報告）

import math
from datetime import datetime

# =========================
# 主升候選模型（結構優先）
# =========================
def is_main_trend(stock):
    return (
        stock["波段分"] >= 85 and
        stock["盤中分"] >= 80 and
        50 <= stock["RSI"] <= 70 and
        stock["價格"] > stock["MA20"] > stock["MA60"] and
        stock["五檔"] >= 1.2
    )

# =========================
# 建議引擎（統一 mapping）
# =========================
def generate_advice(stock):
    signal = stock["訊號"]

    if signal == "強勢追蹤":
        if stock["價格"] > stock["壓力"]:
            return "不追高，等拉回"
        return "拉回布局"

    elif signal == "區間整理":
        return "區間操作"

    elif signal == "轉弱警戒":
        return "減碼/觀望"

    return "觀望"

# =========================
# 進出場計算（交易核心）
# =========================
def calc_trade_plan(stock):
    support = stock["支撐"]
    pressure = stock["壓力"]

    entry_low = support * 1.005
    entry_high = support * 1.015
    stop_loss = support * 0.98
    target = pressure

    rr = (target - entry_high) / (entry_high - stop_loss) if (entry_high - stop_loss) != 0 else 0

    return {
        "entry": (round(entry_low,2), round(entry_high,2)),
        "stop": round(stop_loss,2),
        "target": round(target,2),
        "rr": round(rr,2)
    }

# =========================
# PDF內容（交易報告格式）
# =========================
def build_report(stock):
    advice = generate_advice(stock)
    trade = calc_trade_plan(stock)

    report = []
    report.append(f"【交易結論】")
    report.append(f"建議：{advice}")
    report.append(f"訊號：{stock['訊號']}")
    report.append(f"狀態：{'主升' if is_main_trend(stock) else '非主升'}")

    report.append("\n【三大劇本】")
    report.append(f"A：守支撐 {stock['支撐']}")
    report.append(f"B：突破壓力 {stock['壓力']}")
    report.append(f"C：跌破轉弱")

    report.append("\n【交易計畫】")
    report.append(f"進場：{trade['entry'][0]} ~ {trade['entry'][1]}")
    report.append(f"停損：{trade['stop']}")
    report.append(f"目標：{trade['target']}")
    report.append(f"RR：1 : {trade['rr']}")

    report.append("\n【技術摘要】")
    report.append(f"波段分：{stock['波段分']}")
    report.append(f"盤中分：{stock['盤中分']}")
    report.append(f"RSI：{stock['RSI']}")
    report.append(f"五檔：{stock['五檔']}")

    return "\n".join(report)

# =========================
# 範例測試資料
# =========================
if __name__ == "__main__":
    sample = {
        "名稱": "3017 奇鋐",
        "價格": 2050,
        "波段分": 100,
        "盤中分": 100,
        "RSI": 65,
        "五檔": 1.3,
        "MA20": 1785,
        "MA60": 1558,
        "支撐": 1976,
        "壓力": 2060,
        "訊號": "強勢追蹤"
    }

    print(build_report(sample))
