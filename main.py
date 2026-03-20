# v5.0.2 完整版
# 已修正：
# 1. TWSE MIS 五檔欄位 mapping
# 2. 下方左右文字區加入垂直/水平 scrollbar
# 3. 大跌時強制壓低分數，避免誤判「強勢買進」

import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime
from functools import lru_cache
import pandas as pd
import yfinance as yf
import requests
from reportlab.lib.pagesizes import A4, landscape
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas
import os

APP_TITLE = "GTC 股票專業版看盤分析系統"
APP_VERSION = "v5.0.3-2-TW-Realtime-Fix2-AI-Wave-Fibo-Path"
AUTO_REFRESH_MS = 30000

def setup_pdf_font():
    candidates = [
        r"C:\Windows\Fonts\msjh.ttc",
        r"C:\Windows\Fonts\msjh.ttf",
        r"C:\Windows\Fonts\mingliu.ttc",
        r"C:\Windows\Fonts\kaiu.ttf",
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
            return [f"{s}.TWO", f"{s}.TW"]
        return [s]
    return [s]

@lru_cache(maxsize=1)
def get_tw_name_map():
    mapping = {}
    sources = [
        "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
        "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O",
    ]
    for url in sources:
        try:
            df = pd.read_json(url)
            code_col = None
            name_col = None
            for c in df.columns:
                c_str = str(c).strip()
                if code_col is None and ("代號" in c_str or "Code" in c_str):
                    code_col = c
                if name_col is None and ("簡稱" in c_str or "名稱" in c_str or "Name" in c_str):
                    name_col = c
            if code_col is None or name_col is None:
                continue
            for _, row in df.iterrows():
                code = str(row[code_col]).strip()
                name = str(row[name_col]).strip()
                if code.isdigit() and len(code) == 4 and name:
                    mapping[code] = name
        except Exception:
            continue
    return mapping

def get_stock_name(input_symbol: str, yf_symbol: str) -> str:
    if input_symbol.isdigit() and len(input_symbol) == 4:
        tw_map = get_tw_name_map()
        if input_symbol in tw_map:
            return tw_map[input_symbol]
    try:
        ticker = yf.Ticker(yf_symbol)
        info = ticker.info
        name = info.get("shortName") or info.get("longName")
        if name:
            return str(name)
    except Exception:
        pass
    return yf_symbol

def download_symbol_data(symbol: str, period: str = "12mo") -> tuple[str, pd.DataFrame]:
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
            if not all(c in df.columns for c in needed):
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

def round_price(v: float) -> float:
    return round(float(v), 2)

def safe_float(v, default=None):
    try:
        if v in (None, "", "-", "--"):
            return default
        return float(v)
    except Exception:
        return default

def safe_int(v, default=None):
    try:
        if v in (None, "", "-", "--"):
            return default
        return int(float(v))
    except Exception:
        return default

def split_prices(text):
    if not text:
        return []
    vals = []
    for x in str(text).split("_"):
        v = safe_float(x)
        if v is not None and v > 0:
            vals.append(round_price(v))
    return vals

def split_ints(text):
    if not text:
        return []
    vals = []
    for x in str(text).split("_"):
        v = safe_int(x)
        if v is not None and v >= 0:
            vals.append(v)
    return vals

def get_orderbook_bias(bid_vols, ask_vols):
    buy_qty = sum(bid_vols[:5]) if bid_vols else 0
    sell_qty = sum(ask_vols[:5]) if ask_vols else 0
    if buy_qty == 0 and sell_qty == 0:
        return {"buy_qty": 0, "sell_qty": 0, "ratio": "-", "bias": "無有效五檔"}
    if sell_qty == 0:
        return {"buy_qty": buy_qty, "sell_qty": sell_qty, "ratio": "∞", "bias": "買盤明顯偏強"}
    ratio = buy_qty / sell_qty
    if ratio >= 1.5:
        bias = "買盤偏強"
    elif ratio <= 0.67:
        bias = "賣盤偏強"
    else:
        bias = "多空均衡"
    return {"buy_qty": buy_qty, "sell_qty": sell_qty, "ratio": f"{ratio:.2f}", "bias": bias}

def detect_market(input_symbol: str, yf_symbol: str) -> str:
    if yf_symbol.endswith(".TW"):
        return "台股上市"
    if yf_symbol.endswith(".TWO"):
        return "台股上櫃"
    if input_symbol.isalpha():
        return "美股/海外"
    return "其他"

def get_tw_realtime_quote(symbol: str, market: str) -> dict | None:
    if market not in ("台股上市", "台股上櫃"):
        return None
    ex_prefix = "tse" if market == "台股上市" else "otc"
    ex_ch = f"{ex_prefix}_{symbol}.tw"
    url = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
    params = {"ex_ch": ex_ch, "json": "1", "delay": "0", "_": str(int(datetime.now().timestamp() * 1000))}
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://mis.twse.com.tw/stock/index.jsp"}
    try:
        r = requests.get(url, params=params, headers=headers, timeout=8)
        r.raise_for_status()
        data = r.json()
        msg_array = data.get("msgArray", [])
        if not msg_array:
            return None
        item = msg_array[0]
        last_trade = safe_float(item.get("z"))
        open_price = safe_float(item.get("o"))
        high_price = safe_float(item.get("h"))
        low_price = safe_float(item.get("l"))
        prev_close = safe_float(item.get("y"))
        ask_prices = split_prices(item.get("a"))
        bid_prices = split_prices(item.get("b"))
        ask_vols = split_ints(item.get("f"))
        bid_vols = split_ints(item.get("g"))
        indicative_price = None
        if bid_prices and ask_prices:
            indicative_price = round_price((bid_prices[0] + ask_prices[0]) / 2)
        elif bid_prices:
            indicative_price = bid_prices[0]
        elif ask_prices:
            indicative_price = ask_prices[0]
        if last_trade is not None:
            display_price = round_price(last_trade)
            display_note = "即時成交價"
        elif indicative_price is not None:
            display_price = round_price(indicative_price)
            display_note = "當下無成交，改用買一/賣一中間價"
        elif prev_close is not None:
            display_price = round_price(prev_close)
            display_note = "當下無成交且無五檔，暫以昨收顯示"
        else:
            return None
        ob = get_orderbook_bias(bid_vols, ask_vols)
        return {
            "close": display_price,
            "display_price": display_price,
            "display_note": display_note,
            "last_trade": round_price(last_trade) if last_trade is not None else None,
            "indicative_price": round_price(indicative_price) if indicative_price is not None else None,
            "prev_close": round_price(prev_close if prev_close is not None else display_price),
            "open": round_price(open_price if open_price is not None else display_price),
            "high": round_price(high_price if high_price is not None else display_price),
            "low": round_price(low_price if low_price is not None else display_price),
            "bid_prices": bid_prices,
            "ask_prices": ask_prices,
            "bid_vols": bid_vols,
            "ask_vols": ask_vols,
            "buy_qty": ob["buy_qty"],
            "sell_qty": ob["sell_qty"],
            "orderbook_ratio": ob["ratio"],
            "orderbook_bias": ob["bias"],
            "quote_time": item.get("t") or item.get("tt") or "",
            "source": "TWSE MIS 即時",
        }
    except Exception:
        return None

def get_us_yahoo_quote(yf_symbol: str, fallback_close: float, fallback_prev_close: float, fallback_open: float, fallback_high: float, fallback_low: float) -> dict:
    live_price = fallback_close
    prev_close = fallback_prev_close
    open_price = fallback_open
    high_price = fallback_high
    low_price = fallback_low
    try:
        ticker = yf.Ticker(yf_symbol)
        try:
            fi = ticker.fast_info
            if fi:
                lp = fi.get("lastPrice")
                pc = fi.get("previousClose")
                day_high = fi.get("dayHigh")
                day_low = fi.get("dayLow")
                day_open = fi.get("open")
                if lp is not None:
                    live_price = round(float(lp), 2)
                if pc is not None:
                    prev_close = round(float(pc), 2)
                if day_high is not None:
                    high_price = round(float(day_high), 2)
                if day_low is not None:
                    low_price = round(float(day_low), 2)
                if day_open is not None:
                    open_price = round(float(day_open), 2)
        except Exception:
            pass
        try:
            info = ticker.info
            rp = info.get("regularMarketPrice")
            pcp = info.get("regularMarketPreviousClose")
            day_high = info.get("regularMarketDayHigh")
            day_low = info.get("regularMarketDayLow")
            day_open = info.get("regularMarketOpen")
            if rp is not None:
                live_price = round(float(rp), 2)
            if pcp is not None:
                prev_close = round(float(pcp), 2)
            if day_high is not None:
                high_price = round(float(day_high), 2)
            if day_low is not None:
                low_price = round(float(day_low), 2)
            if day_open is not None:
                open_price = round(float(day_open), 2)
        except Exception:
            pass
    except Exception:
        pass
    return {"close": live_price, "prev_close": prev_close, "open": open_price, "high": high_price, "low": low_price, "source": "Yahoo Finance"}

def calc_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["MA5"] = df["Close"].rolling(5).mean()
    df["MA10"] = df["Close"].rolling(10).mean()
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

def calc_professional_sr(df: pd.DataFrame) -> dict:
    recent20 = df.tail(20)
    recent40 = df.tail(40)
    close = float(df["Close"].iloc[-1])
    support_20 = float(recent20["Low"].min())
    resistance_20 = float(recent20["High"].max())
    swing_low = float(recent40["Low"].min())
    swing_high = float(recent40["High"].max())
    last_bar = df.iloc[-1]
    pivot = (float(last_bar["High"]) + float(last_bar["Low"]) + float(last_bar["Close"])) / 3
    r1 = pivot * 2 - float(last_bar["Low"])
    s1 = pivot * 2 - float(last_bar["High"])
    support_candidates = [support_20, swing_low, s1]
    resistance_candidates = [resistance_20, swing_high, r1]
    supports_below = [x for x in support_candidates if x <= close]
    main_support = max(supports_below) if supports_below else min(support_candidates)
    resistances_above = [x for x in resistance_candidates if x >= close]
    main_resistance = min(resistances_above) if resistances_above else max(resistance_candidates)
    return {
        "support": round_price(main_support),
        "resistance": round_price(main_resistance),
        "support20": round_price(support_20),
        "resistance20": round_price(resistance_20),
        "swing_low": round_price(swing_low),
        "swing_high": round_price(swing_high),
        "pivot": round_price(pivot),
        "s1": round_price(s1),
        "r1": round_price(r1),
    }

def build_trade_advice(close, ma20, ma60, score, rsi, support, resistance, change_pct):
    if change_pct <= -9.0:
        return "跌幅過大，先觀望/減碼"
    if close < support:
        return "減碼 / 停損優先"
    if close > resistance:
        return "突破追蹤 / 拉回不破可偏多"
    if close >= ma20 and close >= ma60 and score >= 80:
        return "強勢買進"
    if close >= ma20 and score >= 65:
        return "拉回布局"
    if score >= 45 and support <= close <= resistance:
        return "區間觀察"
    if rsi > 70:
        return "減碼/停利"
    if close < ma20 and close < ma60 and change_pct < 0:
        return "轉弱觀望"
    return "保守觀察"

def build_risk_note(close, support, resistance, rsi, score, change_pct=None):
    notes = []
    if change_pct is not None and change_pct <= -7:
        notes.append("當日跌幅偏大，短線波動風險升高")
    if change_pct is not None and change_pct <= -9:
        notes.append("接近或達跌停級別，避免把急跌誤判為強勢買點")
    if close <= support * 1.01:
        notes.append("接近支撐，觀察是否守穩")
    if close < support:
        notes.append("已跌破支撐，需提高風險控管")
    if close >= resistance * 0.99:
        notes.append("逼近壓力，留意獲利了結賣壓")
    if close > resistance:
        notes.append("已突破壓力，觀察是否假突破")
    if rsi >= 70:
        notes.append("RSI 偏高，短線過熱風險上升")
    if rsi <= 30:
        notes.append("RSI 偏低，可能進入超跌區")
    if score < 30:
        notes.append("綜合評分偏弱，不宜積極追價")
    if not notes:
        notes.append("目前技術面無明顯異常，但仍須控管部位")
    return "；".join(notes)

def build_ai_analysis(data: dict) -> str:
    close = data["close"]
    ma20 = data["ma20"]
    ma60 = data["ma60"]
    rsi = data["rsi"]
    score = data["score"]
    support = data["support"]
    resistance = data["resistance"]
    signal = data["signal"]
    advice = data["advice"]
    orderbook_bias = data.get("orderbook_bias", "無")
    orderbook_ratio = data.get("orderbook_ratio", "-")
    change_pct = data.get("change_pct", 0.0)
    if close >= ma20 and close >= ma60:
        trend_text = "目前股價位於20日線與60日線之上，中期趨勢偏強。"
        trend = "偏多"
    elif close >= ma20 and close < ma60:
        trend_text = "目前股價站上20日線，但仍在60日線下方，屬短強中性結構。"
        trend = "盤整偏多"
    elif close < ma20 and close >= ma60:
        trend_text = "目前股價跌破20日線但仍守住60日線，短線轉弱、中期待觀察。"
        trend = "盤整偏弱"
    else:
        trend_text = "目前股價位於20日線與60日線下方，技術面偏弱。"
        trend = "偏空"
    if close < support:
        pos_text = f"目前股價 {close} 已跌破支撐 {support}，位置偏弱。"
    elif close > resistance:
        pos_text = f"目前股價 {close} 已突破壓力 {resistance}，位置轉強。"
    else:
        pos_text = f"目前股價位於支撐 {support} 與壓力 {resistance} 之間，仍屬區間內。"
    if rsi >= 70:
        rsi_text = f"RSI為 {rsi}，已接近或進入過熱區，短線需留意震盪與拉回。"
    elif rsi <= 30:
        rsi_text = f"RSI為 {rsi}，已進入相對低檔區，若量價配合有機會出現反彈。"
    elif rsi >= 55:
        rsi_text = f"RSI為 {rsi}，動能偏強，但仍需觀察是否能持續放大。"
    elif rsi >= 40:
        rsi_text = f"RSI為 {rsi}，動能中性偏弱，屬整理觀察區。"
    else:
        rsi_text = f"RSI為 {rsi}，動能偏弱，短線仍需保守。"
    ob_text = f"五檔力道為「{orderbook_bias}」，委買/委賣比為 {orderbook_ratio}。"
    if score >= 80:
        score_text = "綜合評分屬高分區，結構偏強。"
    elif score >= 65:
        score_text = "綜合評分中上，偏多但仍需確認續航力。"
    elif score >= 45:
        score_text = "綜合評分中性，屬區間整理型。"
    else:
        score_text = "綜合評分偏弱，先以風險控制優先。"
    if change_pct <= -9:
        drop_text = f"當日跌幅 {change_pct:+.2f}% 已屬高風險急跌，不宜僅因均線與歷史分數誤判為強勢買點。"
    elif change_pct <= -5:
        drop_text = f"當日跌幅 {change_pct:+.2f}% 偏大，需提高風險意識。"
    elif change_pct >= 5:
        drop_text = f"當日漲幅 {change_pct:+.2f}% 偏強，需觀察是否放量續攻。"
    else:
        drop_text = f"當日漲跌幅 {change_pct:+.2f}% 屬正常波動區間。"
    final_text = f"AI綜合判斷：趨勢偏向「{trend}」，訊號為「{signal}」，建議採取「{advice}」策略。"
    return "\n".join([
        "【AI個股分析】",
        f"1. 趨勢判讀：{trend_text}",
        f"2. 位置判讀：{pos_text}",
        f"3. 動能狀態：{rsi_text}",
        f"4. 五檔力道：{ob_text}",
        f"5. 當日強弱：{drop_text}",
        f"6. 分數解讀：{score_text}",
        f"7. AI結論：{final_text}",
    ])

def detect_local_pivots(series: pd.Series, left: int = 2, right: int = 2):
    pivots = []
    values = series.tolist()
    for i in range(left, len(values) - right):
        window = values[i - left:i + right + 1]
        center = values[i]
        if center == max(window):
            pivots.append((i, "H", float(center)))
        elif center == min(window):
            pivots.append((i, "L", float(center)))
    return pivots

def summarize_wave(df: pd.DataFrame, period: int, label: str) -> str:
    part = df.tail(period).copy()
    if len(part) < 15:
        return f"{label}：資料不足，暫無法判讀。"
    close_start = float(part["Close"].iloc[0])
    close_end = float(part["Close"].iloc[-1])
    highest = float(part["High"].max())
    lowest = float(part["Low"].min())
    amplitude_pct = ((highest - lowest) / lowest * 100) if lowest != 0 else 0
    ma20_last = float(part["Close"].rolling(20).mean().iloc[-1]) if len(part) >= 20 else close_end
    ma60_last = float(part["Close"].rolling(60).mean().iloc[-1]) if len(part) >= 60 else close_end
    pivots = detect_local_pivots(part["Close"], left=2, right=2)
    recent_pivots = pivots[-6:] if len(pivots) >= 6 else pivots
    if close_end > close_start and close_end >= ma20_last:
        if len(recent_pivots) >= 5:
            wave_hint = "較偏推動浪結構，可能處於第3浪或第5浪延伸區。"
        else:
            wave_hint = "偏多推升結構，可能處於推動浪初升段。"
    elif close_end < close_start and close_end < ma20_last:
        if len(recent_pivots) >= 4:
            wave_hint = "較偏修正浪結構，可能位於 A / C 浪下修階段。"
        else:
            wave_hint = "偏弱修正結構，較像回檔整理波。"
    else:
        wave_hint = "目前較像整理浪或轉折確認階段，尚未形成明確單邊波段。"
    if close_end >= ma20_last and close_end >= ma60_last:
        trend_hint = "均線結構偏多。"
    elif close_end >= ma20_last and close_end < ma60_last:
        trend_hint = "短線偏強，但中期壓力仍在。"
    elif close_end < ma20_last and close_end >= ma60_last:
        trend_hint = "短線轉弱，中期尚未完全破壞。"
    else:
        trend_hint = "短中期均線結構偏弱。"
    return f"{label}：區間波動約 {amplitude_pct:.2f}% ，{wave_hint}{trend_hint}"

def build_wave_analysis(df: pd.DataFrame) -> str:
    return "\n".join([
        "【波浪理論分析】",
        f"1. {summarize_wave(df, 20, '短期')}",
        f"2. {summarize_wave(df, 60, '中期')}",
        f"3. {summarize_wave(df, 120, '長期')}",
    ])

def calc_fibonacci_targets(df: pd.DataFrame) -> dict:
    lookback = df.tail(120).copy()
    if len(lookback) < 30:
        close_now = float(df["Close"].iloc[-1])
        return {
            "direction": "資料不足",
            "base_low": round_price(close_now),
            "base_high": round_price(close_now),
            "range": 0.0,
            "target_1_0": round_price(close_now),
            "target_1_382": round_price(close_now),
            "target_1_618": round_price(close_now),
            "next_target": round_price(close_now),
            "summary": "資料不足，暫無法估算費波南西目標位。",
        }
    close_now = float(lookback["Close"].iloc[-1])
    low_val = float(lookback["Low"].min())
    high_val = float(lookback["High"].max())
    price_range = high_val - low_val
    low_idx = lookback["Low"].idxmin()
    high_idx = lookback["High"].idxmax()
    upward = low_idx < high_idx
    if price_range <= 0:
        return {
            "direction": "整理",
            "base_low": round_price(low_val),
            "base_high": round_price(high_val),
            "range": round_price(price_range),
            "target_1_0": round_price(close_now),
            "target_1_382": round_price(close_now),
            "target_1_618": round_price(close_now),
            "next_target": round_price(close_now),
            "summary": "區間過小，暫不適合估算費波南西延伸目標。",
        }
    if upward:
        direction = "上升波"
        target_1_0 = high_val
        target_1_382 = low_val + price_range * 1.382
        target_1_618 = low_val + price_range * 1.618
        if close_now < target_1_0:
            next_target = target_1_0
        elif close_now < target_1_382:
            next_target = target_1_382
        else:
            next_target = target_1_618
        summary = f"目前較偏上升波段，近波段低點 {round_price(low_val)} 至高點 {round_price(high_val)}。若續強，下一觀察目標依序為 1.0={round_price(target_1_0)}、1.382={round_price(target_1_382)}、1.618={round_price(target_1_618)}。"
    else:
        direction = "下降波"
        target_1_0 = low_val
        target_1_382 = high_val - price_range * 1.382
        target_1_618 = high_val - price_range * 1.618
        if close_now > target_1_0:
            next_target = target_1_0
        elif close_now > target_1_382:
            next_target = target_1_382
        else:
            next_target = target_1_618
        summary = f"目前較偏下降修正波，近波段高點 {round_price(high_val)} 至低點 {round_price(low_val)}。若續弱，下一觀察目標依序為 1.0={round_price(target_1_0)}、1.382={round_price(target_1_382)}、1.618={round_price(target_1_618)}。"
    return {
        "direction": direction,
        "base_low": round_price(low_val),
        "base_high": round_price(high_val),
        "range": round_price(price_range),
        "target_1_0": round_price(target_1_0),
        "target_1_382": round_price(target_1_382),
        "target_1_618": round_price(target_1_618),
        "next_target": round_price(next_target),
        "summary": summary,
    }

def build_fibonacci_analysis(fibo: dict) -> str:
    return "\n".join([
        "【費波南西目標位】",
        f"1. 波段方向：{fibo['direction']}",
        f"2. 波段低點：{fibo['base_low']} / 波段高點：{fibo['base_high']}",
        f"3. 1.0 目標位：{fibo['target_1_0']}",
        f"4. 1.382 目標位：{fibo['target_1_382']}",
        f"5. 1.618 目標位：{fibo['target_1_618']}",
        f"6. 下一目標價：{fibo['next_target']}",
        f"7. 判讀：{fibo['summary']}",
    ])

def build_bull_bear_path(data: dict) -> str:
    support = data["support"]
    resistance = data["resistance"]
    next_target = data["fibo"]["next_target"]
    signal = data["signal"]
    advice = data["advice"]
    return "\n".join([
        "【多空路徑圖示】",
        "◎ 多方路徑：",
        f"→ 多方路徑①：守住支撐 {support}",
        f"→ 多方路徑②：重新挑戰壓力 {resistance}",
        f"→ 多方路徑③：若有效突破壓力，下一目標看 {next_target}",
        "",
        "◎ 空方路徑：",
        f"→ 空方路徑①：若跌破支撐 {support}",
        "→ 空方路徑②：短線結構轉弱，恐回測更低整理區",
        f"→ 空方路徑③：若反彈無法站回壓力 {resistance}，弱勢格局延續",
        "",
        f"【路徑結論】當前訊號為「{signal}」，操作建議為「{advice}」。",
    ])

def analyze_symbol(symbol: str) -> dict:
    yf_symbol, df = download_symbol_data(symbol)
    market = detect_market(symbol, yf_symbol)
    stock_name = get_stock_name(symbol, yf_symbol)
    df = calc_indicators(df)
    last = df.iloc[-1]
    fallback_close = round_price(last["Close"])
    fallback_prev_close = round_price(df.iloc[-2]["Close"]) if len(df) >= 2 else fallback_close
    fallback_open = round_price(last["Open"])
    fallback_high = round_price(last["High"])
    fallback_low = round_price(last["Low"])
    if market in ("台股上市", "台股上櫃"):
        rt = get_tw_realtime_quote(symbol, market)
        if rt is None:
            rt = {
                "close": fallback_close, "display_price": fallback_close, "display_note": "日線回退",
                "last_trade": None, "indicative_price": None, "prev_close": fallback_prev_close,
                "open": fallback_open, "high": fallback_high, "low": fallback_low,
                "bid_prices": [], "ask_prices": [], "bid_vols": [], "ask_vols": [],
                "buy_qty": 0, "sell_qty": 0, "orderbook_ratio": "-", "orderbook_bias": "無有效五檔",
                "quote_time": "", "source": "日線回退",
            }
    else:
        rt = get_us_yahoo_quote(yf_symbol, fallback_close, fallback_prev_close, fallback_open, fallback_high, fallback_low)
        rt["display_price"] = rt["close"]
        rt["display_note"] = "即時/近即時成交價"
        rt["last_trade"] = rt["close"]
        rt["indicative_price"] = rt["close"]
        rt["bid_prices"] = []
        rt["ask_prices"] = []
        rt["bid_vols"] = []
        rt["ask_vols"] = []
        rt["buy_qty"] = 0
        rt["sell_qty"] = 0
        rt["orderbook_ratio"] = "-"
        rt["orderbook_bias"] = "不適用"
        rt["quote_time"] = ""
    close = rt["close"]
    prev_close = rt["prev_close"]
    open_price = rt["open"]
    high_price = rt["high"]
    low_price = rt["low"]
    change = round_price(close - prev_close)
    change_pct = round((change / prev_close) * 100, 2) if prev_close != 0 else 0.0
    ma5 = round_price(last["MA5"]) if pd.notna(last["MA5"]) else close
    ma10 = round_price(last["MA10"]) if pd.notna(last["MA10"]) else close
    ma20 = round_price(last["MA20"]) if pd.notna(last["MA20"]) else close
    ma60 = round_price(last["MA60"]) if pd.notna(last["MA60"]) else close
    rsi = round(float(last["RSI"]), 2) if pd.notna(last["RSI"]) else 50.0
    sr = calc_professional_sr(df)
    support = sr["support"]
    resistance = sr["resistance"]
    score = 50
    comments = []
    if close >= ma5:
        score += 4; comments.append("站上5日線")
    else:
        score -= 4; comments.append("跌破5日線")
    if close >= ma10:
        score += 6; comments.append("站上10日線")
    else:
        score -= 5; comments.append("跌破10日線")
    if close >= ma20:
        score += 10; comments.append("站上20日線")
    else:
        score -= 10; comments.append("跌破20日線")
    if close >= ma60:
        score += 15; comments.append("站上60日線")
    else:
        score -= 12; comments.append("跌破60日線")
    if float(last["MACD"]) >= float(last["MACD_SIGNAL"]):
        score += 8; comments.append("MACD偏多")
    else:
        score -= 6; comments.append("MACD偏弱")
    if pd.notna(last["K"]) and pd.notna(last["D"]):
        if float(last["K"]) >= float(last["D"]):
            score += 6; comments.append("KD偏多")
        else:
            score -= 4; comments.append("KD偏空")
    if rsi < 30:
        score += 8; comments.append("RSI超跌")
    elif rsi > 70:
        score -= 8; comments.append("RSI過熱")
    if len(df) >= 20:
        vol5 = df["Volume"].tail(5).mean()
        vol20 = df["Volume"].tail(20).mean()
        if pd.notna(vol5) and pd.notna(vol20) and vol5 > vol20:
            score += 4; comments.append("量能放大")
    if rt.get("orderbook_bias") == "買盤偏強":
        score += 5; comments.append("五檔買盤偏強")
    elif rt.get("orderbook_bias") == "買盤明顯偏強":
        score += 8; comments.append("五檔買盤明顯偏強")
    elif rt.get("orderbook_bias") == "賣盤偏強":
        score -= 6; comments.append("五檔賣盤偏強")
    if change_pct <= -9.0:
        score -= 50; comments.append("當日大跌>9%，強制壓低分數")
    elif change_pct <= -7.0:
        score -= 30; comments.append("當日大跌>7%，顯著壓低分數")
    elif change_pct <= -5.0:
        score -= 18; comments.append("當日跌幅偏大，降低分數")
    if close < open_price:
        score -= 6; comments.append("現價低於開盤")
    if close < prev_close:
        score -= 6; comments.append("現價低於昨收")
    if close <= low_price * 1.01:
        score -= 8; comments.append("接近日低，弱勢")
    if close < support:
        score -= 15; comments.append("跌破主支撐")
    score = max(0, min(100, int(score)))
    if change_pct <= -9.0:
        signal = "急跌風險"
    elif close < support:
        signal = "跌破支撐"
    elif close > resistance:
        signal = "突破壓力"
    elif score >= 80:
        signal = "強勢買進"
    elif score >= 65:
        signal = "偏多觀察"
    elif score >= 45:
        signal = "區間整理"
    elif score >= 30:
        signal = "保守/減碼"
    else:
        signal = "弱勢觀望"
    advice = build_trade_advice(close, ma20, ma60, score, rsi, support, resistance, change_pct)
    risk_note = build_risk_note(close, support, resistance, rsi, score, change_pct)
    extra_comment = f"{'；'.join(comments)}；20日支撐={sr['support20']}；20日壓力={sr['resistance20']}；波段低點={sr['swing_low']}；波段高點={sr['swing_high']}；Pivot={sr['pivot']}；來源={rt['source']}"
    fibo = calc_fibonacci_targets(df)
    result = {
        "input_symbol": symbol, "name": stock_name, "yf_symbol": yf_symbol, "market": market,
        "close": close, "display_price": rt.get("display_price", close), "display_note": rt.get("display_note", ""),
        "last_trade": rt.get("last_trade"), "indicative_price": rt.get("indicative_price"),
        "prev_close": prev_close, "open": open_price, "high": high_price, "low": low_price,
        "change": change, "change_pct": change_pct, "signal": signal, "advice": advice, "score": score,
        "support": support, "resistance": resistance, "rsi": rsi, "ma5": ma5, "ma10": ma10,
        "ma20": ma20, "ma60": ma60, "comment": extra_comment, "risk_note": risk_note,
        "source": rt["source"], "fibo": fibo, "bid_prices": rt.get("bid_prices", []),
        "ask_prices": rt.get("ask_prices", []), "bid_vols": rt.get("bid_vols", []),
        "ask_vols": rt.get("ask_vols", []), "buy_qty": rt.get("buy_qty", 0),
        "sell_qty": rt.get("sell_qty", 0), "orderbook_ratio": rt.get("orderbook_ratio", "-"),
        "orderbook_bias": rt.get("orderbook_bias", "無"), "quote_time": rt.get("quote_time", ""),
    }
    result["ai_analysis"] = build_ai_analysis(result)
    result["wave_analysis"] = build_wave_analysis(df)
    result["fibo_analysis"] = build_fibonacci_analysis(fibo)
    result["path_analysis"] = build_bull_bear_path(result)
    return result

class GTCProApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title(f"{APP_TITLE} {APP_VERSION}")
        self.root.geometry("1920x1000")
        self.root.minsize(1500, 820)
        self.results = []
        self.current_sort_column = None
        self.sort_reverse = True
        self.auto_refresh_enabled = False
        self.next_refresh_sec = AUTO_REFRESH_MS // 1000
        self.last_update_time = None
        self._timer_job_id = None
        self._build_ui()
        self.set_status(f"系統已就緒。當前版本：{APP_VERSION}")

    def _build_ui(self):
        top = ttk.Frame(self.root, padding=10)
        top.pack(fill="x")
        ttk.Label(top, text="股票代號（逗號分隔）").pack(side="left", padx=(0, 8))
        self.symbol_entry = ttk.Entry(top, width=100)
        self.symbol_entry.pack(side="left", padx=(0, 8), fill="x", expand=True)
        self.symbol_entry.insert(0, "2330,2382,3231,2308,3017,4979,AAPL,NVDA,MSFT")
        ttk.Button(top, text="執行分析", command=self.run_analysis).pack(side="left", padx=(0, 8))
        ttk.Button(top, text="啟用自動刷新", command=self.enable_auto_refresh).pack(side="left", padx=(0, 8))
        ttk.Button(top, text="停止自動刷新", command=self.disable_auto_refresh).pack(side="left", padx=(0, 8))
        ttk.Button(top, text="匯出 PDF", command=self.export_pdf).pack(side="left", padx=(0, 8))
        ttk.Button(top, text="匯出 TXT", command=self.export_txt).pack(side="left", padx=(0, 8))
        ttk.Button(top, text="清空", command=self.clear_results).pack(side="left")
        center = ttk.Panedwindow(self.root, orient=tk.VERTICAL)
        center.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        top_frame = ttk.Frame(center)
        bottom_frame = ttk.Frame(center)
        center.add(top_frame, weight=3)
        center.add(bottom_frame, weight=2)
        self._build_table_area(top_frame)
        self._build_detail_area(bottom_frame)

    def _build_table_area(self, parent):
        columns = ("排名", "燈號", "市場", "代號", "名稱", "顯示價", "報價說明", "昨收", "漲跌", "漲跌幅%", "訊號", "建議", "分數", "支撐", "壓力", "RSI", "五檔力道")
        self.tree = ttk.Treeview(parent, columns=columns, show="headings", height=16)
        widths = {"排名": 55, "燈號": 55, "市場": 90, "代號": 80, "名稱": 180, "顯示價": 90, "報價說明": 180, "昨收": 90, "漲跌": 90, "漲跌幅%": 95, "訊號": 100, "建議": 140, "分數": 65, "支撐": 90, "壓力": 90, "RSI": 70, "五檔力道": 110}
        for c in columns:
            self.tree.heading(c, text=c, command=lambda col=c: self.sort_by_column(col))
            self.tree.column(c, width=widths[c], anchor="center")
        self.tree.tag_configure("up", foreground="red", background="#ffecec")
        self.tree.tag_configure("down", foreground="green", background="#ecffec")
        self.tree.tag_configure("flat", foreground="black", background="white")
        self.tree.tag_configure("strong", background="#fff2b3")
        self.tree.tag_configure("watch", background="#eef5ff")
        self.tree.tag_configure("danger", background="#ffd9d9")
        self.tree.bind("<<TreeviewSelect>>", self.on_row_select)
        yscroll = ttk.Scrollbar(parent, orient="vertical", command=self.tree.yview)
        xscroll = ttk.Scrollbar(parent, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")
        parent.rowconfigure(0, weight=1)
        parent.columnconfigure(0, weight=1)

    def _build_detail_area(self, parent):
        left = ttk.LabelFrame(parent, text="個股明細分析", padding=10)
        right = ttk.LabelFrame(parent, text="操作建議 / 風險提醒", padding=10)
        left.pack(side="left", fill="both", expand=True, padx=(0, 5))
        right.pack(side="left", fill="both", expand=True, padx=(5, 0))
        left_text_frame = ttk.Frame(left)
        left_text_frame.pack(fill="both", expand=True)
        self.detail_text = tk.Text(left_text_frame, height=14, wrap="none", font=("Microsoft JhengHei", 10))
        left_y_scroll = ttk.Scrollbar(left_text_frame, orient="vertical", command=self.detail_text.yview)
        left_x_scroll = ttk.Scrollbar(left_text_frame, orient="horizontal", command=self.detail_text.xview)
        self.detail_text.configure(yscrollcommand=left_y_scroll.set, xscrollcommand=left_x_scroll.set)
        self.detail_text.grid(row=0, column=0, sticky="nsew")
        left_y_scroll.grid(row=0, column=1, sticky="ns")
        left_x_scroll.grid(row=1, column=0, sticky="ew")
        left_text_frame.rowconfigure(0, weight=1)
        left_text_frame.columnconfigure(0, weight=1)
        right_text_frame = ttk.Frame(right)
        right_text_frame.pack(fill="both", expand=True)
        self.advice_text = tk.Text(right_text_frame, height=14, wrap="none", font=("Microsoft JhengHei", 10))
        right_y_scroll = ttk.Scrollbar(right_text_frame, orient="vertical", command=self.advice_text.yview)
        right_x_scroll = ttk.Scrollbar(right_text_frame, orient="horizontal", command=self.advice_text.xview)
        self.advice_text.configure(yscrollcommand=right_y_scroll.set, xscrollcommand=right_x_scroll.set)
        self.advice_text.grid(row=0, column=0, sticky="nsew")
        right_y_scroll.grid(row=0, column=1, sticky="ns")
        right_x_scroll.grid(row=1, column=0, sticky="ew")
        right_text_frame.rowconfigure(0, weight=1)
        right_text_frame.columnconfigure(0, weight=1)
        bottom = ttk.LabelFrame(self.root, text="系統訊息", padding=10)
        bottom.pack(fill="x", padx=10, pady=(0, 10))
        self.status_var = tk.StringVar(value="")
        ttk.Label(bottom, textvariable=self.status_var).pack(anchor="w")

    def set_status(self, text: str):
        now = datetime.now().strftime("%H:%M:%S")
        self.status_var.set(f"[{now}] {text}")

    
def get_light(self, signal, score, change_pct):
    if change_pct <= -4:
        return "🟣"
    if change_pct >= 3:
        return "🔵"
    if score >= 80 and change_pct > 0:
        return "🟢"
    if score >= 45:
        return "🟡"
    return "🔴"

def update_status_with_timer(self):
        if self.last_update_time:
            last = self.last_update_time.strftime("%H:%M:%S")
        else:
            last = "-"
        mode = "自動刷新開啟" if self.auto_refresh_enabled else "自動刷新關閉"
        self.status_var.set(f"最後更新：{last} ｜ 下次刷新：{self.next_refresh_sec} 秒 ｜ {mode} ｜ 版本：{APP_VERSION}")

    def clear_results(self):
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.results = []
        self.detail_text.delete("1.0", tk.END)
        self.advice_text.delete("1.0", tk.END)
        self.set_status(f"已清空結果。版本：{APP_VERSION}")

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
            if r["signal"] == "急跌風險":
                tags.append("danger")
            elif r["score"] >= 80:
                tags.append("strong")
            elif r["score"] >= 65:
                tags.append("watch")
            light = self.get_light(r["signal"], r["score"], r["change_pct"])
            self.tree.insert("", "end", values=(idx, light, r["market"], r["input_symbol"], r["name"], r["display_price"], r["display_note"], r["prev_close"], f"{r['change']:+.2f}", f"{r['change_pct']:+.2f}%", r["signal"], r["advice"], r["score"], r["support"], r["resistance"], r["rsi"], r["orderbook_bias"]), tags=tuple(tags))

    def run_analysis(self):
        symbols = self.parse_symbols()
        if not symbols:
            messagebox.showwarning("提醒", "請輸入至少一個股票代號。")
            return
        self.clear_results()
        self.set_status(f"開始抓取即時股票資料... 版本：{APP_VERSION}")
        self.root.update_idletasks()
        ok_results, errors = [], []
        for sym in symbols:
            try:
                result = analyze_symbol(sym)
                ok_results.append(result)
                self.set_status(f"完成：{sym} / 版本：{APP_VERSION}")
                self.root.update_idletasks()
            except Exception as e:
                errors.append(f"{sym}: {e}")
        self.results = sorted(ok_results, key=lambda x: (x["score"] * 0.6 + x["change_pct"] * 0.4), reverse=True)
        self.render_results()
        self.last_update_time = datetime.now()
        self.next_refresh_sec = AUTO_REFRESH_MS // 1000
        self.update_status_with_timer()
        if self.results:
            first_id = self.tree.get_children()[0]
            self.tree.selection_set(first_id)
            self.tree.focus(first_id)
            self.on_row_select()
        if errors:
            self.set_status(f"完成 {len(self.results)} 檔，失敗 {len(errors)} 檔。版本：{APP_VERSION}")
            messagebox.showwarning("部分股票失敗", "\n".join(errors[:10]))
        else:
            self.set_status(f"分析完成，共 {len(self.results)} 檔。版本：{APP_VERSION}")

    
def enable_auto_refresh(self):
    self.auto_refresh_enabled = True
    self.next_refresh_sec = AUTO_REFRESH_MS // 1000
    self.update_status_with_timer()
    if self._timer_job_id is not None:
        try:
            self.root.after_cancel(self._timer_job_id)
        except Exception:
            pass
    self._timer_job_id = self.root.after(1000, self.auto_refresh_job)

def disable_auto_refresh(self):
    self.auto_refresh_enabled = False
    if self._timer_job_id is not None:
        try:
            self.root.after_cancel(self._timer_job_id)
        except Exception:
            pass
        self._timer_job_id = None
    self.update_status_with_timer()

def auto_refresh_job(self):
    if not self.auto_refresh_enabled:
        self._timer_job_id = None
        return
    self.next_refresh_sec -= 1
    if self.next_refresh_sec <= 0:
        symbols = self.parse_symbols()
        if symbols:
            try:
                self.run_analysis()
            except Exception:
                pass
        self.next_refresh_sec = AUTO_REFRESH_MS // 1000
    self.update_status_with_timer()
    self._timer_job_id = self.root.after(1000, self.auto_refresh_job)

def on_row_select(self, event=None):
        selected = self.tree.selection()
        if not selected:
            return
        item = self.tree.item(selected[0])
        values = item["values"]
        if not values:
            return
        symbol = str(values[3])
        target = next((r for r in self.results if r["input_symbol"] == symbol), None)
        if not target:
            return
        detail = [
            f"【{target['input_symbol']} {target['name']}】個股明細分析",
            f"市場：{target['market']}",
            f"資料來源：{target['source']}",
            f"報價時間：{target['quote_time']}",
            f"顯示價：{target['display_price']}",
            f"報價說明：{target['display_note']}",
            f"即時成交價：{target['last_trade'] if target['last_trade'] is not None else '-'}",
            f"參考價/中間價：{target['indicative_price'] if target['indicative_price'] is not None else '-'}",
            f"昨收：{target['prev_close']}",
            f"開盤：{target['open']}",
            f"最高：{target['high']}",
            f"最低：{target['low']}",
            f"漲跌：{target['change']:+.2f}",
            f"漲跌幅：{target['change_pct']:+.2f}%",
            "",
            "【五檔資訊】",
            f"買盤總量：{target['buy_qty']}",
            f"賣盤總量：{target['sell_qty']}",
            f"委買/委賣比：{target['orderbook_ratio']}",
            f"五檔力道：{target['orderbook_bias']}",
            f"買一：{target['bid_prices'][0] if target['bid_prices'] else '-'} / 量：{target['bid_vols'][0] if target['bid_vols'] else '-'}",
            f"賣一：{target['ask_prices'][0] if target['ask_prices'] else '-'} / 量：{target['ask_vols'][0] if target['ask_vols'] else '-'}",
            "",
            "【均線結構】",
            f"MA5：{target['ma5']}",
            f"MA10：{target['ma10']}",
            f"MA20：{target['ma20']}",
            f"MA60：{target['ma60']}",
            "",
            "【技術指標】",
            f"RSI：{target['rsi']}",
            f"綜合訊號：{target['signal']}",
            f"綜合分數：{target['score']}",
            "",
            "【支撐壓力】",
            f"主支撐：{target['support']}",
            f"主壓力：{target['resistance']}",
            "",
            "【技術說明】",
            target["comment"],
            "",
            target["ai_analysis"],
            "",
            target["wave_analysis"],
            "",
            target["fibo_analysis"],
            "",
            target["path_analysis"],
        ]
        advice = [
            f"【{target['input_symbol']} {target['name']}】操作建議",
            f"建議：{target['advice']}",
            "",
            "【風險提醒】",
            target["risk_note"],
            "",
            "【下一目標價】",
            f"下一目標價：{target['fibo']['next_target']}",
            f"1.0：{target['fibo']['target_1_0']}",
            f"1.382：{target['fibo']['target_1_382']}",
            f"1.618：{target['fibo']['target_1_618']}",
            "",
            "【多空路徑重點】",
            f"多方關鍵：守 {target['support']}、破 {target['resistance']}、看 {target['fibo']['next_target']}",
            f"空方關鍵：失守 {target['support']} 後，短線結構轉弱",
            "",
            "【操作觀察重點】",
            f"1. 報價模式：{target['display_note']}",
            f"2. 支撐區：{target['support']} 附近是否守穩",
            f"3. 壓力區：{target['resistance']} 附近是否放量突破",
            f"4. RSI：{target['rsi']} 是否進一步轉強/轉弱",
            f"5. 五檔力道：{target['orderbook_bias']} / 比值={target['orderbook_ratio']}",
            f"6. 均線結構：MA20={target['ma20']} / MA60={target['ma60']}",
            f"7. 分數：{target['score']}，高分優先、低分保守",
        ]
        self.detail_text.delete("1.0", tk.END)
        self.detail_text.insert(tk.END, "\n".join(detail))
        self.advice_text.delete("1.0", tk.END)
        self.advice_text.insert(tk.END, "\n".join(advice))

    def sort_by_column(self, col_name):
        if not self.results:
            return
        key_map = {"排名": None, "燈號": None, "市場": "market", "代號": "input_symbol", "名稱": "name", "顯示價": "display_price", "報價說明": "display_note", "昨收": "prev_close", "漲跌": "change", "漲跌幅%": "change_pct", "訊號": "signal", "建議": "advice", "分數": "score", "支撐": "support", "壓力": "resistance", "RSI": "rsi", "五檔力道": "orderbook_bias"}
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
        file_path = filedialog.asksaveasfilename(title="匯出分析報告", defaultextension=".txt", filetypes=[("Text file", "*.txt"), ("All files", "*.*")])
        if not file_path:
            return
        lines = [f"{APP_TITLE} {APP_VERSION}", f"報告時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", "=" * 140]
        for idx, r in enumerate(self.results, start=1):
            lines.extend([
                f"{idx}. 市場：{r['market']} / 股票代號：{r['input_symbol']} / 名稱：{r['name']}",
                f"   資料來源：{r['source']}",
                f"   顯示價：{r['display_price']} / 報價說明：{r['display_note']}",
                f"   即時成交價：{r['last_trade']} / 參考價：{r['indicative_price']}",
                f"   昨收：{r['prev_close']} / 開盤：{r['open']} / 高：{r['high']} / 低：{r['low']}",
                f"   漲跌：{r['change']:+.2f} / 漲跌幅：{r['change_pct']:+.2f}%",
                f"   訊號：{r['signal']} / 建議：{r['advice']} / 分數：{r['score']}",
                f"   支撐：{r['support']} / 壓力：{r['resistance']} / RSI：{r['rsi']}",
                f"   五檔：{r['orderbook_bias']} / 委買委賣比：{r['orderbook_ratio']}",
                f"   買一：{r['bid_prices'][0] if r['bid_prices'] else '-'} / 量：{r['bid_vols'][0] if r['bid_vols'] else '-'}",
                f"   賣一：{r['ask_prices'][0] if r['ask_prices'] else '-'} / 量：{r['ask_vols'][0] if r['ask_vols'] else '-'}",
                f"   說明：{r['comment']}",
                f"   風險：{r['risk_note']}",
                r["ai_analysis"],
                r["wave_analysis"],
                r["fibo_analysis"],
                r["path_analysis"],
                "-" * 140,
            ])
        with open(file_path, "w", encoding="utf-8-sig") as f:
            f.write("\n".join(lines))
        self.set_status(f"已匯出 TXT：{file_path} / 版本：{APP_VERSION}")
        messagebox.showinfo("完成", "TXT 匯出成功。")

    def export_pdf(self):
        if not self.results:
            messagebox.showwarning("提醒", "目前沒有分析結果可匯出。")
            return
        file_path = filedialog.asksaveasfilename(title="匯出 PDF 報告", defaultextension=".pdf", filetypes=[("PDF file", "*.pdf"), ("All files", "*.*")])
        if not file_path:
            return
        font_name = setup_pdf_font()
        c = canvas.Canvas(file_path, pagesize=landscape(A4))
        width, height = landscape(A4)
        c.setFont(font_name, 16)
        c.drawString(24, height - 28, f"{APP_TITLE} {APP_VERSION}")
        c.setFont(font_name, 9)
        c.drawString(24, height - 46, f"報告時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        headers = ["排名", "市場", "代號", "名稱", "顯示價", "昨收", "漲跌", "漲跌幅%", "訊號", "建議", "分數", "支撐", "壓力", "RSI"]
        x_positions = [20, 50, 105, 155, 320, 385, 445, 510, 580, 650, 750, 810, 865, 920]
        y = height - 72
        c.setFont(font_name, 8)
        for h, x in zip(headers, x_positions):
            c.drawString(x, y, h)
        y -= 14
        c.line(18, y + 8, width - 18, y + 8)
        for idx, r in enumerate(self.results, start=1):
            if y < 42:
                c.showPage()
                c.setFont(font_name, 8)
                y = height - 40
            row = [str(idx), r["market"], r["input_symbol"], r["name"][:18], str(r["display_price"]), str(r["prev_close"]), f"{r['change']:+.2f}", f"{r['change_pct']:+.2f}%", r["signal"], r["advice"][:10], str(r["score"]), str(r["support"]), str(r["resistance"]), str(r["rsi"])]
            for text, x in zip(row, x_positions):
                c.drawString(x, y, str(text))
            y -= 14
        c.save()
        self.set_status(f"已匯出 PDF：{file_path} / 版本：{APP_VERSION}")
        messagebox.showinfo("完成", "PDF 匯出成功。")

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
