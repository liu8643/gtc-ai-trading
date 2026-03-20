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
APP_VERSION = "v4.3.0-TW-Realtime-AI-Wave-Fibo"
AUTO_REFRESH_MS = 30000  # 30 秒


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
            return [f"{s}.TW", f"{s}.TWO"]
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
    params = {
        "ex_ch": ex_ch,
        "json": "1",
        "delay": "0",
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://mis.twse.com.tw/stock/index.jsp",
    }

    try:
        r = requests.get(url, params=params, headers=headers, timeout=8)
        r.raise_for_status()
        data = r.json()

        msg_array = data.get("msgArray", [])
        if not msg_array:
            return None

        item = msg_array[0]

        last_price = safe_float(item.get("z"))
        open_price = safe_float(item.get("o"))
        high_price = safe_float(item.get("h"))
        low_price = safe_float(item.get("l"))
        prev_close = safe_float(item.get("y"))

        if last_price is None:
            last_price = prev_close
        if last_price is None:
            return None

        return {
            "close": round_price(last_price),
            "prev_close": round_price(prev_close if prev_close is not None else last_price),
            "open": round_price(open_price if open_price is not None else last_price),
            "high": round_price(high_price if high_price is not None else last_price),
            "low": round_price(low_price if low_price is not None else last_price),
            "source": "TWSE MIS 即時",
        }
    except Exception:
        return None


def get_us_yahoo_quote(
    yf_symbol: str,
    fallback_close: float,
    fallback_prev_close: float,
    fallback_open: float,
    fallback_high: float,
    fallback_low: float,
) -> dict:
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

    return {
        "close": live_price,
        "prev_close": prev_close,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "source": "Yahoo Finance",
    }


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


def build_trade_advice(close, ma20, ma60, score, rsi, support, resistance, change_pct) -> str:
    if close >= ma20 and close >= ma60 and score >= 80:
        return "強勢買進"
    if close >= ma20 and score >= 65:
        return "拉回布局"
    if score >= 45 and close >= support:
        return "區間觀察"
    if rsi > 70 or close >= resistance * 0.98:
        return "減碼/停利"
    if close < ma20 and close < ma60 and change_pct < 0:
        return "轉弱觀望"
    return "保守觀察"


def build_risk_note(close, support, resistance, rsi, score):
    notes = []

    if close <= support * 1.01:
        notes.append("接近支撐，觀察是否守穩")
    if close >= resistance * 0.99:
        notes.append("逼近壓力，留意獲利了結賣壓")
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
    lines = []

    close = data["close"]
    ma20 = data["ma20"]
    ma60 = data["ma60"]
    rsi = data["rsi"]
    score = data["score"]
    support = data["support"]
    resistance = data["resistance"]
    change_pct = data["change_pct"]
    signal = data["signal"]
    advice = data["advice"]

    if close >= ma20 and close >= ma60:
        trend_text = "目前股價位於20日線與60日線之上，中期趨勢偏強。"
    elif close >= ma20 and close < ma60:
        trend_text = "目前股價站上20日線，但仍在60日線下方，屬短強中性結構。"
    elif close < ma20 and close >= ma60:
        trend_text = "目前股價跌破20日線但仍守住60日線，短線轉弱、中期待觀察。"
    else:
        trend_text = "目前股價位於20日線與60日線下方，技術面偏弱。"

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

    if change_pct >= 5:
        move_text = f"當前漲跌幅為 {change_pct:+.2f}%，屬於強勢波動，市場追價意願明顯。"
    elif change_pct > 0:
        move_text = f"當前漲跌幅為 {change_pct:+.2f}%，價格仍維持正向變化。"
    elif change_pct <= -5:
        move_text = f"當前漲跌幅為 {change_pct:+.2f}%，屬於明顯轉弱走勢，須提高風險意識。"
    else:
        move_text = f"當前漲跌幅為 {change_pct:+.2f}%，短線價格變化偏保守。"

    sr_text = f"下方主支撐約在 {support}，上方主壓力約在 {resistance}。若守穩支撐，有利延續整理後再攻；若壓力無法突破，則仍以區間看待。"

    if score >= 85:
        final_text = f"AI綜合判斷：目前屬高分強勢結構，訊號為「{signal}」，建議採取「{advice}」策略，但接近壓力區時不宜過度追價。"
    elif score >= 65:
        final_text = f"AI綜合判斷：目前結構偏多，訊號為「{signal}」，建議以「{advice}」方式操作，等待拉回或突破確認。"
    elif score >= 45:
        final_text = f"AI綜合判斷：目前屬整理盤，訊號為「{signal}」，建議以「{advice}」為主，不宜激進追價。"
    else:
        final_text = f"AI綜合判斷：目前技術面偏弱，訊號為「{signal}」，建議採「{advice}」策略，先以風險控制優先。"

    lines.append("【AI個股分析】")
    lines.append(f"1. 趨勢判讀：{trend_text}")
    lines.append(f"2. 動能狀態：{rsi_text}")
    lines.append(f"3. 價格強弱：{move_text}")
    lines.append(f"4. 關鍵位置：{sr_text}")
    lines.append(f"5. AI結論：{final_text}")

    return "\n".join(lines)


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
    short_text = summarize_wave(df, 20, "短期")
    mid_text = summarize_wave(df, 60, "中期")
    long_text = summarize_wave(df, 120, "長期")

    return "\n".join([
        "【波浪理論分析】",
        f"1. {short_text}",
        f"2. {mid_text}",
        f"3. {long_text}",
    ])


def calc_fibonacci_targets(df: pd.DataFrame) -> dict:
    """
    以近 120 日主波段高低點計算費波南西目標位
    1. 先判斷當前比較像上升波或下降波
    2. 列出 1.0 / 1.382 / 1.618
    3. 給下一目標價
    """
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

        summary = (
            f"目前較偏上升波段，近波段低點 {round_price(low_val)} 至高點 {round_price(high_val)}。"
            f"若續強，下一觀察目標依序為 1.0={round_price(target_1_0)}、"
            f"1.382={round_price(target_1_382)}、1.618={round_price(target_1_618)}。"
        )
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

        summary = (
            f"目前較偏下降修正波，近波段高點 {round_price(high_val)} 至低點 {round_price(low_val)}。"
            f"若續弱，下一觀察目標依序為 1.0={round_price(target_1_0)}、"
            f"1.382={round_price(target_1_382)}、1.618={round_price(target_1_618)}。"
        )

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
                "close": fallback_close,
                "prev_close": fallback_prev_close,
                "open": fallback_open,
                "high": fallback_high,
                "low": fallback_low,
                "source": "日線回退",
            }
    else:
        rt = get_us_yahoo_quote(
            yf_symbol=yf_symbol,
            fallback_close=fallback_close,
            fallback_prev_close=fallback_prev_close,
            fallback_open=fallback_open,
            fallback_high=fallback_high,
            fallback_low=fallback_low,
        )

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
        score += 4
        comments.append("站上5日線")
    else:
        score -= 4
        comments.append("跌破5日線")

    if close >= ma10:
        score += 6
        comments.append("站上10日線")
    else:
        score -= 5
        comments.append("跌破10日線")

    if close >= ma20:
        score += 10
        comments.append("站上20日線")
    else:
        score -= 10
        comments.append("跌破20日線")

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

    advice = build_trade_advice(
        close=close,
        ma20=ma20,
        ma60=ma60,
        score=score,
        rsi=rsi,
        support=support,
        resistance=resistance,
        change_pct=change_pct,
    )

    risk_note = build_risk_note(
        close=close,
        support=support,
        resistance=resistance,
        rsi=rsi,
        score=score,
    )

    extra_comment = (
        f"{'；'.join(comments)}"
        f"；20日支撐={sr['support20']}"
        f"；20日壓力={sr['resistance20']}"
        f"；波段低點={sr['swing_low']}"
        f"；波段高點={sr['swing_high']}"
        f"；Pivot={sr['pivot']}"
        f"；來源={rt['source']}"
    )

    fibo = calc_fibonacci_targets(df)

    result = {
        "input_symbol": symbol,
        "name": stock_name,
        "yf_symbol": yf_symbol,
        "market": market,
        "close": close,
        "prev_close": prev_close,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "change": change,
        "change_pct": change_pct,
        "signal": signal,
        "advice": advice,
        "score": score,
        "support": support,
        "resistance": resistance,
        "rsi": rsi,
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "ma60": ma60,
        "comment": extra_comment,
        "risk_note": risk_note,
        "source": rt["source"],
        "fibo": fibo,
    }

    result["ai_analysis"] = build_ai_analysis(result)
    result["wave_analysis"] = build_wave_analysis(df)
    result["fibo_analysis"] = build_fibonacci_analysis(fibo)
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
        self._build_ui()
        self.set_status(f"系統已就緒。當前版本：{APP_VERSION}")

    def _build_ui(self):
        top = ttk.Frame(self.root, padding=10)
        top.pack(fill="x")

        ttk.Label(top, text="股票代號（逗號分隔）").pack(side="left", padx=(0, 8))

        self.symbol_entry = ttk.Entry(top, width=100)
        self.symbol_entry.pack(side="left", padx=(0, 8), fill="x", expand=True)
        self.symbol_entry.insert(0, "2330,2382,3231,2308,3017,AAPL,NVDA,MSFT")

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
        columns = (
            "排名", "市場", "代號", "名稱", "收盤", "昨收", "漲跌", "漲跌幅%",
            "訊號", "建議", "分數", "支撐", "壓力", "RSI"
        )

        self.tree = ttk.Treeview(parent, columns=columns, show="headings", height=16)

        widths = {
            "排名": 55,
            "市場": 90,
            "代號": 80,
            "名稱": 180,
            "收盤": 90,
            "昨收": 90,
            "漲跌": 90,
            "漲跌幅%": 95,
            "訊號": 100,
            "建議": 100,
            "分數": 65,
            "支撐": 90,
            "壓力": 90,
            "RSI": 70,
        }

        for c in columns:
            self.tree.heading(c, text=c, command=lambda col=c: self.sort_by_column(col))
            self.tree.column(c, width=widths[c], anchor="center")

        self.tree.tag_configure("up", foreground="red", background="#ffecec")
        self.tree.tag_configure("down", foreground="green", background="#ecffec")
        self.tree.tag_configure("flat", foreground="black", background="white")
        self.tree.tag_configure("strong", background="#fff2b3")
        self.tree.tag_configure("watch", background="#eef5ff")

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

        self.detail_text = tk.Text(left, height=14, wrap="word", font=("Microsoft JhengHei", 10))
        self.detail_text.pack(fill="both", expand=True)

        self.advice_text = tk.Text(right, height=14, wrap="word", font=("Microsoft JhengHei", 10))
        self.advice_text.pack(fill="both", expand=True)

        bottom = ttk.LabelFrame(self.root, text="系統訊息", padding=10)
        bottom.pack(fill="x", padx=10, pady=(0, 10))

        self.status_var = tk.StringVar(value="")
        ttk.Label(bottom, textvariable=self.status_var).pack(anchor="w")

    def set_status(self, text: str):
        now = datetime.now().strftime("%H:%M:%S")
        self.status_var.set(f"[{now}] {text}")

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

            if r["score"] >= 80:
                tags.append("strong")
            elif r["score"] >= 65:
                tags.append("watch")

            change_str = f"{r['change']:+.2f}"
            change_pct_str = f"{r['change_pct']:+.2f}%"

            self.tree.insert(
                "",
                "end",
                values=(
                    idx,
                    r["market"],
                    r["input_symbol"],
                    r["name"],
                    r["close"],
                    r["prev_close"],
                    change_str,
                    change_pct_str,
                    r["signal"],
                    r["advice"],
                    r["score"],
                    r["support"],
                    r["resistance"],
                    r["rsi"],
                ),
                tags=tuple(tags),
            )

    def run_analysis(self):
        symbols = self.parse_symbols()
        if not symbols:
            messagebox.showwarning("提醒", "請輸入至少一個股票代號。")
            return

        self.clear_results()
        self.set_status(f"開始抓取即時股票資料... 版本：{APP_VERSION}")
        self.root.update_idletasks()

        ok_results = []
        errors = []

        for sym in symbols:
            try:
                result = analyze_symbol(sym)
                ok_results.append(result)
                self.set_status(f"完成：{sym} / 版本：{APP_VERSION}")
                self.root.update_idletasks()
            except Exception as e:
                errors.append(f"{sym}: {e}")

        self.results = sorted(ok_results, key=lambda x: x["score"], reverse=True)
        self.render_results()

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
        self.set_status(f"已啟用自動刷新（每 {AUTO_REFRESH_MS // 1000} 秒）。版本：{APP_VERSION}")
        self.root.after(AUTO_REFRESH_MS, self.auto_refresh_job)

    def disable_auto_refresh(self):
        self.auto_refresh_enabled = False
        self.set_status(f"已停止自動刷新。版本：{APP_VERSION}")

    def auto_refresh_job(self):
        if not self.auto_refresh_enabled:
            return

        symbols = self.parse_symbols()
        if symbols:
            try:
                self.run_analysis()
            except Exception:
                pass

        self.root.after(AUTO_REFRESH_MS, self.auto_refresh_job)

    def on_row_select(self, event=None):
        selected = self.tree.selection()
        if not selected:
            return

        item = self.tree.item(selected[0])
        values = item["values"]
        if not values:
            return

        symbol = str(values[2])

        target = None
        for r in self.results:
            if r["input_symbol"] == symbol:
                target = r
                break

        if not target:
            return

        detail = []
        detail.append(f"【{target['input_symbol']} {target['name']}】個股明細分析")
        detail.append(f"市場：{target['market']}")
        detail.append(f"資料來源：{target['source']}")
        detail.append(f"收盤/現價：{target['close']}")
        detail.append(f"昨收：{target['prev_close']}")
        detail.append(f"開盤：{target['open']}")
        detail.append(f"最高：{target['high']}")
        detail.append(f"最低：{target['low']}")
        detail.append(f"漲跌：{target['change']:+.2f}")
        detail.append(f"漲跌幅：{target['change_pct']:+.2f}%")
        detail.append("")
        detail.append("【均線結構】")
        detail.append(f"MA5：{target['ma5']}")
        detail.append(f"MA10：{target['ma10']}")
        detail.append(f"MA20：{target['ma20']}")
        detail.append(f"MA60：{target['ma60']}")
        detail.append("")
        detail.append("【技術指標】")
        detail.append(f"RSI：{target['rsi']}")
        detail.append(f"綜合訊號：{target['signal']}")
        detail.append(f"綜合分數：{target['score']}")
        detail.append("")
        detail.append("【支撐壓力】")
        detail.append(f"主支撐：{target['support']}")
        detail.append(f"主壓力：{target['resistance']}")
        detail.append("")
        detail.append("【技術說明】")
        detail.append(target["comment"])
        detail.append("")
        detail.append(target["ai_analysis"])
        detail.append("")
        detail.append(target["wave_analysis"])
        detail.append("")
        detail.append(target["fibo_analysis"])

        advice = []
        advice.append(f"【{target['input_symbol']} {target['name']}】操作建議")
        advice.append(f"建議：{target['advice']}")
        advice.append("")
        advice.append("【風險提醒】")
        advice.append(target["risk_note"])
        advice.append("")
        advice.append("【下一目標價】")
        advice.append(f"下一目標價：{target['fibo']['next_target']}")
        advice.append(f"1.0：{target['fibo']['target_1_0']}")
        advice.append(f"1.382：{target['fibo']['target_1_382']}")
        advice.append(f"1.618：{target['fibo']['target_1_618']}")
        advice.append("")
        advice.append("【操作觀察重點】")
        advice.append(f"1. 支撐區：{target['support']} 附近是否守穩")
        advice.append(f"2. 壓力區：{target['resistance']} 附近是否放量突破")
        advice.append(f"3. RSI：{target['rsi']} 是否進一步轉強/轉弱")
        advice.append(f"4. 均線結構：MA20={target['ma20']} / MA60={target['ma60']}")
        advice.append(f"5. 分數：{target['score']}，高分優先、低分保守")

        self.detail_text.delete("1.0", tk.END)
        self.detail_text.insert(tk.END, "\n".join(detail))

        self.advice_text.delete("1.0", tk.END)
        self.advice_text.insert(tk.END, "\n".join(advice))

    def sort_by_column(self, col_name):
        if not self.results:
            return

        key_map = {
            "排名": None,
            "市場": "market",
            "代號": "input_symbol",
            "名稱": "name",
            "收盤": "close",
            "昨收": "prev_close",
            "漲跌": "change",
            "漲跌幅%": "change_pct",
            "訊號": "signal",
            "建議": "advice",
            "分數": "score",
            "支撐": "support",
            "壓力": "resistance",
            "RSI": "rsi",
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
        lines.append(f"{APP_TITLE} {APP_VERSION}")
        lines.append(f"報告時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("=" * 140)

        for idx, r in enumerate(self.results, start=1):
            lines.append(f"{idx}. 市場：{r['market']} / 股票代號：{r['input_symbol']} / 名稱：{r['name']}")
            lines.append(f"   資料來源：{r['source']}")
            lines.append(f"   收盤/現價：{r['close']} / 昨收：{r['prev_close']}")
            lines.append(f"   漲跌：{r['change']:+.2f} / 漲跌幅：{r['change_pct']:+.2f}%")
            lines.append(f"   訊號：{r['signal']} / 建議：{r['advice']} / 分數：{r['score']}")
            lines.append(f"   支撐：{r['support']} / 壓力：{r['resistance']} / RSI：{r['rsi']}")
            lines.append(f"   說明：{r['comment']}")
            lines.append(f"   風險：{r['risk_note']}")
            lines.append(r["ai_analysis"])
            lines.append(r["wave_analysis"])
            lines.append(r["fibo_analysis"])
            lines.append("-" * 140)

        try:
            with open(file_path, "w", encoding="utf-8-sig") as f:
                f.write("\n".join(lines))
            self.set_status(f"已匯出 TXT：{file_path} / 版本：{APP_VERSION}")
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
            c.drawString(24, height - 28, f"{APP_TITLE} {APP_VERSION}")

            c.setFont(font_name, 9)
            c.drawString(24, height - 46, f"報告時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            headers = ["排名", "市場", "代號", "名稱", "收盤", "漲跌", "漲跌幅%", "訊號", "建議", "分數", "支撐", "壓力", "RSI"]
            x_positions = [20, 50, 105, 155, 320, 380, 445, 515, 585, 655, 715, 775, 840]
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

                row = [
                    str(idx),
                    r["market"],
                    r["input_symbol"],
                    r["name"][:20],
                    str(r["close"]),
                    f"{r['change']:+.2f}",
                    f"{r['change_pct']:+.2f}%",
                    r["signal"],
                    r["advice"],
                    str(r["score"]),
                    str(r["support"]),
                    str(r["resistance"]),
                    str(r["rsi"]),
                ]

                for text, x in zip(row, x_positions):
                    c.drawString(x, y, str(text))

                y -= 14

            c.save()
            self.set_status(f"已匯出 PDF：{file_path} / 版本：{APP_VERSION}")
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
