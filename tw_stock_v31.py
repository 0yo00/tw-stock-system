
import json
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(layout="wide", page_title="台股短線系統 v31")
st.title("🚀 台股短線系統 v31")

FAVORITES_FILE = Path("stock_favorites.json")
SNAPSHOT_FILE = Path("stock_snapshots.json")
TRADES_FILE = Path("trades_v13.json")
NAMES_FILE = Path("tw_stock_names.json")
MAX_SNAPSHOTS = 5000

builtin_stock_names = {
    "2330.TW": "台積電", "2317.TW": "鴻海", "2454.TW": "聯發科", "2303.TW": "聯電",
    "2382.TW": "廣達", "3037.TW": "欣興", "3443.TW": "創意", "2603.TW": "長榮",
    "2609.TW": "陽明", "2615.TW": "萬海", "4989.TW": "榮科", "1815.TWO": "富喬",
    "1463.TW": "強盛新", "3017.TW": "奇鋐", "3665.TW": "貿聯-KY", "3715.TW": "定穎投控",
    "8046.TW": "南電", "2368.TW": "金像電", "3044.TW": "健鼎", "5274.TW": "信驊",
    "6805.TW": "富世達", "3231.TW": "緯創", "2356.TW": "英業達", "2345.TW": "智邦",
    "2376.TW": "技嘉", "6669.TW": "緯穎", "1519.TW": "華城", "2308.TW": "台達電",
    "2327.TW": "國巨", "8299.TW": "群聯", "2408.TW": "南亞科", "3189.TW": "景碩",
    "2449.TW": "京元電子", "6223.TW": "旺矽", "6515.TW": "穎崴",
    "1802.TW": "台玻", "2495.TW": "普安", "6531.TW": "愛普", "4979.TW": "華星光",
    "3013.TW": "晟銘電", "2371.TW": "大同", "2451.TW": "創見", "3702.TW": "大聯大",
    "8261.TW": "富鼎", "3645.TW": "達邁", "6695.TWO": "芯鼎", "3483.TW": "力致",
    "3025.TW": "星通"
}

stock_sector = {
    "2330.TW": "半導體", "2317.TW": "AI伺服器", "2454.TW": "IC設計", "2303.TW": "半導體",
    "2382.TW": "AI伺服器", "3037.TW": "PCB", "3443.TW": "ASIC", "2603.TW": "航運",
    "2609.TW": "航運", "2615.TW": "航運", "4989.TW": "電子材料", "1815.TWO": "PCB材料",
    "1463.TW": "紡織", "3017.TW": "散熱", "3665.TW": "連接線", "3715.TW": "PCB",
    "8046.TW": "ABF", "2368.TW": "PCB", "3044.TW": "PCB", "5274.TW": "AI伺服器",
    "6805.TW": "摺疊軸承", "3231.TW": "AI伺服器", "2356.TW": "AI伺服器", "2345.TW": "網通",
    "2376.TW": "顯卡", "6669.TW": "AI伺服器", "1519.TW": "重電", "2308.TW": "電源供應",
    "2327.TW": "被動元件", "8299.TW": "NAND", "2408.TW": "DRAM", "3189.TW": "ABF",
    "2449.TW": "封測", "6223.TW": "半導體設備", "6515.TW": "半導體設備",
    "1802.TW": "玻璃陶瓷", "2495.TW": "儲存設備", "6531.TW": "記憶體IP", "4979.TW": "光通訊",
    "3013.TW": "機殼", "2371.TW": "重電", "2451.TW": "記憶體", "3702.TW": "半導體通路",
    "8261.TW": "功率半導體", "3645.TW": "PI材料", "6695.TWO": "影像IC", "3483.TW": "散熱",
    "3025.TW": "通訊"
}
watchlist_default = list(builtin_stock_names.keys())


def load_json_list(path: Path):
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            return data if isinstance(data, list) else []
        except Exception:
            return []
    return []


def save_json_list(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def ensure_names_file():
    if not NAMES_FILE.exists():
        NAMES_FILE.write_text(json.dumps(builtin_stock_names, ensure_ascii=False, indent=2), encoding="utf-8")


def load_name_map():
    ensure_names_file()
    name_map = dict(builtin_stock_names)
    try:
        extra = json.loads(NAMES_FILE.read_text(encoding="utf-8"))
        if isinstance(extra, dict):
            for k, v in extra.items():
                if isinstance(v, str) and v.strip():
                    name_map[str(k).upper()] = v.strip()
    except Exception:
        pass
    return name_map


def load_favorites():
    return [str(x) for x in load_json_list(FAVORITES_FILE)]


def save_favorites(favorites):
    save_json_list(FAVORITES_FILE, sorted(list(set(favorites))))


def load_snapshots():
    return load_json_list(SNAPSHOT_FILE)[-MAX_SNAPSHOTS:]


def save_snapshots(data):
    save_json_list(SNAPSHOT_FILE, data[-MAX_SNAPSHOTS:])


def load_trades():
    return load_json_list(TRADES_FILE)


def save_trades(data):
    save_json_list(TRADES_FILE, data)


def display_name(code: str, name_map: dict):
    code = code.upper()
    name = name_map.get(code, "")
    return f"{code}（{name}）" if name else code


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[["Open", "High", "Low", "Close", "Volume"]].apply(pd.to_numeric, errors="coerce")
    return df.dropna(subset=["Open", "High", "Low", "Close"])


def calc_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, pd.NA)
    return (100 - (100 / (1 + rs))).fillna(50)


def calc_kd(df: pd.DataFrame, period: int = 9):
    low_n = df["Low"].rolling(period).min()
    high_n = df["High"].rolling(period).max()
    rsv = ((df["Close"] - low_n) / (high_n - low_n).replace(0, pd.NA) * 100).fillna(50)
    k = rsv.ewm(alpha=1/3, adjust=False).mean()
    d = k.ewm(alpha=1/3, adjust=False).mean()
    return k.fillna(50), d.fillna(50)


def indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ma5"] = df["Close"].rolling(5).mean()
    df["ma20"] = df["Close"].rolling(20).mean()
    df["ma60"] = df["Close"].rolling(60).mean()
    df["ma120"] = df["Close"].rolling(120).mean()
    df["vol5"] = df["Volume"].rolling(5).mean()
    df["atr"] = (df["High"] - df["Low"]).rolling(14).mean()
    df["rsi"] = calc_rsi(df["Close"], 14)
    df["bias5"] = ((df["Close"] - df["ma5"]) / df["ma5"] * 100).fillna(0)
    df["k"], df["d"] = calc_kd(df, 9)
    return df


@st.cache_data(ttl=600, show_spinner=False)
def download_symbol(symbol: str) -> pd.DataFrame:
    df = yf.download(symbol.strip(), period="8mo", interval="1d", auto_adjust=False, progress=False, threads=False)
    return normalize_df(df)


def resolve_symbol(raw_input: str):
    raw = raw_input.strip().upper()
    if not raw:
        return None, pd.DataFrame()
    if raw.endswith(".TW") or raw.endswith(".TWO"):
        candidates = [raw]
    elif raw.isdigit() and len(raw) == 4:
        candidates = [f"{raw}.TW", f"{raw}.TWO"]
    else:
        candidates = [raw, f"{raw}.TW", f"{raw}.TWO"]
    for symbol in candidates:
        df = download_symbol(symbol)
        if not df.empty and len(df) >= 20:
            return symbol, df
    return candidates[0], pd.DataFrame()


@st.cache_data(ttl=600, show_spinner=False)
def get_market_data():
    df = yf.download("^TWII", period="6mo", interval="1d", auto_adjust=False, progress=False, threads=False)
    return normalize_df(df)

@st.cache_data(ttl=900, show_spinner=False)
def get_stock_news(symbol: str):
    items = []
    try:
        ticker = yf.Ticker(symbol)
        news = getattr(ticker, "news", []) or []
        for n in news[:8]:
            title = n.get("title", "")
            link = n.get("link", "")
            publisher = n.get("publisher", "")
            provider_time = n.get("providerPublishTime")
            time_str = ""
            if provider_time:
                try:
                    time_str = datetime.fromtimestamp(provider_time).strftime("%Y-%m-%d %H:%M")
                except Exception:
                    time_str = ""
            if title:
                items.append({"title": title, "link": link, "publisher": publisher, "time": time_str})
    except Exception:
        return []
    return items

def summarize_event_signals(row, news_items):
    titles = [x.get("title", "") for x in news_items if x.get("title")]
    text = " ".join(titles)

    pos_words = ["創高", "成長", "利多", "擴產", "合作", "訂單", "上修", "突破", "增溫", "樂觀", "營收", "法說", "受惠", "AI", "GB300", "新品"]
    neg_words = ["下修", "衰退", "虧損", "調降", "利空", "賣壓", "疲弱", "風險", "下滑", "衝擊", "裁員", "虧", "調查", "訴訟", "停工"]

    pos = sum(word in text for word in pos_words)
    neg = sum(word in text for word in neg_words)

    tags = []
    if "法說" in text:
        tags.append("法說會")
    if "營收" in text:
        tags.append("營收")
    if "AI" in text or "GB300" in text:
        tags.append("AI題材")
    if "訂單" in text or "合作" in text:
        tags.append("接單/合作")
    if "擴產" in text:
        tags.append("擴產")
    if "虧損" in text or "下修" in text:
        tags.append("財務/展望風險")

    if pos > neg:
        sentiment = "偏多"
    elif neg > pos:
        sentiment = "偏空"
    else:
        sentiment = "中性"

    bullets = []
    bullets.append(f"事件氣氛：{sentiment}" + (f"；關鍵題材：{'、'.join(tags[:3])}" if tags else "；目前未抓到明確題材關鍵字"))
    bullets.append(f"技術面：{row['結論']} / {row['交易訊號']}，風報比 {row['風報比']}")
    bullets.append(f"量價面：{row['量能變化']} {row['量能變化%']:+.2f}% ，量比(5日) {row['量比5日']}")
    bullets.append(f"位置面：現價距支撐約 {round((row['收盤']-row['支撐'])/row['收盤']*100,1)}% ，距短壓約 {round((row['短期壓力']-row['收盤'])/row['收盤']*100,1)}%")
    if titles:
        bullets.append("最近關鍵標題：" + "；".join(titles[:2]))
    return bullets[:4]


def market_filter():
    df = get_market_data()
    if df.empty or len(df) < 20:
        return {"label": "資料不足", "score_adj": 0, "text": "大盤資料不足。", "close": None}
    df = indicators(df)
    close = float(df["Close"].iloc[-1])
    ma20 = float(df["ma20"].iloc[-1]) if pd.notna(df["ma20"].iloc[-1]) else close
    ma5 = float(df["ma5"].iloc[-1]) if pd.notna(df["ma5"].iloc[-1]) else close
    prev5 = float(df["Close"].iloc[-5]) if len(df) >= 5 else close
    if close > ma20 and ma5 >= ma20 and close > prev5:
        return {"label": "偏多", "score_adj": 8, "text": "大盤站上月線且短線偏強。", "close": round(close, 2)}
    if close < ma20 and ma5 < ma20 and close < prev5:
        return {"label": "偏空", "score_adj": -10, "text": "大盤偏弱，建議保守。", "close": round(close, 2)}
    return {"label": "中性", "score_adj": 0, "text": "大盤方向不明，宜選強不選弱。", "close": round(close, 2)}


def market_bias(df: pd.DataFrame):
    prev_close = float(df["Close"].iloc[-2])
    last_close = float(df["Close"].iloc[-1])
    change = (last_close - prev_close) / prev_close if prev_close else 0
    if change > 0.02:
        return "偏多（可進場）"
    if change < -0.02:
        return "偏空（觀望）"
    return "中性（等待確認）"


def breakout_analysis(df: pd.DataFrame, resistance: float):
    close = float(df["Close"].iloc[-1]); high = float(df["High"].iloc[-1]); open_ = float(df["Open"].iloc[-1])
    vol = float(df["Volume"].iloc[-1]); vol5 = float(df["vol5"].iloc[-1]) if pd.notna(df["vol5"].iloc[-1]) else vol
    closed_above = close > resistance * 1.002
    intraday_break = high > resistance * 1.002
    strong_volume = vol > vol5 * 1.2 if vol5 > 0 else False
    close_near_high = close >= high * 0.985 if high > 0 else False
    candle_positive = close >= open_
    if closed_above and strong_volume and close_near_high:
        return "已突破", "強", "可追，但以不爆量長黑為前提"
    elif intraday_break and not closed_above:
        return "假突破風險", "弱", "不建議追，等重新站穩"
    elif closed_above and not strong_volume:
        return "已突破", "普通", "可少量追或等回測"
    return "尚未突破", "普通" if candle_positive else "弱", "先觀察壓力是否有效站上"


def calculate_score(df: pd.DataFrame):
    close = float(df["Close"].iloc[-1]); prev5 = float(df["Close"].iloc[-5]) if len(df) >= 5 else close
    prev10 = float(df["Close"].iloc[-10]) if len(df) >= 10 else close
    ma5 = float(df["ma5"].iloc[-1]) if pd.notna(df["ma5"].iloc[-1]) else close
    ma20 = float(df["ma20"].iloc[-1]) if pd.notna(df["ma20"].iloc[-1]) else close
    vol = float(df["Volume"].iloc[-1]); vol5 = float(df["vol5"].iloc[-1]) if pd.notna(df["vol5"].iloc[-1]) else vol
    rsi = float(df["rsi"].iloc[-1]) if pd.notna(df["rsi"].iloc[-1]) else 50
    k = float(df["k"].iloc[-1]) if pd.notna(df["k"].iloc[-1]) else 50
    d = float(df["d"].iloc[-1]) if pd.notna(df["d"].iloc[-1]) else 50
    score = 0
    if close > ma20: score += 18
    if close > ma5: score += 10
    if close > prev5: score += 10
    if close > prev10: score += 6
    if vol > vol5: score += 14
    if 45 <= rsi <= 75: score += 8
    if k > d: score += 5
    return int(score)


def decision_star_and_action(score: int, breakout_status: str, breakout_strength: str, rr: float, bias: str):
    adj = score
    if breakout_status == "已突破": adj += 12
    elif breakout_status == "假突破風險": adj -= 18
    else: adj -= 4
    if breakout_strength == "強": adj += 6
    elif breakout_strength == "弱": adj -= 6
    if rr >= 2.0: adj += 8
    elif rr >= 1.5: adj += 4
    elif rr < 1.0: adj -= 8
    if bias == "偏多（可進場）": adj += 5
    elif bias == "偏空（觀望）": adj -= 8
    adj = max(0, min(100, adj))
    if adj >= 88: return "★★★★★", "可以考慮進場"
    if adj >= 72: return "★★★★☆", "觀察"
    if adj >= 58: return "★★★☆☆", "普通"
    if adj >= 40: return "★★☆☆☆", "避開"
    return "★☆☆☆☆", "空方"


def trend_conclusion(action_label: str, breakout_status: str, bias: str):
    if action_label == "可以考慮進場": return "看多"
    if action_label == "空方": return "看空"
    if breakout_status == "尚未突破" and bias == "偏多（可進場）": return "中性偏多"
    if bias == "偏空（觀望）": return "中性偏空"
    return "中性"


def signal_label(action_label: str, breakout_status: str, rr: float):
    if action_label == "可以考慮進場" and rr >= 1.2: return "🔥進場"
    if breakout_status == "尚未突破": return "⏳等待"
    if action_label in ["避開", "空方"]: return "❌不進"
    return "👀觀察"


def signal_priority(signal: str) -> int:
    return {"🔥進場": 4, "👀觀察": 3, "⏳等待": 2, "❌不進": 0}.get(signal, 1)


def conclusion_priority(conclusion: str) -> int:
    return {"看多": 5, "中性偏多": 4, "中性": 3, "中性偏空": 2, "看空": 0}.get(conclusion, 1)


def action_priority(action_label: str) -> int:
    return {"可以考慮進場": 5, "觀察": 4, "普通": 3, "避開": 1, "空方": 0}.get(action_label, 2)


def rank_bucket(signal: str, conclusion: str, action_label: str) -> str:
    if signal == "❌不進" or conclusion == "看空" or action_label == "空方":
        return "D_後段排除"
    if signal == "🔥進場" and conclusion in ["看多", "中性偏多"] and action_label in ["可以考慮進場", "觀察"]:
        return "A_優先關注"
    if signal in ["👀觀察", "⏳等待"] and conclusion in ["看多", "中性偏多", "中性"]:
        return "B_中性觀察"
    return "C_保守追蹤"


def rank_reason(bucket: str, conclusion: str, signal: str) -> str:
    if bucket == "A_優先關注": return f"{conclusion}＋{signal}＋風報比佳"
    if bucket == "B_中性觀察": return f"{conclusion}＋{signal}，先觀察"
    if bucket == "C_保守追蹤": return f"{conclusion}，條件普通"
    return "看空 / 不進，已自動降級到後段"


def build_reason(df: pd.DataFrame, breakout_status: str):
    close = float(df["Close"].iloc[-1]); ma5 = float(df["ma5"].iloc[-1]) if pd.notna(df["ma5"].iloc[-1]) else close
    ma20 = float(df["ma20"].iloc[-1]) if pd.notna(df["ma20"].iloc[-1]) else close
    vol = float(df["Volume"].iloc[-1]); vol5 = float(df["vol5"].iloc[-1]) if pd.notna(df["vol5"].iloc[-1]) else vol
    rsi = float(df["rsi"].iloc[-1]) if pd.notna(df["rsi"].iloc[-1]) else 50
    k = float(df["k"].iloc[-1]) if pd.notna(df["k"].iloc[-1]) else 50
    d = float(df["d"].iloc[-1]) if pd.notna(df["d"].iloc[-1]) else 50
    reasons = []
    if close > ma20: reasons.append("站上月線")
    if close > ma5: reasons.append("短線偏強")
    if vol > vol5: reasons.append("量能放大")
    if 45 <= rsi <= 75: reasons.append("RSI健康")
    if k > d: reasons.append("KD偏多")
    if breakout_status == "已突破": reasons.append("突破成立")
    elif breakout_status == "尚未突破": reasons.append("接近壓力")
    if not reasons: reasons.append("條件普通")
    return "、".join(reasons[:3]), round(rsi, 1)


def analyze_one(raw_stock: str, market_adj: int = 0, name_map: dict | None = None):
    name_map = name_map or load_name_map()
    resolved_code, raw_df = resolve_symbol(raw_stock)
    if raw_df.empty or len(raw_df) < 20:
        return None
    df = indicators(raw_df)
    close = round(float(df["Close"].iloc[-1]), 2)

    high5 = float(df["High"].tail(5).max()); low5 = float(df["Low"].tail(5).min())
    high10 = float(df["High"].tail(10).max()); low10 = float(df["Low"].tail(10).min())
    ma5 = float(df["ma5"].iloc[-1]) if pd.notna(df["ma5"].iloc[-1]) else close
    ma20 = float(df["ma20"].iloc[-1]) if pd.notna(df["ma20"].iloc[-1]) else close
    atr = float(df["atr"].iloc[-1]) if pd.notna(df["atr"].iloc[-1]) else max(close * 0.02, 0.5)
    vol = float(df["Volume"].iloc[-1]); vol_prev = float(df["Volume"].iloc[-2]) if len(df) >= 2 else vol
    vol5 = float(df["vol5"].iloc[-1]) if pd.notna(df["vol5"].iloc[-1]) else vol
    k = float(df["k"].iloc[-1]) if pd.notna(df["k"].iloc[-1]) else 50
    d = float(df["d"].iloc[-1]) if pd.notna(df["d"].iloc[-1]) else 50
    bias5 = float(df["bias5"].iloc[-1]) if pd.notna(df["bias5"].iloc[-1]) else 0

    support_candidates = [x for x in [low5, low10, ma5, ma20] if x < close and x > close * 0.93]
    support = max(support_candidates) if support_candidates else max([x for x in [low5, low10, ma5, ma20] if x < close] or [close * 0.96])
    resistance_candidates = [x for x in [high5, high10] if x > close and x < close * 1.12]
    resistance = min(resistance_candidates) if resistance_candidates else close + atr * 0.8
    breakout_base = max(high10, resistance)
    mid_target = breakout_base + atr * 0.8
    final_target = breakout_base + atr * 1.6

    support = round(float(support), 2); resistance = round(float(resistance), 2)
    mid_target = round(float(mid_target), 2); final_target = round(float(final_target), 2); atr = round(float(atr), 2)

    breakout_status, breakout_strength, chase = breakout_analysis(df, resistance)
    score = max(0, calculate_score(df) + market_adj)
    bias = market_bias(df)

    dist_support_pct = (close - support) / close if close else 0
    dist_resist_pct = (resistance - close) / close if close else 0
    if breakout_status == "已突破":
        entry = close * 0.996
    elif dist_resist_pct <= 0.03:
        entry = max(support + atr * 0.35, close - atr * 0.45)
    elif dist_support_pct <= 0.03:
        entry = support + atr * 0.2
    else:
        entry = max(support + atr * 0.3, close - atr * 0.35)
    entry = round(float(min(entry, close * 0.995)), 2)
    stop = round(float(support - atr * 0.25), 2)
    short_target = resistance
    rr = round((short_target - entry) / max(entry - stop, 0.01), 2)

    star, action_label = decision_star_and_action(score, breakout_status, breakout_strength, rr, bias)
    conclusion = trend_conclusion(action_label, breakout_status, bias)
    signal = signal_label(action_label, breakout_status, rr)
    reason, rsi_round = build_reason(df, breakout_status)

    bucket = rank_bucket(signal, conclusion, action_label)
    bucket_score = {"A_優先關注": 300, "B_中性觀察": 200, "C_保守追蹤": 100, "D_後段排除": 0}[bucket]
    vol_ratio = round(vol / vol5, 2) if vol5 else 1.0
    rank = (
        bucket_score
        + signal_priority(signal) * 20
        + conclusion_priority(conclusion) * 12
        + action_priority(action_label) * 10
        + min(max(rr, 0), 3) * 5
        + (4 if breakout_strength == "強" else 2 if breakout_strength == "普通" else 0)
        + min(max(vol_ratio, 0), 3) * 3
        + (3 if k > d else 0)
    )
    vol_change_pct = round((vol - vol_prev) / vol_prev * 100, 2) if vol_prev else 0.0
    vol_trend = "量增" if vol_change_pct > 0 else "量縮" if vol_change_pct < 0 else "持平"
    dist_support = round((close - support) / close * 100, 1) if close else 0
    dist_resistance = round((resistance - close) / close * 100, 1) if close else 0
    dist_target = round((final_target - close) / close * 100, 1) if close else 0

    return {
        "股票": display_name(resolved_code, name_map), "股票代碼": resolved_code, "族群": stock_sector.get(resolved_code, "未分類"),
        "收盤": close, "支撐": support, "短期壓力": resistance, "中繼目標": mid_target, "突破目標": final_target,
        "星級": star, "操作評級": action_label, "結論": conclusion, "交易訊號": signal,
        "排名分組": bucket, "排名原因": rank_reason(bucket, conclusion, signal),
        "盤前建議": bias, "突破狀態": breakout_status, "突破強度": breakout_strength, "追價建議": chase,
        "進場": entry, "停損": stop, "目標": short_target, "風報比": rr, "ATR": atr,
        "RSI": rsi_round, "KD_K": round(k, 1), "KD_D": round(d, 1), "乖離率5日": round(bias5, 2),
        "成交量": int(vol), "昨量": int(vol_prev), "量比5日": vol_ratio, "量能變化": vol_trend, "量能變化%": vol_change_pct,
        "選股理由": reason,
        "摘要1": f"位置評語：現價距短撐約 {dist_support}% ，距短壓約 {dist_resistance}% 。",
        "摘要2": f"策略評語：短線結論偏{conclusion}，交易訊號為 {signal} ，建議進場 {entry:.2f} ，風報比 {rr}。",
        "摘要3": f"量價評語：{vol_trend} {vol_change_pct:+.2f}% ，若有效突破短壓，突破後目標空間約 {dist_target}% 。",
        "_code": resolved_code, "_rank": rank,
    }


def save_snapshot(snapshot_type: str, results):
    snaps = load_snapshots()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for row in results:
        rec = {k: v for k, v in row.items() if not k.startswith("_")}
        rec["時間"] = ts
        rec["類型"] = snapshot_type
        snaps.append(rec)
    save_snapshots(snaps)


def find_latest_snapshot_for_stock(stock_code: str):
    snaps = sorted(load_snapshots(), key=lambda x: x.get("時間", ""), reverse=True)
    q = stock_code.upper()
    for s in snaps:
        code = str(s.get("股票代碼", "")).upper()
        if code == q or code.startswith(q + ".") or q.startswith(code.split(".")[0]):
            return s
    return None


def pair_trades(trades):
    grouped, closed, open_pos = {}, [], []
    for t in trades:
        grouped.setdefault(t["股票"], []).append(t)
    for code, rows in grouped.items():
        buys = []
        for r in sorted(rows, key=lambda x: x["時間"]):
            if r["動作"] == "買進":
                buys.append(r.copy())
            elif r["動作"] == "賣出":
                qty_to_close = r["數量"]
                while qty_to_close > 0 and buys:
                    buy = buys[0]
                    matched = min(qty_to_close, buy["剩餘數量"])
                    pnl = (r["價格"] - buy["價格"]) * matched
                    ret = ((r["價格"] / buy["價格"]) - 1) * 100 if buy["價格"] else 0
                    closed.append({"股票": code, "買進時間": buy["時間"], "賣出時間": r["時間"], "買進價": buy["價格"], "賣出價": r["價格"], "數量": matched, "損益": round(pnl, 2), "報酬率%": round(ret, 2)})
                    buy["剩餘數量"] -= matched
                    qty_to_close -= matched
                    if buy["剩餘數量"] == 0:
                        buys.pop(0)
        for b in buys:
            if b["剩餘數量"] > 0:
                open_pos.append({"股票": code, "買進時間": b["時間"], "買進價": b["價格"], "剩餘數量": b["剩餘數量"]})
    return pd.DataFrame(closed), pd.DataFrame(open_pos)


def signal_stats(df_closed):
    return pd.DataFrame()


def summary_stats(df_closed):
    if df_closed.empty:
        return 0.0, 0, 0.0, 0.0
    wins = (df_closed["損益"] > 0).sum()
    return round(df_closed["損益"].sum(), 2), int(len(df_closed)), round(wins / len(df_closed) * 100, 2), round(df_closed["報酬率%"].mean(), 2)



def light_color(level: str) -> str:
    return {"綠燈": "#16a34a", "黃燈": "#eab308", "紅燈": "#dc2626"}.get(level, "#6b7280")

def signal_light_pack(row: pd.Series):
    trend_light = "綠燈" if row["結論"] in ["看多", "中性偏多"] else "黃燈" if row["結論"] == "中性" else "紅燈"
    entry_light = "綠燈" if row["交易訊號"] == "🔥進場" else "黃燈" if row["交易訊號"] in ["👀觀察", "⏳等待"] else "紅燈"
    volume_light = "綠燈" if row["量能變化"] == "量增" and row["量比5日"] >= 1 else "黃燈" if row["量比5日"] >= 0.8 else "紅燈"
    kd_light = "紅燈" if row["KD_K"] >= 80 else "綠燈" if row["KD_K"] > row["KD_D"] else "黃燈"
    rr_light = "綠燈" if row["風報比"] >= 1.8 else "黃燈" if row["風報比"] >= 1.2 else "紅燈"
    return {
        "趨勢燈號": trend_light,
        "進場燈號": entry_light,
        "量能燈號": volume_light,
        "KD燈號": kd_light,
        "風報燈號": rr_light
    }

def render_signal_lights(row: pd.Series):
    lights = signal_light_pack(row)
    cols = st.columns(5)
    for col, (name, level) in zip(cols, lights.items()):
        color = light_color(level)
        col.markdown(
            f"""
            <div style="border:1px solid #334155;border-radius:10px;padding:12px 10px;text-align:center;background:#0b1220;">
                <div style="font-size:13px;color:#cbd5e1;margin-bottom:8px;">{name}</div>
                <div style="display:inline-block;width:14px;height:14px;border-radius:999px;background:{color};margin-right:6px;"></div>
                <span style="font-size:22px;font-weight:700;color:white;">{level}</span>
            </div>
            """,
            unsafe_allow_html=True
        )


def ensure_selected_code(select_source: pd.DataFrame):
    if select_source.empty:
        st.session_state.selected_code = None
        return
    valid_codes = select_source["_code"].tolist()
    if st.session_state.selected_code not in valid_codes:
        st.session_state.selected_code = valid_codes[0]

def move_selected(select_source: pd.DataFrame, step: int):
    if select_source.empty:
        return
    codes = select_source["_code"].tolist()
    ensure_selected_code(select_source)
    try:
        idx = codes.index(st.session_state.selected_code)
    except ValueError:
        idx = 0
    new_idx = max(0, min(len(codes) - 1, idx + step))
    st.session_state.selected_code = codes[new_idx]


def build_favorites_panel(favs, market_score_adj, name_map):
    rows = []
    for code in favs:
        try:
            item = analyze_one(code, market_score_adj, name_map)
            if item is not None:
                item["趨勢燈號"] = signal_light_pack(item)["趨勢燈號"]
                item["進場燈號"] = signal_light_pack(item)["進場燈號"]
                item["量能燈號"] = signal_light_pack(item)["量能燈號"]
                rows.append(item)
        except Exception:
            pass
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).sort_values(["_rank", "風報比"], ascending=[False, False])
    return df

def make_candle_figure(df: pd.DataFrame, row: pd.Series):
    show_df = df.tail(90).copy()
    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True, vertical_spacing=0.03,
        row_heights=[0.52, 0.18, 0.15, 0.15],
        subplot_titles=("K線 / 均線", "成交量", "KD", "5日乖離率")
    )

    fig.add_trace(
        go.Candlestick(
            x=show_df.index,
            open=show_df["Open"], high=show_df["High"], low=show_df["Low"], close=show_df["Close"],
            name="K線",
            increasing_line_color="#ef5350", increasing_fillcolor="#ef5350",
            decreasing_line_color="#26a69a", decreasing_fillcolor="#26a69a"
        ),
        row=1, col=1
    )

    for col, name in [("ma5", "5MA"), ("ma20", "20MA"), ("ma60", "60MA"), ("ma120", "120MA")]:
        fig.add_trace(
            go.Scatter(x=show_df.index, y=show_df[col], mode="lines", name=name),
            row=1, col=1
        )

    for value, label in [
        (row["支撐"], "支撐"),
        (row["短期壓力"], "短壓"),
        (row["進場"], "進場"),
        (row["停損"], "停損"),
        (row["突破目標"], "突破目標")
    ]:
        fig.add_hline(y=float(value), line_dash="dot", annotation_text=label, row=1, col=1)

    colors = ["#26a69a" if c < o else "#ef5350" for o, c in zip(show_df["Open"], show_df["Close"])]
    fig.add_trace(
        go.Bar(x=show_df.index, y=show_df["Volume"], name="成交量", marker_color=colors),
        row=2, col=1
    )

    fig.add_trace(go.Scatter(x=show_df.index, y=show_df["k"], mode="lines", name="K值"), row=3, col=1)
    fig.add_trace(go.Scatter(x=show_df.index, y=show_df["d"], mode="lines", name="D值"), row=3, col=1)
    fig.add_hline(y=80, line_dash="dot", row=3, col=1)
    fig.add_hline(y=20, line_dash="dot", row=3, col=1)

    bias_colors = ["#ef5350" if x >= 0 else "#26a69a" for x in show_df["bias5"]]
    fig.add_trace(
        go.Bar(x=show_df.index, y=show_df["bias5"], name="5日乖離率", marker_color=bias_colors),
        row=4, col=1
    )
    fig.add_hline(y=0, line_dash="dot", row=4, col=1)

    fig.update_layout(
        height=650 if st.session_state.get("mobile_mode", False) else 920,
        xaxis_rangeslider_visible=False,
        showlegend=True,
        margin=dict(l=20, r=20, t=30, b=20),
        legend=dict(orientation="h")
    )
    return fig


ensure_names_file()
name_map = load_name_map()
if "favorites" not in st.session_state:
    st.session_state.favorites = load_favorites()
if "results_data" not in st.session_state:
    st.session_state.results_data = []
if "selected_code" not in st.session_state:
    st.session_state.selected_code = None
if "input_value" not in st.session_state:
    st.session_state.input_value = ""
if "analysis_mode" not in st.session_state:
    st.session_state.analysis_mode = "idle"
if "preset_strategy" not in st.session_state:
    st.session_state.preset_strategy = "none"
if "mobile_mode" not in st.session_state:
    st.session_state.mobile_mode = True

with st.sidebar:
    st.header("⭐ 我的最愛")
    favs = st.session_state.favorites
    if favs:
        fav_display = [display_name(x, name_map) for x in favs]
        fav_choice = st.selectbox("快速載入", [""] + fav_display, index=0)
        if st.button("載入我的最愛") and fav_choice:
            st.session_state.input_value = fav_choice.split("（")[0]
            st.rerun()
        if st.button("一鍵分析最愛"):
            market_info = market_filter()
            tmp = [analyze_one(code, market_info["score_adj"], name_map) for code in favs]
            tmp = [x for x in tmp if x is not None]
            tmp = sorted(tmp, key=lambda x: x["_rank"], reverse=True)
            st.session_state.results_data = tmp
            st.session_state.selected_code = tmp[0]["_code"] if tmp else None
            st.session_state.analysis_mode = "favorites"
            st.rerun()
        if st.button("清空我的最愛"):
            st.session_state.favorites = []
            save_favorites([])
            st.rerun()
    else:
        st.caption("目前還沒有收藏股票。")

tab1, tab2, tab3 = st.tabs(["📈 分析中心", "🕘 快照中心", "📒 交易中心"])



with tab1:
    market_info = market_filter()

    top_a, top_b = st.columns([3, 1])
    with top_a:
        st.markdown("### 市場概況")
    with top_b:
        st.toggle("手機精簡版", key="mobile_mode")

    mc1, mc2 = st.columns(2)
    mc1.metric("大盤濾網", market_info["label"])
    mc2.metric("加權指數", f'{market_info["close"]:.2f}' if market_info["close"] else "N/A")
    st.info(market_info["text"])

    with st.container(border=True):
        st.markdown("### 操作列")
        op1, op2 = st.columns([3, 1])
        with op1:
            stocks = st.text_input("輸入股票（可直接打四位數）", key="input_value", placeholder="例如：1815, 2330, 2330.TW")
        with op2:
            top_n = st.selectbox("顯示檔數", [5,10,15,20], index=0)

        op3, op4 = st.columns(2)
        manual_search = op3.button("搜尋", use_container_width=True)
        auto_pick = op4.button("自動挑股", use_container_width=True)

    if manual_search:
        tmp = []
        for stock in [s.strip() for s in stocks.split(",") if s.strip()]:
            item = analyze_one(stock, market_info["score_adj"], name_map)
            if item is not None:
                tmp.append(item)
        tmp = sorted(tmp, key=lambda x: x["_rank"], reverse=True)
        st.session_state.results_data = tmp
        st.session_state.selected_code = tmp[0]["_code"] if tmp else None
        st.session_state.analysis_mode = "manual"

    if auto_pick:
        candidates = []
        for code in watchlist_default:
            item = analyze_one(code, market_info["score_adj"], name_map)
            if item is None:
                continue
            if item["排名分組"] == "D_後段排除":
                continue
            if item["風報比"] < 1.0:
                continue
            if item["突破狀態"] == "假突破風險":
                continue
            candidates.append(item)
        candidates = sorted(candidates, key=lambda x: x["_rank"], reverse=True)
        st.session_state.results_data = candidates[:top_n]
        st.session_state.selected_code = st.session_state.results_data[0]["_code"] if st.session_state.results_data else None
        st.session_state.analysis_mode = "auto"

    results = st.session_state.results_data
    if not results:
        st.info("請先輸入股票後按『搜尋』，或使用『自動挑股』。")
    else:
        if st.session_state.analysis_mode == "manual":
            st.caption("目前模式：手動搜尋")
        elif st.session_state.analysis_mode == "auto":
            st.caption("目前模式：自動挑股")
        elif st.session_state.analysis_mode == "favorites":
            st.caption("目前模式：最愛分析")

        df_result = pd.DataFrame(results)
        df_result["趨勢燈號"] = df_result.apply(lambda r: signal_light_pack(r)["趨勢燈號"], axis=1)
        df_result["進場燈號"] = df_result.apply(lambda r: signal_light_pack(r)["進場燈號"], axis=1)
        df_result["量能燈號"] = df_result.apply(lambda r: signal_light_pack(r)["量能燈號"], axis=1)

        favs = st.session_state.favorites
        if favs:
            with st.container(border=True):
                st.markdown("### 我的最愛追蹤面板")
                fav_df = build_favorites_panel(favs, market_info["score_adj"], name_map)
                if fav_df.empty:
                    st.caption("目前最愛股無法取得分析資料。")
                else:
                    fm1, fm2 = st.columns(2)
                    fm1.metric("最愛股檔數", len(fav_df))
                    fm2.metric("平均風報比", round(fav_df["風報比"].mean(), 2) if not fav_df.empty else 0)
                    fav_core = fav_df[["股票","結論","交易訊號","風報比","量能變化","量能變化%"]]
                    st.dataframe(fav_core, use_container_width=True, hide_index=True)
                    top_fav = fav_df.head(min(3, len(fav_df)))
                    st.markdown("#### 最愛快速查看")
                    fav_cols = st.columns(len(top_fav))
                    for col, (_, r) in zip(fav_cols, top_fav.iterrows()):
                        if col.button(f'{r["股票"]}', use_container_width=True, key=f'fav_jump_{r["_code"]}'):
                            st.session_state.results_data = fav_df.to_dict("records")
                            st.session_state.selected_code = r["_code"]
                            st.session_state.analysis_mode = "favorites"
                            st.rerun()

        with st.expander("策略與篩選", expanded=not st.session_state.mobile_mode):
            ps1, ps2, ps3 = st.columns(3)
            if ps1.button("偏多股", use_container_width=True):
                st.session_state.preset_strategy = "bullish"
            if ps2.button("量增股", use_container_width=True):
                st.session_state.preset_strategy = "volume_up"
            if ps3.button("高風報比", use_container_width=True):
                st.session_state.preset_strategy = "high_rr"

            ps4, ps5, ps6, ps7 = st.columns(4)
            if ps4.button("突破觀察", use_container_width=True):
                st.session_state.preset_strategy = "breakout_watch"
            if ps5.button("低風險觀察", use_container_width=True):
                st.session_state.preset_strategy = "low_risk"
            if ps6.button("排除紅燈", use_container_width=True):
                st.session_state.preset_strategy = "hide_red_only"
            if ps7.button("取消策略", use_container_width=True):
                st.session_state.preset_strategy = "none"

            preset = st.session_state.preset_strategy
            fl1, fl2 = st.columns(2)
            trend_filter = fl1.selectbox("趨勢燈號", ["全部", "綠燈", "黃燈", "紅燈"], index=0)
            signal_filter = fl2.selectbox("交易訊號", ["全部", "🔥進場", "👀觀察", "⏳等待", "❌不進"], index=0)
            fl3, fl4, fl5 = st.columns(3)
            volume_filter = fl3.selectbox("量能條件", ["全部", "量增", "量縮", "量比>=1"], index=0)
            min_rr = fl4.selectbox("最低風報比", ["全部", ">=1.0", ">=1.2", ">=1.5", ">=2.0"], index=0)
            hide_red = fl5.checkbox("隱藏紅燈/不進", value=False)

            if preset == "bullish":
                trend_filter = "綠燈"; min_rr = ">=1.2"; hide_red = True
            elif preset == "volume_up":
                volume_filter = "量增"; min_rr = ">=1.0"
            elif preset == "high_rr":
                min_rr = ">=1.5"; hide_red = True
            elif preset == "breakout_watch":
                signal_filter = "⏳等待"; min_rr = ">=1.0"
            elif preset == "low_risk":
                trend_filter = "綠燈"; min_rr = ">=1.2"; volume_filter = "量比>=1"; hide_red = True
            elif preset == "hide_red_only":
                hide_red = True

            filtered_df = df_result.copy()
            if trend_filter != "全部":
                filtered_df = filtered_df[filtered_df["趨勢燈號"] == trend_filter]
            if signal_filter != "全部":
                filtered_df = filtered_df[filtered_df["交易訊號"] == signal_filter]
            if volume_filter == "量增":
                filtered_df = filtered_df[filtered_df["量能變化"] == "量增"]
            elif volume_filter == "量縮":
                filtered_df = filtered_df[filtered_df["量能變化"] == "量縮"]
            elif volume_filter == "量比>=1":
                filtered_df = filtered_df[filtered_df["量比5日"] >= 1]
            if min_rr != "全部":
                rr_value = float(min_rr.replace(">=", ""))
                filtered_df = filtered_df[filtered_df["風報比"] >= rr_value]
            if hide_red:
                filtered_df = filtered_df[(filtered_df["進場燈號"] != "紅燈") & (filtered_df["交易訊號"] != "❌不進")]

            preset_name_map = {
                "none": "自訂",
                "bullish": "偏多股",
                "volume_up": "量增股",
                "high_rr": "高風報比",
                "breakout_watch": "突破觀察",
                "low_risk": "低風險觀察",
                "hide_red_only": "排除紅燈"
            }
            st.caption(f"目前預設策略：{preset_name_map.get(preset, '自訂')}")

        with st.container(border=True):
            panel_df = filtered_df.copy()
            green_count = int((panel_df["趨勢燈號"] == "綠燈").sum()) if not panel_df.empty else 0
            volume_up_count = int((panel_df["量能變化"] == "量增").sum()) if not panel_df.empty else 0
            actionable_count = int(panel_df["交易訊號"].isin(["🔥進場", "👀觀察"]).sum()) if not panel_df.empty else 0
            avg_rr = round(panel_df["風報比"].mean(), 2) if not panel_df.empty else 0.0

            st.markdown("### 盤前候選股面板")
            pc1, pc2 = st.columns(2)
            pc1.metric("篩選後檔數", len(panel_df))
            pc2.metric("綠燈數", green_count)
            pc3, pc4 = st.columns(2)
            pc3.metric("量增股數", volume_up_count)
            pc4.metric("可優先觀察", actionable_count)
            st.caption(f"篩選名單平均風報比：{avg_rr}")

            if not panel_df.empty:
                top_cards = panel_df.sort_values(["風報比", "量比5日"], ascending=[False, False]).head(3)
                st.markdown("#### TOP 候選")
                top_cols = st.columns(1 if st.session_state.mobile_mode else 3)
                if st.session_state.mobile_mode:
                    top_cols = [st.container(), st.container(), st.container()]
                for col, (_, r) in zip(top_cols, top_cards.iterrows()):
                    with col:
                        st.markdown(
                            f"""
                            <div style="border:1px solid #334155;border-radius:12px;padding:12px;background:#0b1220;margin-bottom:8px;">
                                <div style="font-size:18px;font-weight:700;color:white;margin-bottom:8px;">{r["股票"]}</div>
                                <div style="color:#cbd5e1;font-size:13px;margin-bottom:6px;">{r["族群"]}｜{r["結論"]}｜{r["交易訊號"]}</div>
                                <div style="color:#cbd5e1;font-size:13px;">風報比：{r["風報比"]}｜量比：{r["量比5日"]}</div>
                                <div style="color:#cbd5e1;font-size:13px;">支撐：{r["支撐"]}｜短壓：{r["短期壓力"]}</div>
                                <div style="color:#93c5fd;font-size:13px;margin-top:6px;">{r["排名原因"]}</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

        with st.container(border=True):
            st.markdown("### 總表分析")
            sort_c1, sort_c2 = st.columns([2, 3])
            sort_options = {
                "預設排名": ("_rank", False),
                "風報比（高→低）": ("風報比", False),
                "量比（高→低）": ("量比5日", False),
                "KD-K（高→低）": ("KD_K", False),
                "乖離率（高→低）": ("乖離率5日", False),
                "乖離率（低→高）": ("乖離率5日", True),
                "收盤價（高→低）": ("收盤", False),
                "收盤價（低→高）": ("收盤", True),
                "RSI（高→低）": ("RSI", False),
                "量能變化%（高→低）": ("量能變化%", False),
            }
            sort_label = sort_c1.selectbox("總表排序方式", list(sort_options.keys()), index=0)
            sort_col, asc = sort_options[sort_label]
            sorted_df = filtered_df.sort_values(sort_col, ascending=asc).copy()
            sort_c2.caption(f"目前排序：{sort_label}")

            mobile_core_cols = ["股票","結論","交易訊號","趨勢燈號","風報比"]
            desktop_core_cols = ["股票","族群","收盤","星級","結論","交易訊號","趨勢燈號","進場燈號","量能燈號","支撐","短期壓力","風報比"]
            detail_cols = ["中繼目標","突破目標","排名分組","排名原因","RSI","KD_K","KD_D","乖離率5日","量能變化","量能變化%","量比5日"]
            core_cols = mobile_core_cols if st.session_state.mobile_mode else desktop_core_cols
            st.dataframe(sorted_df[core_cols], use_container_width=True, hide_index=True)

            with st.expander("展開詳細欄位 / 排名系統", expanded=False):
                detail_df = sorted_df[list(dict.fromkeys(core_cols + detail_cols))]
                st.dataframe(detail_df, use_container_width=True, hide_index=True)
                rank_df = sorted_df.copy()
                rank_df.insert(0,"排名", range(1, len(rank_df)+1))
                st.dataframe(rank_df[["排名","股票","排名分組","排名原因","星級","操作評級","結論","交易訊號","風報比","RSI","KD_K","KD_D"]], use_container_width=True, hide_index=True)

        select_source = sorted_df if ("sorted_df" in locals() and not sorted_df.empty) else (filtered_df if not filtered_df.empty else df_result)
        ensure_selected_code(select_source)
        option_map = {row["股票"]: row["_code"] for _, row in select_source.iterrows()}
        reverse_map = {v:k for k,v in option_map.items()}

        with st.container(border=True):
            st.markdown("### 單股詳細分析")
            if st.session_state.mobile_mode:
                labels = list(option_map.keys())
                current_label = reverse_map.get(st.session_state.selected_code, labels[0])
                selected_display = st.selectbox("選擇查看單股", labels, index=labels.index(current_label))
                st.session_state.selected_code = option_map[selected_display]
                nav1, nav2 = st.columns(2)
                if nav1.button("上一檔", use_container_width=True):
                    move_selected(select_source, -1)
                    st.rerun()
                if nav2.button("下一檔", use_container_width=True):
                    move_selected(select_source, 1)
                    st.rerun()
                st.caption(f"目前位置：{list(option_map.values()).index(st.session_state.selected_code)+1} / {len(option_map)}")
            else:
                nav1, nav2, nav3, nav4 = st.columns([1, 1, 4, 2])
                with nav1:
                    if st.button("上一檔", use_container_width=True):
                        move_selected(select_source, -1)
                with nav2:
                    if st.button("下一檔", use_container_width=True):
                        move_selected(select_source, 1)
                with nav3:
                    labels = list(option_map.keys())
                    current_label = reverse_map.get(st.session_state.selected_code, labels[0])
                    selected_display = st.selectbox("選擇查看單股", labels, index=labels.index(current_label))
                    st.session_state.selected_code = option_map[selected_display]
                with nav4:
                    st.caption(f"目前位置：{list(option_map.values()).index(st.session_state.selected_code)+1} / {len(option_map)}")

                st.markdown("#### 快速查看")
                quick_cols = st.columns(min(5, len(select_source)))
                top_quick = select_source.head(5)
                for col, (_, quick_row) in zip(quick_cols, top_quick.iterrows()):
                    if col.button(quick_row["股票"], use_container_width=True):
                        st.session_state.selected_code = quick_row["_code"]
                        st.rerun()

            row = df_result[df_result["_code"] == st.session_state.selected_code].iloc[0]
            df_chart = indicators(download_symbol(st.session_state.selected_code))

            f1, f2 = st.columns([1,5])
            with f1:
                if st.session_state.selected_code not in st.session_state.favorites:
                    if st.button("加入最愛"):
                        st.session_state.favorites.append(st.session_state.selected_code)
                        save_favorites(st.session_state.favorites)
                        st.rerun()
                else:
                    if st.button("移除最愛"):
                        st.session_state.favorites = [x for x in st.session_state.favorites if x != st.session_state.selected_code]
                        save_favorites(st.session_state.favorites)
                        st.rerun()
            with f2:
                if st.session_state.selected_code in st.session_state.favorites:
                    st.success(f"{display_name(st.session_state.selected_code, name_map)} 已收藏到我的最愛。")

            st.subheader(f"{reverse_map[st.session_state.selected_code]} 詳細分析")
            metric_pairs = [
                ("星級", row["星級"]), ("操作評級", row["操作評級"]), ("結論", row["結論"]), ("交易訊號", row["交易訊號"]),
                ("最新收盤", f'{row["收盤"]:.2f}'), ("短期支撐", f'{row["支撐"]:.2f}'), ("短期壓力", f'{row["短期壓力"]:.2f}'), ("突破目標", f'{row["突破目標"]:.2f}'),
                ("建議進場", f'{row["進場"]:.2f}'), ("停損", f'{row["停損"]:.2f}'), ("中繼目標", f'{row["中繼目標"]:.2f}'), ("風報比", f'{row["風報比"]:.2f}'),
                ("KD-K", f'{row["KD_K"]:.1f}'), ("KD-D", f'{row["KD_D"]:.1f}'), ("5日乖離率", f'{row["乖離率5日"]:.2f}%'), ("量比(5日)", f'{row["量比5日"]:.2f}'),
                ("今日成交量", f'{int(row["成交量"]):,}'), ("昨日成交量", f'{int(row["昨量"]):,}'), ("量能變化", f'{row["量能變化"]} {row["量能變化%"]:+.2f}%')
            ]
            cols_per_row = 2 if st.session_state.mobile_mode else 4
            for i in range(0, len(metric_pairs), cols_per_row):
                cols = st.columns(cols_per_row)
                for col, (label, value) in zip(cols, metric_pairs[i:i+cols_per_row]):
                    col.metric(label, value)

            st.markdown("#### 訊號燈號")
            render_signal_lights(row)

            with st.expander("展開圖表", expanded=not st.session_state.mobile_mode):
                st.plotly_chart(make_candle_figure(df_chart, row), use_container_width=True)

            st.info(row["摘要1"]); st.info(row["摘要2"]); st.info(row["摘要3"])
            if row["交易訊號"] == "🔥進場":
                st.success(f"操作建議：可列入優先觀察，重點看 {row['短期壓力']:.2f} 是否有效站上。")
            elif row["交易訊號"] == "⏳等待":
                st.warning(f"操作建議：先等突破或回測確認，不建議在 {row['收盤']:.2f} 直接追價。")
            elif row["交易訊號"] == "❌不進":
                st.error("操作建議：目前偏保守，先不進場，等待結構或燈號改善。")
            else:
                st.info("操作建議：可持續觀察量價與燈號變化，再決定是否列入候選。")

            with st.expander("短線重點資訊 / 最近標題", expanded=False):
                news_items = get_stock_news(st.session_state.selected_code)
                summary_bullets = summarize_event_signals(row, news_items)
                for b in summary_bullets:
                    st.info(b)
                if news_items:
                    for item in news_items[:2]:
                        title = item.get("title", "")
                        meta = "｜".join([x for x in [item.get("publisher", ""), item.get("time", "")] if x])
                        if item.get("link"):
                            st.markdown(f"- [{title}]({item['link']})")
                        else:
                            st.write(f"- {title}")
                        if meta:
                            st.caption(meta)
                else:
                    st.caption("目前抓不到新聞資料，因此改以技術面與量價重點為主。")


with tab2:
    results = st.session_state.results_data
    with st.container(border=True):
        st.markdown("### 快照儲存")
        if results:
            s1, s2, s3 = st.columns(3)
            if s1.button("儲存盤前快照", use_container_width=True):
                save_snapshot("盤前", results)
                st.success("已儲存盤前快照。")
            if s2.button("儲存盤中快照", use_container_width=True):
                save_snapshot("盤中", results)
                st.success("已儲存盤中快照。")
            if s3.button("儲存盤後快照", use_container_width=True):
                save_snapshot("盤後", results)
                st.success("已儲存盤後快照。")
        else:
            st.info("請先到『分析中心』跑出分析結果，再來儲存快照。")

    snapshots = load_snapshots()
    df_snap = pd.DataFrame(snapshots) if snapshots else pd.DataFrame()

    with st.container(border=True):
        st.markdown("### 快照管理")
        if df_snap.empty:
            st.caption("目前沒有快照紀錄。")
        else:
            c1, c2, c3 = st.columns(3)
            with c1:
                stock_options = ["全部"] + sorted(df_snap["股票"].dropna().unique().tolist())
                hist_stock = st.selectbox("選擇股票", stock_options, index=0, key="hist_stock")
            with c2:
                type_options = ["全部"] + sorted(df_snap["類型"].dropna().unique().tolist())
                hist_type = st.selectbox("選擇類型", type_options, index=0, key="hist_type")
            with c3:
                hist_n = st.selectbox("顯示筆數", [20, 50, 100, 200], index=1, key="hist_n")

            hist = df_snap.copy()
            if hist_stock != "全部":
                hist = hist[hist["股票"] == hist_stock]
            if hist_type != "全部":
                hist = hist[hist["類型"] == hist_type]
            hist = hist.sort_values("時間", ascending=False).head(hist_n)

            st.dataframe(
                hist[[c for c in ["時間","類型","股票","收盤","星級","結論","交易訊號","風報比","KD_K","KD_D","乖離率5日","量能變化","量能變化%"] if c in hist.columns]],
                use_container_width=True,
                hide_index=True
            )

            d1, d2 = st.columns(2)
            if d1.button("刪除目前篩選結果", use_container_width=True):
                delete_keys = set(
                    hist.apply(lambda r: (r.get("時間",""), r.get("類型",""), r.get("股票","")), axis=1).tolist()
                )
                remain = []
                for s in snapshots:
                    key = (s.get("時間",""), s.get("類型",""), s.get("股票",""))
                    if key not in delete_keys:
                        remain.append(s)
                save_snapshots(remain)
                st.success("已刪除目前篩選結果的快照。")
                st.rerun()

            if d2.button("清空全部快照", use_container_width=True):
                save_snapshots([])
                st.success("已清空全部快照。")
                st.rerun()

with tab3:

    t1,t2,t3,t4 = st.columns(4)
    trade_stock = t1.text_input("股票代碼", placeholder="例如 8046 或 8046.TW")
    trade_price = t2.number_input("成交價格", min_value=0.0, value=0.0, step=0.01)
    trade_qty = t3.number_input("數量", min_value=1, value=1000, step=1)
    trade_action = t4.selectbox("動作", ["買進","賣出"])
    trade_note = st.text_input("備註", placeholder="例如：盤中追價 / 回測進場 / 手動出場")

    b1,b2 = st.columns(2)
    if b1.button("新增交易紀錄"):
        code = trade_stock.strip().upper()
        if code:
            latest_snap = find_latest_snapshot_for_stock(code)
            record = {"時間": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "股票": code, "價格": float(trade_price), "數量": int(trade_qty), "動作": trade_action, "備註": trade_note}
            if trade_action == "買進":
                record["剩餘數量"] = int(trade_qty)
                if latest_snap:
                    record["快照類型"] = latest_snap.get("類型","")
                    record["快照結論"] = latest_snap.get("結論","")
                    record["快照訊號"] = latest_snap.get("交易訊號","")
                    record["快照星級"] = latest_snap.get("星級","")
            trades = load_trades(); trades.append(record); save_trades(trades); st.success("已新增交易紀錄。"); st.rerun()
    if b2.button("清空全部交易紀錄"):
        save_trades([]); st.success("已清空全部交易紀錄。"); st.rerun()

    trades = load_trades()
    df_trades = pd.DataFrame(trades)
    if not df_trades.empty:
        with st.expander("查看原始交易紀錄"):
            st.dataframe(df_trades, use_container_width=True, hide_index=True)

    df_closed, df_open = pair_trades(trades)
    total_pnl, total_count, win_rate, avg_ret = summary_stats(df_closed)
    s1,s2,s3,s4 = st.columns(4)
    s1.metric("總損益", f"{total_pnl:.2f}"); s2.metric("交易次數", total_count); s3.metric("勝率", f"{win_rate:.2f}%"); s4.metric("平均報酬率", f"{avg_ret:.2f}%")
    st.subheader("已完成交易")
    st.dataframe(df_closed if not df_closed.empty else pd.DataFrame(), use_container_width=True, hide_index=True)
    st.subheader("尚未平倉")
    st.dataframe(df_open if not df_open.empty else pd.DataFrame(), use_container_width=True, hide_index=True)
