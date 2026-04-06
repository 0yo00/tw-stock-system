import re
import pandas as pd


def market_bias(df: pd.DataFrame):
    if len(df) < 2:
        return "中性（等待確認）"
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


def price_volume_analysis(df: pd.DataFrame):
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else last
    close = float(last["Close"]); prev_close = float(prev["Close"])
    open_ = float(last["Open"]); high = float(last["High"]); low = float(last["Low"])
    vol = float(last["Volume"]); prev_vol = float(prev["Volume"]) if pd.notna(prev["Volume"]) else vol
    vol5 = float(df["vol5"].iloc[-1]) if "vol5" in df.columns and pd.notna(df["vol5"].iloc[-1]) else vol

    price_up = close > prev_close
    price_down = close < prev_close
    vol_up = vol > prev_vol and vol5 and vol / vol5 >= 1.0
    vol_down = vol < prev_vol or (vol5 and vol / vol5 < 1.0)

    gap_up = open_ > prev_close * 1.01
    gap_down = open_ < prev_close * 0.99
    close_near_low = close <= low + (high - low) * 0.25 if high > low else False
    close_near_high = close >= high - (high - low) * 0.25 if high > low else False
    red_candle = close < open_
    green_candle = close > open_

    if gap_up and red_candle and price_down and vol_up and close_near_low:
        return "開高走低爆量", "紅燈", ["開盤跳高後一路走弱", "收盤接近日低", "量能放大但價格轉弱"]
    if gap_up and green_candle and price_up and vol_up and close_near_high:
        return "跳空上漲爆量", "綠燈", ["跳空上漲", "收盤接近日高", "量價同步轉強"]

    if price_up and vol_up:
        return "價漲量增", "綠燈", ["價格上漲且量能放大", "多方動能延續", "續強機率較高"]
    if price_up and vol_down:
        return "價漲量縮", "黃燈", ["價格上漲但量能未跟上", "續航待確認", "不宜過度追價"]
    if price_down and vol_up:
        return "價跌量增", "紅燈", ["價格下跌且量能放大", "賣壓增強", "偏空解讀"]
    if price_down and vol_down:
        return "價跌量縮", "黃燈", ["價格回落但量能收斂", "弱勢整理或跌勢暫緩", "等待下一步方向"]

    if gap_down and price_down:
        return "跳空下跌", "紅燈", ["開盤跳空轉弱", "市場承接偏弱", "先保守看待"]

    return "量價中性", "黃燈", ["價量沒有明顯共振", "暫以觀察為主", "等待更多確認"]


def _safe_float(val, default=0.0):
    import math
    try:
        f = float(val) if val is not None and val != "" else default
        return default if math.isnan(f) or math.isinf(f) else f
    except (ValueError, TypeError):
        return default

def build_long_daytrade(row: dict):
    close = _safe_float(row.get("收盤", 0))
    support = _safe_float(row.get("支撐", 0))
    resistance = _safe_float(row.get("短期壓力", 0))
    atr = _safe_float(row.get("ATR", 0))
    if close <= 0:
        return {"多方建議進場": "", "多方停損": "", "多方當沖目標1": "", "多方當沖目標2": "", "多方適合當沖": "否"}
    if atr <= 0:
        atr = close * 0.02

    entry = round(max(close - atr * 0.3, support + atr * 0.15) if support > 0 and support < close else close - atr * 0.3, 2)
    entry = round(min(entry, close * 0.998), 2)
    stop = round(entry - atr * 0.5, 2)
    if stop >= entry:
        stop = round(entry - max(atr * 0.3, 0.01), 2)
    t1 = round(entry + atr * 0.6, 2)
    t2 = round(entry + atr * 1.0, 2)
    if resistance > 0 and t2 > resistance:
        t2 = round(resistance, 2)
    if t1 >= t2:
        t1 = round(entry + (t2 - entry) * 0.5, 2)

    risk = entry - stop
    reward = t1 - entry
    suitable = "是" if risk > 0 and reward / max(risk, 0.01) >= 1.0 and atr >= close * 0.015 else "否"

    return {
        "多方建議進場": entry,
        "多方停損": stop,
        "多方當沖目標1": t1,
        "多方當沖目標2": t2,
        "多方適合當沖": suitable,
    }


def build_short_daytrade(row: dict):
    close = _safe_float(row.get("收盤", 0))
    support = _safe_float(row.get("支撐", 0))
    resistance = _safe_float(row.get("短期壓力", 0))
    atr = _safe_float(row.get("ATR", 0))
    if close <= 0:
        return {"空方建議進場": "", "空方停損": "", "空方當沖目標1": "", "空方當沖目標2": "", "空方適合當沖": "否"}
    if atr <= 0:
        atr = close * 0.02

    entry = round(min(close + atr * 0.2, resistance - atr * 0.1) if resistance > 0 and resistance > close else close + atr * 0.2, 2)
    entry = round(max(entry, close * 1.002), 2)
    stop = round(entry + atr * 0.5, 2)
    if resistance > 0:
        stop = round(min(stop, resistance + atr * 0.3), 2)
    t1 = round(entry - atr * 0.6, 2)
    t2 = round(entry - atr * 1.0, 2)
    if support > 0 and t2 < support:
        t2 = round(support, 2)
    if t1 <= t2:
        t1 = round(entry - (entry - t2) * 0.5, 2)

    risk = stop - entry
    reward = entry - t1
    suitable = "是" if risk > 0 and reward / max(risk, 0.01) >= 1.0 and atr >= close * 0.015 else "否"

    return {
        "空方建議進場": entry,
        "空方停損": stop,
        "空方當沖目標1": t1,
        "空方當沖目標2": t2,
        "空方適合當沖": suitable,
    }


def analyze_one(raw_stock: str, market_adj: int, name_map: dict, resolve_symbol, indicators, display_name_func, stock_sector: dict, liquidity_builder=None):
    resolved_code, raw_df = resolve_symbol(raw_stock)
    if raw_df.empty or len(raw_df) < 20:
        return None

    try:
        df = indicators(raw_df)
    except Exception:
        return None

    if df.empty or len(df) < 20:
        return None

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
    if stop >= entry:
        stop = round(entry - max(atr * 0.25, 0.01), 2)
    short_target = resistance
    rr = max(0.0, round((short_target - entry) / max(entry - stop, 0.01), 2))

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
    pv_label, pv_light, pv_reasons = price_volume_analysis(df)
    dist_support = round((close - support) / close * 100, 1) if close else 0
    dist_resistance = round((resistance - close) / close * 100, 1) if close else 0
    dist_target = round((final_target - close) / close * 100, 1) if close else 0

    liquidity_profile = {}
    if liquidity_builder is not None:
        try:
            liquidity_profile = liquidity_builder(df) or {}
        except Exception as e:
            liquidity_profile = {
                "流動性20日均量張": 0.0,
                "流動性20日均值億": 0.0,
                "流動性20日均振幅%": 0.0,
                "流動性5日均量張": 0.0,
                "流動性5日低量天數": 0,
                "流動性5日穩定比": 0.0,
                "流動性20日中位量張": 0.0,
                "流動性爆量比": 0.0,
                "流動性單日爆量偏乾": "否",
                "流動性達標": "否",
                "流動性分數": 0.0,
                "流動性結論": "流動性計算失敗",
                "流動性摘要": f"流動性計算失敗：{type(e).__name__}",
                "流動性排除原因": f"流動性計算失敗：{type(e).__name__}",
            }

    _dt_base = {"收盤": close, "支撐": support, "短期壓力": resistance, "ATR": atr}
    long_dt = build_long_daytrade(_dt_base)
    short_dt = build_short_daytrade(_dt_base)

    return {
        "股票": display_name_func(resolved_code, name_map), "股票代碼": resolved_code, "族群": stock_sector.get(resolved_code, "未分類"),
        "收盤": close, "支撐": support, "短期壓力": resistance, "ATR": atr,
        "星級": star, "操作評級": action_label, "結論": conclusion, "交易訊號": signal,
        "排名分組": bucket, "排名原因": rank_reason(bucket, conclusion, signal),
        "盤前建議": bias, "突破狀態": breakout_status, "突破強度": breakout_strength, "追價建議": chase,
        "進場": entry, "停損": stop, "風報比": rr,
        "RSI": rsi_round, "KD_K": round(k, 1), "KD_D": round(d, 1),
        "KDJ_J": round(float(df["j"].iloc[-1]), 1) if "j" in df.columns and pd.notna(df["j"].iloc[-1]) else 50.0,
        "MACD_DIF": round(float(df["macd_dif"].iloc[-1]), 2) if "macd_dif" in df.columns and pd.notna(df["macd_dif"].iloc[-1]) else 0.0,
        "MACD_DEA": round(float(df["macd_dea"].iloc[-1]), 2) if "macd_dea" in df.columns and pd.notna(df["macd_dea"].iloc[-1]) else 0.0,
        "MACD_BAR": round(float(df["macd_bar"].iloc[-1]), 2) if "macd_bar" in df.columns and pd.notna(df["macd_bar"].iloc[-1]) else 0.0,
        "乖離率5日": round(bias5, 2),
        "成交量": int(vol), "昨量": int(vol_prev), "量比5日": vol_ratio, "量能變化": vol_trend, "量能變化%": vol_change_pct,
        "價量結論": pv_label, "價量燈號": pv_light, "價量理由": "｜".join(pv_reasons),
        **liquidity_profile,
        **long_dt,
        **short_dt,
        "選股理由": reason,
        "摘要1": f"位置評語：現價距短撐約 {dist_support}% ，距短壓約 {dist_resistance}% 。",
        "摘要2": f"策略評語：短線結論偏{conclusion}，交易訊號為 {signal} ，建議進場 {entry:.2f} ，風報比 {rr}。",
        "摘要3": f"量價評語：{vol_trend} {vol_change_pct:+.2f}% 。",
        "_code": resolved_code, "_rank": rank,
    }


def build_candidate_pool_250(name_map: dict, resolve_symbol, indicators, watchlist_default, max_price: float = 1100.0, target_count: int = 250):
    ordered_codes = []
    seen = set()

    for code in watchlist_default:
        c = str(code).strip()
        if c and c not in seen:
            ordered_codes.append(c)
            seen.add(c)

    for full_code in name_map.keys():
        full_code = str(full_code).upper().strip()
        if not full_code:
            continue
        base_code = full_code.split(".")[0]
        if re.fullmatch(r"\d{4}", base_code) and base_code not in seen:
            ordered_codes.append(base_code)
            seen.add(base_code)

    pool = []
    for code in ordered_codes:
        resolved_code, raw_df = resolve_symbol(code)
        if raw_df is None or raw_df.empty or len(raw_df) < 20:
            continue
        try:
            df = indicators(raw_df)
        except Exception:
            continue
        if df is None or df.empty:
            continue
        try:
            close = float(df["Close"].iloc[-1])
        except Exception:
            continue
        if close > max_price:
            continue
        pool.append(code)
        if len(pool) >= target_count:
            break

    return pool
