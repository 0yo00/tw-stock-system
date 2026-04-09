
import json
import re
import html as htmllib
from io import StringIO
from pathlib import Path
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
import ssl
from urllib.request import Request, urlopen
import requests
import urllib3
from urllib.error import URLError, HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
import threading
import warnings
import logging

import pandas as pd
import streamlit as st
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import candidate_engine as ce

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message=".*use_container_width.*")
warnings.filterwarnings("ignore", category=FutureWarning, message=r".*Downcasting object dtype arrays on \.fillna.*")
try:
    pd.set_option("future.no_silent_downcasting", True)
except Exception:
    pass
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

# yfinance 非執行緒安全：並行自動挑股時若同時 download 多檔，曾出現多檔 OHLC 錯位／相同收盤。
_YFIN_DOWNLOAD_LOCK = threading.Lock()

APP_VERSION = "V189"
APP_VERSION_LOWER = APP_VERSION.lower()
APP_VERSION_NOTE = "單股綜合評分：風險扣分改為實際觸發說明；判斷理由明確標示價漲量縮為黃燈觀察。"

# yfinance 單次下載逾時（秒）；逾時會拋錯以避免 Streamlit 快取空結果，並讓 resolve 可改試 .TWO
_YF_DOWNLOAD_TIMEOUT_SEC = 36.0
# 大盤 ^TWII 日線：加權採「靜態收盤基準」後不需頻繁重抓，適度拉長 TTL 減負擔
_MARKET_DATA_CACHE_TTL_SEC = 600

st.set_page_config(layout="wide", page_title=f"台股短線系統 {APP_VERSION_LOWER}")
st.markdown(f"""
<div class="app-sticky-header">
  <div class="app-sticky-inner">
    <div class="app-sticky-left">
      <div class="app-sticky-title">台股短線系統 <span>{APP_VERSION_LOWER}</span></div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

def inject_responsive_css():
    st.markdown("""
    <style>

    /* ═══════════════════════════════════════════════════════════════
       DESIGN SYSTEM — 台股短線系統 V189
       色調：深海藍底 + 電藍高亮 + 綠/紅語意色
    ═══════════════════════════════════════════════════════════════ */

    /* ── 全域底色 ── */
    .stApp { background: #080f1c; }

    /* ── 主內容區 padding ── */
    .block-container {
        padding-top: 4.4rem;
        padding-bottom: 2rem;
        max-width: 100% !important;
    }
    @media (min-width: 901px) {
        .block-container {
            padding-left: 1.2rem !important;
            padding-right: 1.2rem !important;
        }
    }
    @media (max-width: 900px) {
        .block-container {
            padding-left: 0.75rem;
            padding-right: 0.75rem;
        }
    }

    /* ══════════════════════════════════════════
       STICKY HEADER（頂部常駐列）
    ══════════════════════════════════════════ */
    .app-sticky-header {
        position: fixed;
        top: 0; left: 0; right: 0;
        z-index: 9999;
        background: rgba(8, 15, 28, 0.92);
        backdrop-filter: blur(16px);
        -webkit-backdrop-filter: blur(16px);
        border-bottom: 1px solid rgba(59, 110, 248, 0.18);
        padding: 0;
        height: 44px;
        display: flex;
        align-items: center;
    }
    .app-sticky-inner {
        display: flex;
        align-items: center;
        justify-content: space-between;
        width: 100%;
        padding: 0 1rem;
        gap: 12px;
    }
    .app-sticky-left {
        display: flex;
        align-items: center;
        gap: 10px;
        min-width: 0;
    }
    .app-sticky-title {
        font-size: 0.95rem;
        font-weight: 800;
        color: #e8f0ff;
        white-space: nowrap;
        letter-spacing: -0.01em;
    }
    .app-sticky-title span { color: #5b9aff; }
    .app-sticky-divider {
        width: 1px; height: 16px;
        background: rgba(100, 140, 220, 0.2);
        flex-shrink: 0;
    }
    .topbar-pill {
        display: inline-flex;
        align-items: center;
        gap: 5px;
        padding: 3px 9px;
        border-radius: 99px;
        font-size: 0.72rem;
        font-weight: 700;
        white-space: nowrap;
        border: 1px solid transparent;
    }
    .topbar-pill.bull {
        background: rgba(34, 197, 94, 0.10);
        border-color: rgba(34, 197, 94, 0.22);
        color: #4ade80;
    }
    .topbar-pill.bear {
        background: rgba(239, 68, 68, 0.10);
        border-color: rgba(239, 68, 68, 0.22);
        color: #f87171;
    }
    .topbar-pill.neu {
        background: rgba(100, 160, 255, 0.10);
        border-color: rgba(100, 160, 255, 0.22);
        color: #93c5fd;
    }
    .topbar-live-dot {
        width: 6px; height: 6px;
        border-radius: 50%;
        background: #4ade80;
        animation: blink 2.2s ease-in-out infinite;
        flex-shrink: 0;
    }
    @keyframes blink { 0%,100%{opacity:1} 50%{opacity:.3} }
    .topbar-idx {
        font-size: 0.75rem;
        color: #8ab0e0;
        white-space: nowrap;
    }
    .topbar-idx strong { color: #d0e8ff; font-weight: 700; }
    .topbar-idx .up  { color: #4ade80; }
    .topbar-idx .dn  { color: #f87171; }
    @media (max-width: 900px) {
        .topbar-pill { font-size: 0.68rem; padding: 2px 7px; }
        .topbar-idx  { font-size: 0.70rem; }
        .app-sticky-title { font-size: 0.85rem; }
    }

    /* ══════════════════════════════════════════
       SIDEBAR
    ══════════════════════════════════════════ */
    section[data-testid="stSidebar"] {
        background: #0a1220 !important;
        border-right: 1px solid rgba(59, 110, 248, 0.14) !important;
    }
    section[data-testid="stSidebar"] > div {
        padding-top: 0.8rem;
    }
    @media (min-width: 901px) {
        section[data-testid="stSidebar"] {
            width: 240px !important;
            min-width: 240px !important;
        }
        section[data-testid="stSidebar"] > div {
            width: 240px !important;
            min-width: 240px !important;
        }
        section[data-testid="stSidebar"] + div {
            margin-left: 0 !important;
        }
    }

    /* Sidebar logo card */
    .nav-card {
        background: rgba(59, 110, 248, 0.06);
        border: 1px solid rgba(59, 110, 248, 0.14);
        border-radius: 14px;
        padding: 0.85rem 1rem;
        margin-bottom: 0.75rem;
    }
    .nav-title {
        font-size: 0.72rem;
        letter-spacing: .08em;
        text-transform: uppercase;
        color: #3b6ef8;
        margin-bottom: 0.3rem;
        font-weight: 700;
    }

    /* st.radio 導覽選單美化 */
    div[data-testid="stRadio"] > div {
        gap: 4px !important;
        display: flex;
        flex-direction: column;
    }
    div[data-testid="stRadio"] label {
        display: flex;
        align-items: center;
        padding: 8px 12px !important;
        border-radius: 10px !important;
        border: 1px solid transparent !important;
        transition: background 0.13s, border-color 0.13s !important;
        cursor: pointer;
        margin: 0 !important;
    }
    div[data-testid="stRadio"] label:hover {
        background: rgba(59, 110, 248, 0.08) !important;
        border-color: rgba(59, 110, 248, 0.14) !important;
    }
    div[data-testid="stRadio"] label[data-baseweb="radio"]:has(input:checked) {
        background: rgba(59, 110, 248, 0.14) !important;
        border-color: rgba(59, 110, 248, 0.28) !important;
    }
    div[data-testid="stRadio"] label p {
        font-size: 1.10rem !important;
        font-weight: 600 !important;
        color: #8ab4f8 !important;
        margin: 0 !important;
    }
    /* 隱藏原生 radio 圓點（相容新舊版 Streamlit） */
    div[data-testid="stRadio"] [data-baseweb="radio"] div[class*="indicator"],
    div[data-testid="stRadio"] [data-baseweb="radio"] > div:first-child > div {
        display: none !important;
    }

    /* Sidebar header 標籤去掉多餘底線 */
    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 {
        font-size: 0.78rem !important;
        font-weight: 700 !important;
        color: #3a5580 !important;
        letter-spacing: .06em;
        text-transform: uppercase;
        margin: 0.9rem 0 0.3rem 0.1rem !important;
        border: none !important;
        padding: 0 !important;
    }

    /* ══════════════════════════════════════════
       METRIC CARDS（st.metric）
    ══════════════════════════════════════════ */
    div[data-testid="stMetric"] {
        background: rgba(255, 255, 255, 0.025);
        border: 1px solid rgba(100, 140, 220, 0.13);
        border-radius: 14px;
        padding: 12px 16px;
        transition: border-color 0.2s;
    }
    div[data-testid="stMetric"]:hover {
        border-color: rgba(100, 140, 220, 0.26);
    }
    div[data-testid="stMetricLabel"] p {
        font-size: 0.78rem !important;
        font-weight: 700 !important;
        color: #4a6890 !important;
        letter-spacing: .04em;
        text-transform: uppercase;
    }
    div[data-testid="stMetricValue"] {
        font-size: 1.75rem !important;
        font-weight: 800 !important;
        line-height: 1.15 !important;
        color: #d8ecff !important;
    }
    div[data-testid="stMetricDelta"] {
        font-size: 0.80rem !important;
        font-weight: 600 !important;
    }
    /* delta 顏色調整（Streamlit 預設太亮） */
    div[data-testid="stMetricDelta"] svg { display: none !important; }
    @media (max-width: 900px) {
        div[data-testid="stMetric"] { padding: 10px 12px; }
        div[data-testid="stMetricLabel"] p { font-size: 0.72rem !important; }
        div[data-testid="stMetricValue"] { font-size: 1.35rem !important; }
    }

    /* ══════════════════════════════════════════
       BUTTONS（st.button）
    ══════════════════════════════════════════ */
    div[data-testid="stButton"] > button {
        background: rgba(59, 110, 248, 0.10) !important;
        border: 1px solid rgba(59, 110, 248, 0.28) !important;
        border-radius: 10px !important;
        color: #7eb5ff !important;
        font-weight: 600 !important;
        font-size: 0.88rem !important;
        transition: background 0.15s, border-color 0.15s, transform 0.1s !important;
        padding: 0.45rem 1rem !important;
    }
    div[data-testid="stButton"] > button:hover {
        background: rgba(59, 110, 248, 0.20) !important;
        border-color: rgba(59, 110, 248, 0.50) !important;
        transform: translateY(-1px) !important;
    }
    div[data-testid="stButton"] > button:active {
        transform: translateY(0) !important;
    }
    /* Primary 按鈕（use_container_width）稍微深一點 */
    div[data-testid="stButton"] > button[kind="primary"] {
        background: rgba(59, 110, 248, 0.22) !important;
        border-color: rgba(59, 110, 248, 0.50) !important;
        color: #b0d0ff !important;
    }

    /* ══════════════════════════════════════════
       INPUT / SELECT / TOGGLE
    ══════════════════════════════════════════ */
    div[data-testid="stTextInput"] input,
    div[data-testid="stSelectbox"] > div > div,
    div[data-testid="stNumberInput"] input {
        background: rgba(255, 255, 255, 0.04) !important;
        border: 1px solid rgba(100, 140, 220, 0.18) !important;
        border-radius: 10px !important;
        color: #c8dcf8 !important;
        font-size: 0.92rem !important;
    }
    div[data-testid="stTextInput"] input:focus,
    div[data-testid="stNumberInput"] input:focus {
        border-color: rgba(59, 110, 248, 0.55) !important;
        box-shadow: 0 0 0 3px rgba(59, 110, 248, 0.10) !important;
    }
    div[data-testid="stToggle"] label span { font-size: 0.88rem !important; }

    /* ══════════════════════════════════════════
       TABS（st.tabs）
    ══════════════════════════════════════════ */
    .stTabs [data-baseweb="tab-list"] {
        background: transparent !important;
        border-bottom: 1px solid rgba(100, 140, 220, 0.14) !important;
        gap: 4px;
    }
    .stTabs [data-baseweb="tab-list"] button {
        background: transparent !important;
        border: none !important;
        border-radius: 8px 8px 0 0 !important;
        color: #4a6890 !important;
        font-size: 0.88rem !important;
        font-weight: 600 !important;
        padding: 0.55rem 1rem !important;
        transition: color 0.15s, background 0.15s !important;
    }
    .stTabs [data-baseweb="tab-list"] button:hover {
        color: #8ab4f8 !important;
        background: rgba(59, 110, 248, 0.07) !important;
    }
    .stTabs [aria-selected="true"] {
        color: #7eb5ff !important;
        border-bottom: 2px solid #3b6ef8 !important;
        background: rgba(59, 110, 248, 0.08) !important;
    }
    @media (max-width: 900px) {
        .stTabs [data-baseweb="tab-list"] button {
            padding: 0.45rem 0.65rem !important;
            font-size: 0.80rem !important;
        }
    }

    /* ══════════════════════════════════════════
       DATAFRAME / TABLE
    ══════════════════════════════════════════ */
    .stDataFrame, div[data-testid="stDataFrame"] {
        border-radius: 14px;
        overflow: hidden;
        border: 1px solid rgba(100, 140, 220, 0.12) !important;
    }

    /* ══════════════════════════════════════════
       EXPANDER
    ══════════════════════════════════════════ */
    div[data-testid="stExpander"] {
        border: 1px solid rgba(100, 140, 220, 0.13) !important;
        border-radius: 12px !important;
        background: rgba(255, 255, 255, 0.018) !important;
        margin-bottom: 0.55rem;
    }
    div[data-testid="stExpander"] summary {
        font-weight: 600 !important;
        font-size: 0.90rem !important;
        color: #8ab4f8 !important;
        padding: 0.65rem 0.9rem !important;
    }
    div[data-testid="stExpander"] summary:hover {
        background: rgba(59, 110, 248, 0.06) !important;
        border-radius: 12px;
    }

    /* ══════════════════════════════════════════
       INFO / WARNING / SUCCESS / ERROR boxes
    ══════════════════════════════════════════ */
    div[data-testid="stInfo"] {
        background: rgba(59, 110, 248, 0.07) !important;
        border: 1px solid rgba(59, 110, 248, 0.20) !important;
        border-radius: 12px !important;
        font-size: 0.88rem !important;
    }
    div[data-testid="stWarning"] {
        background: rgba(251, 191, 36, 0.07) !important;
        border: 1px solid rgba(251, 191, 36, 0.22) !important;
        border-radius: 12px !important;
        font-size: 0.88rem !important;
    }
    div[data-testid="stSuccess"] {
        background: rgba(34, 197, 94, 0.07) !important;
        border: 1px solid rgba(34, 197, 94, 0.22) !important;
        border-radius: 12px !important;
        font-size: 0.88rem !important;
    }
    div[data-testid="stError"] {
        background: rgba(239, 68, 68, 0.07) !important;
        border: 1px solid rgba(239, 68, 68, 0.22) !important;
        border-radius: 12px !important;
        font-size: 0.88rem !important;
    }

    /* ══════════════════════════════════════════
       HEADINGS
    ══════════════════════════════════════════ */
    h1 { font-size: 2.00rem !important; font-weight: 800 !important; color: #d8ecff !important; margin-top: 0.2rem !important; }
    h2 { font-size: 1.70rem !important; font-weight: 700 !important; color: #c0d8f8 !important; }
    h3 { font-size: 1.50rem !important; font-weight: 700 !important; color: #b0ccf0 !important; }
    h4 { font-size: 1.35rem !important; font-weight: 700 !important; color: #8ab4f8 !important; }

    /* 明確覆蓋 Streamlit markdown 容器內的標題（優先度高於 Streamlit 預設） */
    div[data-testid="stMarkdownContainer"] h2,
    div[data-testid="stMarkdownContainer"] h3 {
        font-size: 1.50rem !important;
        font-weight: 700 !important;
        color: #b0ccf0 !important;
        margin-top: 0.6rem !important;
        margin-bottom: 0.3rem !important;
    }
    div[data-testid="stMarkdownContainer"] h4 {
        font-size: 1.35rem !important;
        font-weight: 700 !important;
        color: #8ab4f8 !important;
        margin-top: 0.5rem !important;
        margin-bottom: 0.25rem !important;
    }

    @media (max-width: 900px) {
        h1 { font-size: 1.60rem !important; }
        h2, h3,
        div[data-testid="stMarkdownContainer"] h2,
        div[data-testid="stMarkdownContainer"] h3 { font-size: 1.35rem !important; }
        h4,
        div[data-testid="stMarkdownContainer"] h4 { font-size: 1.20rem !important; }
    }

    /* ══════════════════════════════════════════
       CUSTOM COMPONENT CARDS（既有 classes 保留升級）
    ══════════════════════════════════════════ */
    .main-shell {
        border: 1px solid rgba(59, 110, 248, 0.16);
        background: rgba(10, 20, 45, 0.72);
        border-radius: 16px;
        padding: 0.85rem 1rem 0.6rem;
        margin-bottom: 0.9rem;
    }
    .main-shell h3 { margin-bottom: 0.15rem; }
    .main-shell p { color: #8090a8; margin-bottom: 0; font-size: 0.88rem; }

    .nav-card {
        border: 1px solid rgba(59, 110, 248, 0.14);
        background: rgba(59, 110, 248, 0.055);
        border-radius: 14px;
        padding: 0.85rem 1rem;
        margin-bottom: 0.75rem;
    }
    .nav-title {
        font-size: 0.70rem;
        letter-spacing: .09em;
        text-transform: uppercase;
        color: #3b6ef8;
        margin-bottom: 0.3rem;
        font-weight: 700;
    }

    /* 比較矩陣 */
    .compare-matrix {
        width: 100%;
        border-collapse: collapse;
        margin: 0.35rem 0 0.9rem 0;
        font-size: 0.88rem;
        overflow: hidden;
        border-radius: 12px;
        border: 1px solid rgba(100, 140, 220, 0.16);
    }
    .compare-matrix th, .compare-matrix td {
        padding: 0.55rem 0.7rem;
        border-bottom: 1px solid rgba(100, 140, 220, 0.10);
        text-align: left;
        vertical-align: top;
    }
    .compare-matrix th {
        background: rgba(59, 110, 248, 0.06);
        font-weight: 700;
        font-size: 0.80rem;
        text-transform: uppercase;
        letter-spacing: .05em;
        color: #4a6890;
    }
    .compare-matrix tr:last-child td { border-bottom: none; }

    /* chips */
    .compare-chip-wrap { display: flex; gap: 0.4rem; flex-wrap: wrap; margin: 0.2rem 0 0.8rem; }
    .compare-chip {
        border: 1px solid rgba(100, 140, 220, 0.18);
        border-radius: 99px;
        padding: 0.28rem 0.65rem;
        font-size: 0.78rem;
        background: rgba(59, 110, 248, 0.06);
        color: #8ab4f8;
    }

    /* rank chip */
    .rank-chip {
        display: inline-block;
        padding: 0.18rem 0.42rem;
        border-radius: 99px;
        font-size: 0.72rem;
        margin-right: 0.3rem;
        background: rgba(59, 110, 248, 0.10);
        border: 1px solid rgba(59, 110, 248, 0.20);
        color: #93c5fd;
    }
    .status-chip {
        display: inline-block;
        padding: 0.16rem 0.48rem;
        border-radius: 99px;
        font-size: 0.72rem;
        margin-right: 0.3rem;
        margin-top: 0.22rem;
        background: rgba(59, 110, 248, 0.10);
        border: 1px solid rgba(59, 110, 248, 0.20);
        color: #93c5fd;
    }
    .compact-note { font-size: 0.82rem; color: #8090a8; }
    .compact-toolbar-note { font-size: 0.78rem; color: #6080a0; margin-top: 0.3rem; }

    /* soft-status */
    .soft-status {
        border: 1px solid rgba(100, 140, 220, 0.16);
        background: rgba(255,255,255,0.02);
        border-radius: 14px;
        padding: 0.8rem 0.95rem;
        margin: 0.4rem 0 0.85rem;
    }
    .soft-status-title { font-size: 1.20rem; font-weight: 800; color: #d8ecff; margin-bottom: 0.3rem; }
    .soft-status-note  { font-size: 0.82rem; color: #8090a8; line-height: 1.65; }

    /* section divider */
    .section-divider-card {
        border: 1px solid rgba(100, 140, 220, 0.14);
        background: rgba(5, 10, 25, 0.55);
        border-radius: 14px;
        padding: 0.85rem 1rem;
        margin: 0.3rem 0 0.85rem;
    }
    .section-divider-title { font-size: 1.20rem; font-weight: 800; color: #d8ecff; margin-bottom: 0.25rem; }
    .section-divider-note  { font-size: 0.82rem; color: #8090a8; line-height: 1.7; }

    /* dashboard card */
    .dashboard-card {
        border: 1px solid rgba(100, 140, 220, 0.14);
        background: rgba(255,255,255,0.022);
        border-radius: 14px;
        padding: 0.8rem 0.9rem;
        min-height: 120px;
    }
    .dashboard-card h4 { margin: 0 0 0.25rem; font-size: 0.95rem; }
    .dashboard-sub { color: #5a7898; font-size: 0.78rem; margin-bottom: 0.45rem; }

    /* mini-batch */
    .mini-batch-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 0.7rem;
        margin: 0.2rem 0 0.55rem;
    }
    .mini-batch-card {
        border: 1px solid rgba(100, 140, 220, 0.14);
        background: rgba(10, 20, 45, 0.65);
        border-radius: 14px;
        padding: 0.8rem 0.9rem;
        min-height: 88px;
    }
    .mini-batch-label { color: #5b9aff; font-size: 0.74rem; margin-bottom: 0.3rem; letter-spacing:.03em; font-weight:700; }
    .mini-batch-value { color: #d8ecff; font-size: 1.12rem; font-weight: 800; line-height: 1.25; word-break: break-word; }
    .mini-batch-sub   { color: #8090a8; font-size: 0.78rem; margin-top: 0.3rem; line-height: 1.5; }

    /* diff-top */
    .diff-top-grid {
        display: grid;
        grid-template-columns: repeat(3, minmax(0, 1fr));
        gap: 0.7rem;
        margin: 0.3rem 0 0.8rem;
    }
    .diff-top-card {
        border: 1px solid rgba(100, 140, 220, 0.14);
        background: rgba(10, 20, 45, 0.68);
        border-radius: 14px;
        padding: 0.8rem 0.9rem;
        min-height: 112px;
    }
    .diff-top-rank  { color: #5b9aff; font-size: 0.73rem; font-weight: 700; margin-bottom: 0.3rem; }
    .diff-top-title { color: #d8ecff; font-size: 0.96rem; font-weight: 800; line-height: 1.3; }
    .diff-top-main  { color: #93c5fd; font-size: 1.28rem; font-weight: 800; margin-top: 0.28rem; }
    .diff-top-sub   { color: #8090a8; font-size: 0.78rem; margin-top: 0.3rem; line-height: 1.5; }
    @media (max-width: 900px) {
        .mini-batch-grid, .diff-top-grid { grid-template-columns: 1fr; }
    }

    /* ══════════════════════════════════════════
       LANDING PAGE
    ══════════════════════════════════════════ */
    .lp-hero {
        text-align: center;
        padding: 3.5rem 1.2rem 2rem;
        background: radial-gradient(ellipse at 50% 0%, rgba(59,110,248,0.16) 0%, rgba(120,80,240,0.07) 45%, transparent 70%);
        border-radius: 24px;
        margin-bottom: 1rem;
        position: relative;
        overflow: hidden;
    }
    .lp-hero::before {
        content: "";
        position: absolute;
        inset: 0;
        border-radius: 24px;
        padding: 1px;
        background: linear-gradient(135deg, rgba(59,110,248,0.22), rgba(120,80,240,0.12), transparent 55%);
        -webkit-mask: linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
        mask: linear-gradient(#fff 0 0) content-box, linear-gradient(#fff 0 0);
        -webkit-mask-composite: xor; mask-composite: exclude;
        pointer-events: none;
    }
    .lp-hero h1 {
        font-size: 2.2rem !important;
        font-weight: 900;
        background: linear-gradient(135deg, #5b9aff 0%, #a78bfa 45%, #f472b6 80%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.45rem;
        line-height: 1.15;
        position: relative; z-index: 1;
    }
    .lp-hero .lp-sub { font-size: 1rem; color: #8090a8; margin-bottom: 0.55rem; font-weight: 500; position: relative; z-index: 1; }
    .lp-hero .lp-tags { display: flex; flex-wrap: wrap; justify-content: center; gap: 0.35rem; margin-bottom: 1.5rem; position: relative; z-index: 1; }
    .lp-tag {
        background: rgba(59,110,248,0.08);
        border: 1px solid rgba(59,110,248,0.18);
        border-radius: 99px;
        padding: 0.24rem 0.7rem;
        font-size: 0.72rem;
        color: #6090c8;
        letter-spacing: .02em;
    }
    .lp-section { margin-bottom: 2.2rem; }
    .lp-section-title { font-size: 1.40rem; font-weight: 800; color: #c0d8f8; margin-bottom: 0.85rem; padding-left: 0.1rem; }
    .lp-card {
        border: 1px solid rgba(100,140,220,0.12);
        background: rgba(255,255,255,0.03);
        border-radius: 18px;
        padding: 1.3rem 1.2rem;
        margin-bottom: 0.65rem;
        transition: border-color 0.2s, transform 0.2s;
        position: relative; overflow: hidden;
    }
    .lp-card::after {
        content: ""; position: absolute;
        top: 0; left: 0; width: 3px; height: 100%;
        border-radius: 18px 0 0 18px;
    }
    .lp-card:nth-child(1)::after { background: linear-gradient(180deg,#3b6ef8,#5b9aff); }
    .lp-card:nth-child(2)::after { background: linear-gradient(180deg,#7c3aed,#a78bfa); }
    .lp-card:nth-child(3)::after { background: linear-gradient(180deg,#f59e0b,#fbbf24); }
    .lp-card:nth-child(4)::after { background: linear-gradient(180deg,#10b981,#34d399); }
    .lp-card:hover { border-color: rgba(59,110,248,0.30); transform: translateY(-1px); }
    .lp-card { cursor: default; }
    .lp-card-icon { font-size: 1.8rem; margin-bottom: 0.45rem; }
    .lp-card-title { font-size: 1.20rem; font-weight: 700; color: #c8dcf8; margin-bottom: 0.28rem; }
    .lp-card-desc  { font-size: 0.92rem; color: #5a7898; line-height: 1.55; }
    .lp-card-arrow { position: absolute; right: 1rem; top: 50%; transform: translateY(-50%); font-size: 1rem; color: #2a4060; transition: all 0.18s; }
    .lp-card:hover .lp-card-arrow { color: #5b9aff; transform: translateY(-50%) translateX(3px); }

    .lp-news-item {
        border: 1px solid rgba(100,140,220,0.10);
        background: rgba(255,255,255,0.02);
        border-radius: 12px;
        padding: 0.85rem 0.95rem;
        margin-bottom: 0.5rem;
        transition: border-color 0.18s;
    }
    .lp-news-item:hover { border-color: rgba(59,110,248,0.26); }
    .lp-news-meta { display: flex; align-items: center; gap: 0.45rem; margin-bottom: 0.3rem; flex-wrap: wrap; }
    .lp-news-time { font-size: 0.70rem; color: #3a5570; }
    .lp-news-badge { font-size: 0.66rem; padding: 0.13rem 0.42rem; border-radius: 99px; font-weight: 700; }
    .lp-news-badge.bull   { background: rgba(34,197,94,0.12);  color: #4ade80; }
    .lp-news-badge.bear   { background: rgba(239,68,68,0.12);   color: #f87171; }
    .lp-news-badge.neutral{ background: rgba(251,191,36,0.10);  color: #fbbf24; }
    .lp-news-title  { font-size: 0.86rem; color: #c0d8f8; font-weight: 500; line-height: 1.45; }
    .lp-news-source { font-size: 0.68rem; color: #3a5570; margin-top: 0.18rem; }

    .lp-state-card { border: 1px solid rgba(100,140,220,0.10); border-radius: 16px; padding: 1.5rem 1.2rem; text-align: center; }
    .lp-state-card.empty { background: rgba(255,255,255,0.018); }
    .lp-state-card.error { background: rgba(239,68,68,0.04); border-color: rgba(239,68,68,0.14); }
    .lp-state-icon  { font-size: 1.6rem; margin-bottom: 0.5rem; }
    .lp-state-title { font-size: 0.96rem; font-weight: 700; color: #c0d8f8; margin-bottom: 0.3rem; }
    .lp-state-desc  { font-size: 0.80rem; color: #5a7898; line-height: 1.5; }

    .lp-steps-compact { display: grid; grid-template-columns: 1fr 1fr; gap: 0.55rem; }
    .lp-step-compact {
        border: 1px solid rgba(100,140,220,0.09);
        border-radius: 12px;
        padding: 0.7rem 0.8rem;
        background: rgba(255,255,255,0.014);
    }
    .lp-step-compact-num {
        display: inline-block; width: 20px; height: 20px;
        border-radius: 50%;
        background: linear-gradient(135deg,#3b6ef8,#7c3aed);
        text-align: center; line-height: 20px;
        font-size: 0.68rem; font-weight: 800; color: white;
        margin-right: 0.35rem;
    }
    .lp-step-compact-title { font-size: 1.05rem; font-weight: 600; color: #c0d8f8; display: inline; }
    .lp-step-compact-desc  { font-size: 0.73rem; color: #5a7898; margin-top: 0.22rem; line-height: 1.4; }

    .lp-strategy-panel {
        border: 1px solid rgba(100,140,220,0.13);
        background: rgba(8,18,40,0.85);
        border-radius: 20px;
        padding: 1.3rem 1.1rem 1.1rem;
    }
    .lp-strategy-header { font-size: 0.68rem; color: #3a5570; text-transform: uppercase; letter-spacing:.12em; margin-bottom:0.18rem; font-weight:700; }
    .lp-strategy-row {
        display: flex; justify-content: space-between; align-items: center;
        padding: 0.75rem 0.5rem;
        border-bottom: 1px solid rgba(100,140,220,0.07);
        border-radius: 9px; margin: 0 -0.25rem;
        transition: background 0.13s;
    }
    .lp-strategy-row:hover { background: rgba(255,255,255,0.018); }
    .lp-strategy-label { font-size: 0.88rem; color: #8090a8; font-weight: 600; display: flex; align-items: center; gap: 0.45rem; }
    .lp-strategy-label .lp-dot { width: 5px; height: 5px; border-radius: 50%; background: #2a4060; flex-shrink: 0; }
    .lp-strategy-value { font-size: 1.05rem; font-weight: 700; color: #d8ecff; }

    .lp-score-cards { display: grid; grid-template-columns: 1fr 1fr; gap: 0.65rem; margin-top: 0.55rem; }
    .lp-score-card {
        border: 1px solid rgba(100,140,220,0.10);
        border-radius: 14px;
        padding: 1rem 0.85rem 0.85rem;
        text-align: center; position: relative; overflow: hidden;
    }
    .lp-score-card::before { content:""; position:absolute; top:0; left:0; right:0; height:2px; }
    .lp-score-card.long  { background: rgba(34,197,94,0.05); }
    .lp-score-card.long::before  { background: linear-gradient(90deg,#22c55e,#4ade80); }
    .lp-score-card.short { background: rgba(239,68,68,0.05); }
    .lp-score-card.short::before { background: linear-gradient(90deg,#ef4444,#f87171); }
    .lp-score-card-label { font-size: 0.72rem; color: #5a7898; font-weight: 700; letter-spacing:.03em; margin-bottom:0.35rem; }
    .lp-score-card-value { font-size: 1.85rem; font-weight: 800; line-height:1.1; margin-bottom:0.45rem; }
    .lp-score-card.long .lp-score-card-value  { color: #4ade80; }
    .lp-score-card.short .lp-score-card-value { color: #f87171; }
    .lp-score-mini-bar  { height: 4px; background: rgba(255,255,255,0.05); border-radius: 2px; overflow: hidden; }
    .lp-score-mini-fill { height: 100%; border-radius: 2px; transition: width 0.6s ease; }
    .lp-score-card.long .lp-score-mini-fill  { background: linear-gradient(90deg,#22c55e,#4ade80); }
    .lp-score-card.short .lp-score-mini-fill { background: linear-gradient(90deg,#ef4444,#f87171); }

    .lp-insight {
        margin-top: 0.85rem; padding: 0.8rem 0.95rem;
        background: rgba(59,110,248,0.06);
        border-left: 2px solid #3b6ef8;
        border-radius: 0 12px 12px 0;
        font-size: 0.82rem; color: #8090a8; line-height: 1.55;
    }
    .lp-stat-card {
        border: 1px solid rgba(100,140,220,0.12);
        background: rgba(255,255,255,0.025);
        border-radius: 16px; padding: 1.2rem 0.95rem; text-align: center;
    }
    .lp-stat-value { font-size: 1.6rem; font-weight: 800; color: #d8ecff; }
    .lp-stat-label { font-size: 0.72rem; color: #3a5570; margin-top: 0.22rem; letter-spacing:.04em; }
    .lp-step { display: flex; align-items: flex-start; gap: 0.9rem; margin-bottom: 1.2rem; }
    .lp-step-num {
        width: 34px; height: 34px; border-radius: 50%;
        background: linear-gradient(135deg,#3b6ef8,#7c3aed);
        display: flex; align-items: center; justify-content: center;
        font-weight: 800; font-size: 0.88rem; color: white; flex-shrink: 0;
    }
    .lp-step-text h4 { margin: 0.08rem 0 0.2rem; font-size: 0.94rem; color: #c0d8f8; }
    .lp-step-text p  { margin: 0; font-size: 0.80rem; color: #5a7898; line-height: 1.5; }
    .lp-divider { height: 1px; background: linear-gradient(90deg,transparent,rgba(100,140,220,0.16),transparent); margin: 2rem 0; }
    .lp-footer { text-align: center; padding: 1.3rem 0 2.2rem; color: #2a4060; font-size: 0.75rem; }
    @media (max-width: 900px) {
        .lp-hero { padding: 2.5rem 0.75rem 1.6rem; }
        .lp-hero h1 { font-size: 1.70rem !important; }
        .lp-strategy-panel { padding: 1.1rem 0.9rem; }
    }

    </style>
    """, unsafe_allow_html=True)


NAMES_FILE = Path("tw_stock_names.json")
MAX_SNAPSHOTS = 5000
USERS_FILE = Path("users_list.json")
DEFAULT_USERS = ["承佑", "測試"]

def load_users():
    users = load_json_list(USERS_FILE)
    if not users:
        return DEFAULT_USERS.copy()
    clean = [str(x).strip() for x in users if str(x).strip()]
    return clean if clean else DEFAULT_USERS.copy()

def save_users(users):
    clean = []
    seen = set()
    for u in users:
        name = str(u).strip()
        if name and name not in seen:
            clean.append(name)
            seen.add(name)
    save_json_list(USERS_FILE, clean)

def safe_user_key(user_name: str) -> str:
    text = str(user_name).strip()
    if not text:
        text = "default"
    return re.sub(r"[^0-9A-Za-z_\-一-龥]+", "_", text)

def user_file(prefix: str, user_name: str) -> Path:
    return Path(f"{prefix}_{safe_user_key(user_name)}.json")

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
watchlist_default = ['2330', '2317', '2454', '2303', '2308', '2382', '3231', '6669', '2356', '2376', '2324', '2357', '2377', '3017', '3034', '3661', '3443', '3035', '4966', '6533', '2383', '6274', '2368', '3037', '3044', '8046', '3189', '6147', '6269', '3596', '2345', '2344', '2408', '2337', '2401', '2371', '3702', '3013', '8210', '3324', '3653', '4938', '4979', '3363', '3406', '3008', '3583', '3483', '3025', '6695', '6146', '3592', '4976', '6805', '3665', '4977', '6223', '3413', '2404', '6196', '2451', '2374', '3533', '2388', '3030', '6191', '6670', '6510', '6781', '6789', '1504', '1519', '1503', '1513', '1514', '1536', '1590', '2049', '2201', '2204', '2231', '2250', '3019', '4526', '4532', '4551', '4552', '6605', '9958', '1301', '1303', '1304', '1305', '1308', '1326', '1310', '1325', '6505', '6509', '1717', '1722', '1710', '4743', '1905', '1802', '2603', '2609', '2615', '2634', '2618', '2606', '2610', '2612', '5608', '2637', '2613', '2027', '2002', '2006', '2014', '2031', '2069', '9955', '8436', '1909', '3006', '8054', '6187', '6285', '6104', '3036', '6116', '4906', '6282', '6415', '6446', '6176', '3234', '8086', '5243', '1560', '3010', '1515', '1597', '6414', '6125', '5536', '1568', '6416', '6535', '5483', '1815', '1463', '6275', '1810', '6278', '8299', '8261', '3645', '2495', '2385', '2327', '2449', '2455', '2441', '3706', '4931', '2329', '2379', '2890', '3715', '4974', '3593', '5371', '5347', '6271', '4919', '2457', '2421', '8069', '8016', '5258', '6190', '2498', '6538', '6443', '6472']

LIQ_MIN_AVG_VOL20_LOTS = 15000.0
LIQ_MIN_AVG_VALUE20_EOK = 5.0
LIQ_MIN_AVG_AMP20_PCT = 3.0
LIQ_LOW_DAY5_LOTS = 8000.0
LIQ_MAX_LOW_DAYS5 = 2
LIQ_MIN_STABLE_RATIO = 0.60
LIQ_BURST_MEDIAN20_LIMIT = 8000.0
LIQ_BURST_RATIO_LIMIT = 4.0




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

def current_favorites_file():
    return user_file("stock_favorites", st.session_state.get("current_user", DEFAULT_USERS[0]))

def current_snapshots_file():
    return user_file("stock_snapshots", st.session_state.get("current_user", DEFAULT_USERS[0]))

def current_trades_file():
    return user_file("trades_v13", st.session_state.get("current_user", DEFAULT_USERS[0]))

def current_compare_history_file():
    return user_file("batch_compare_history", st.session_state.get("current_user", DEFAULT_USERS[0]))


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


def _ordered_candidate_codes(name_map: dict, seed_list=None):
    ordered_codes = []
    seen = set()
    for code in (seed_list or watchlist_default):
        c = str(code).strip().upper()
        if c and c not in seen:
            ordered_codes.append(c)
            seen.add(c)
    for full_code in sorted(name_map.keys()):
        base_code = str(full_code).upper().strip().split(".")[0]
        if re.fullmatch(r"\d{4}", base_code) and base_code not in seen:
            ordered_codes.append(base_code)
            seen.add(base_code)
    return ordered_codes


@st.cache_data(ttl=1800, show_spinner=False)
def _filter_pool_with_official_snapshot(code_list, max_price: float = 1100.0, target_count: int | None = None):
    ordered = [str(x).strip().upper() for x in (code_list or []) if str(x).strip()]
    if not ordered:
        return []
    try:
        latest_snapshot_df = load_latest_snapshot()
    except Exception:
        latest_snapshot_df = pd.DataFrame()
    snap_map = {}
    if latest_snapshot_df is not None and not latest_snapshot_df.empty and "_merge_code" in latest_snapshot_df.columns:
        snap_map = latest_snapshot_df.set_index("_merge_code", drop=False).to_dict("index")

    pool, missing, over_price = [], [], []
    for code in ordered:
        merge_code = _normalize_merge_code(code)
        row = snap_map.get(merge_code)
        if row is None:
            missing.append(code)
            continue
        close = _to_number_or_none(row.get("close"))
        if close is None:
            missing.append(code)
            continue
        if close > max_price:
            over_price.append(code)
            continue
        pool.append(code)
        if target_count and len(pool) >= target_count:
            return pool[:target_count]

    for code in missing:
        if code not in over_price and code not in pool:
            pool.append(code)
            if target_count and len(pool) >= target_count:
                break
    return pool[:target_count] if target_count else pool


@st.cache_data(ttl=900, show_spinner=False)
def build_candidate_pool_250(name_map: dict, max_price: float = 1100.0, target_count: int = 250, seed_list=None):
    ordered_codes = _ordered_candidate_codes(name_map, seed_list=seed_list)
    uniq = []
    seen = set()
    for code in ordered_codes:
        c = str(code).strip().upper()
        if not c or c in seen:
            continue
        uniq.append(c)
        seen.add(c)
        if target_count and len(uniq) >= target_count:
            break
    return uniq[:target_count] if target_count else uniq


@st.cache_data(ttl=900, show_spinner=False)
def build_candidate_core_pool(name_map: dict, max_price: float = 1100.0):
    ordered_codes = _ordered_candidate_codes(name_map, seed_list=watchlist_default)
    uniq = []
    seen = set()
    for code in ordered_codes[:len(watchlist_default)]:
        c = str(code).strip().upper()
        if not c or c in seen:
            continue
        uniq.append(c)
        seen.add(c)
    return uniq


@st.cache_data(ttl=900, show_spinner=False)
def build_candidate_pool_layers(name_map: dict, max_price: float = 1100.0, expanded_target: int = 250):
    core_pool = build_candidate_core_pool(name_map, max_price=max_price)
    full_pool = build_candidate_pool_250(name_map, max_price=max_price, target_count=expanded_target, seed_list=watchlist_default)
    core_set = {str(x).strip().upper() for x in core_pool}
    expansion_pool = [code for code in full_pool if str(code).strip().upper() not in core_set]
    return {
        "core_pool": core_pool,
        "expansion_pool": expansion_pool,
        "full_pool": full_pool,
        "core_count": len(core_pool),
        "expansion_count": len(expansion_pool),
        "full_count": len(full_pool),
    }


def load_favorites():
    return [str(x) for x in load_json_list(current_favorites_file())]

def save_favorites(favorites):
    save_json_list(current_favorites_file(), sorted(list(set(favorites))))

def load_snapshots():
    return load_json_list(current_snapshots_file())[-MAX_SNAPSHOTS:]

def save_snapshots(data):
    save_json_list(current_snapshots_file(), data[-MAX_SNAPSHOTS:])

def load_trades():
    return load_json_list(current_trades_file())

def save_trades(data):
    save_json_list(current_trades_file(), data)

def load_compare_history():
    return load_json_list(current_compare_history_file())

def save_compare_history(data):
    save_json_list(current_compare_history_file(), data)

def _jsonable(value):
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value

def save_compare_result_history(batch_id: str, batch_label: str, mode: str, compare_df: pd.DataFrame):
    records = [x for x in load_compare_history() if str(x.get("批次ID", "")) != str(batch_id)]
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if compare_df is not None and not compare_df.empty:
        for row in compare_df.to_dict(orient="records"):
            rec = {str(k): _jsonable(v) for k, v in row.items()}
            rec["批次ID"] = str(batch_id)
            rec["批次標籤"] = str(batch_label)
            rec["快照邏輯"] = rec.get("快照邏輯") or str(mode or "")
            rec["對照建立時間"] = created_at
            records.append(rec)
    save_compare_history(records)

def load_compare_result_history_df(batch_id: str) -> pd.DataFrame:
    rows = [x for x in load_compare_history() if str(x.get("批次ID", "")) == str(batch_id)]
    return pd.DataFrame(rows) if rows else pd.DataFrame()

def clear_compare_result_history(batch_id: str = ""):
    if not batch_id:
        save_compare_history([])
        return
    rows = [x for x in load_compare_history() if str(x.get("批次ID", "")) != str(batch_id)]
    save_compare_history(rows)


def display_name(code: str, name_map: dict):
    code = code.upper()
    name = name_map.get(code, "")
    return f"{code}（{name}）" if name else code


def html_escape(value):
    # 直接使用已 import 的標準庫 html.escape，行為完全一致且更完整
    return htmllib.escape(str(value), quote=True)


def render_compare_matrix(detail_row):
    compare_rows = [
        ("收盤", fmt_price(detail_row.get("盤前收盤", "")), fmt_price(detail_row.get("盤後收盤", ""))),
        ("支撐/最低", fmt_price(detail_row.get("盤前支撐", "")), fmt_price(detail_row.get("盤後最低", ""))),
        ("壓力/最高", fmt_price(detail_row.get("盤前壓力", "")), fmt_price(detail_row.get("盤後最高", ""))),
        ("建議進場", fmt_price(detail_row.get("盤前建議進場", "")), fmt_price(detail_row.get("模擬進場價", detail_row.get("盤前建議進場", "")))),
        ("停損", fmt_price(detail_row.get("盤前停損", "")), fmt_text(detail_row.get("停損觸發", ""))),
        ("風報比", fmt_price(detail_row.get("盤前風報比", "")), fmt_price(detail_row.get("盤後風報比", ""))),
        ("價量結論", "盤前未記錄", fmt_text(detail_row.get("價量結論", ""))),
        ("價格變化", "盤前基準", fmt_text(detail_row.get("價格變化", ""))),
        ("結構變化", "盤前基準", fmt_text(detail_row.get("結構變化", detail_row.get("變化判斷", "")))),
        ("結論", fmt_text(detail_row.get("盤前結論", "")), fmt_text(detail_row.get("盤後結論", ""))),
        ("訊號", fmt_text(detail_row.get("盤前訊號", "")), fmt_text(detail_row.get("盤後訊號", ""))),
    ]
    table_rows = "".join(
        f"<tr><td>{html_escape(label)}</td><td>{html_escape(pre)}</td><td>{html_escape(post)}</td></tr>"
        for label, pre, post in compare_rows
    )
    st.markdown(
        f"""
    <table class="compare-matrix">
      <thead>
        <tr><th>項目</th><th>盤前</th><th>盤後</th></tr>
      </thead>
      <tbody>{table_rows}</tbody>
    </table>
    """,
        unsafe_allow_html=True,
    )


def render_sim_matrix(entry_time, close_price, close_result, close_pnl, close_ret, high_price, high_result, high_pnl, high_ret, low_price, low_result, low_pnl, low_ret):
    sim_rows = [
        ("進場時間", fmt_text(entry_time), fmt_text("成交後起算" if fmt_text(entry_time) not in ["", "--", "資料不足", "未成交"] else "")),
        ("模擬收盤價", fmt_price(close_price), fmt_text(close_result)),
        ("收盤損益", fmt_price(close_pnl), fmt_pct(close_ret)),
        ("模擬最高價", fmt_price(high_price), fmt_text(high_result)),
        ("最高損益", fmt_price(high_pnl), fmt_pct(high_ret)),
        ("模擬最低價", fmt_price(low_price), fmt_text(low_result)),
        ("最低損益", fmt_price(low_pnl), fmt_pct(low_ret)),
    ]
    table_rows = "".join(
        f"<tr><td>{html_escape(label)}</td><td>{html_escape(v1)}</td><td>{html_escape(v2)}</td></tr>"
        for label, v1, v2 in sim_rows
    )
    st.markdown(
        f"""
    <table class="compare-matrix">
      <thead>
        <tr><th>模擬項目</th><th>數值</th><th>結果/報酬</th></tr>
      </thead>
      <tbody>{table_rows}</tbody>
    </table>
    """,
        unsafe_allow_html=True,
    )


def render_compare_chips(detail_row):
    chips = [
        f"支撐驗證：{fmt_text(detail_row.get('支撐驗證', ''))}",
        f"壓力驗證：{fmt_text(detail_row.get('壓力驗證', ''))}",
        f"停損觸發：{fmt_text(detail_row.get('停損觸發', ''))}",
        f"是否可成交：{fmt_text(detail_row.get('是否可成交', ''))}",
        f"變化判斷：{fmt_text(detail_row.get('變化判斷', ''))}",
    ]
    chip_html = "".join(f'<div class="compare-chip">{html_escape(c)}</div>' for c in chips)
    st.markdown(f'<div class="compare-chip-wrap">{chip_html}</div>', unsafe_allow_html=True)


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


def calc_macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    bar = (dif - dea) * 2
    return dif.fillna(0), dea.fillna(0), bar.fillna(0)


def indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    df = df.loc[:, ~pd.Index(df.columns).duplicated()].copy()

    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col not in df.columns:
            df[col] = pd.NA

    def _to_series(col_data):
        """MultiIndex yfinance 有時會回傳 DataFrame，取第一欄並轉 numeric。"""
        if isinstance(col_data, pd.DataFrame):
            col_data = col_data.iloc[:, 0]
        return pd.to_numeric(col_data, errors="coerce")

    df["Close"] = _to_series(df["Close"])
    df["Volume"] = _to_series(df["Volume"])
    df["High"] = _to_series(df["High"])
    df["Low"] = _to_series(df["Low"])

    df = df.dropna(subset=["Close", "High", "Low"])

    if df.empty:
        return pd.DataFrame()

    # dropna 之後直接從 df 取欄，不需再建中間變數
    df["ma5"] = df["Close"].rolling(5).mean()
    df["ma10"] = df["Close"].rolling(10).mean()
    df["ma20"] = df["Close"].rolling(20).mean()
    df["ma60"] = df["Close"].rolling(60).mean()
    df["ma120"] = df["Close"].rolling(120).mean()
    df["vol5"] = df["Volume"].rolling(5).mean()
    df["vol20"] = df["Volume"].rolling(20).mean()
    df["atr"] = (df["High"] - df["Low"]).rolling(14).mean()
    df["rsi"] = calc_rsi(df["Close"], 14)
    df["bias5"] = ((df["Close"] - df["ma5"]) / df["ma5"] * 100).fillna(0)
    df["k"], df["d"] = calc_kd(df, 9)
    df["j"] = (3 * df["k"] - 2 * df["d"]).fillna(50)
    df["macd_dif"], df["macd_dea"], df["macd_bar"] = calc_macd(df["Close"])

    return df
def _official_latest_row_for_symbol(symbol: str):
    try:
        latest_snapshot_df = load_latest_snapshot()
    except Exception:
        return None
    if latest_snapshot_df is None or latest_snapshot_df.empty or "_merge_code" not in latest_snapshot_df.columns:
        return None
    merge_code = _normalize_merge_code(symbol)
    if not merge_code:
        return None
    try:
        snap = latest_snapshot_df.set_index("_merge_code", drop=False)
    except Exception:
        return None
    if merge_code not in snap.index:
        return None
    row = snap.loc[merge_code]
    if isinstance(row, pd.DataFrame):
        row = row.iloc[-1]
    return row


def _official_latest_ohlc_map(symbol: str):
    row = _official_latest_row_for_symbol(symbol)
    if row is None:
        return {"hit": False, "date": "", "source": "", "close": "", "high": "", "low": "", "open": "", "volume": ""}
    date_str = _roc_to_ad(row.get("date", ""))
    return {
        "hit": True,
        "date": date_str,
        "source": str(row.get("source", "") or "official"),
        "open": _to_number_or_none(row.get("open")),
        "high": _to_number_or_none(row.get("high")),
        "low": _to_number_or_none(row.get("low")),
        "close": _to_number_or_none(row.get("close")),
        "volume": _to_number_or_none(row.get("volume")),
    }


def _apply_official_latest_bar(symbol: str, df: pd.DataFrame) -> pd.DataFrame:
    df = normalize_df(df)
    if df is None or df.empty:
        return df
    s = _official_latest_row_for_symbol(symbol)
    if s is None:
        return df
    official_date = pd.to_datetime(_roc_to_ad(s.get("date", "")), errors="coerce")
    if pd.isna(official_date):
        return df

    official_open = _to_number_or_none(s.get("open"))
    official_high = _to_number_or_none(s.get("high"))
    official_low = _to_number_or_none(s.get("low"))
    official_close = _to_number_or_none(s.get("close"))
    official_volume = _to_number_or_none(s.get("volume"))

    if official_close is None:
        return df

    base_open = official_open if official_open is not None else float(df["Open"].iloc[-1])
    base_high = official_high if official_high is not None else max(float(df["High"].iloc[-1]), official_close)
    base_low = official_low if official_low is not None else min(float(df["Low"].iloc[-1]), official_close)
    base_volume = official_volume if official_volume is not None else float(df["Volume"].iloc[-1])

    last_idx = pd.to_datetime(df.index[-1]).normalize()
    target_idx = official_date.normalize()
    new_row = {
        "Open": base_open,
        "High": max(base_high, official_close, base_open),
        "Low": min(base_low, official_close, base_open),
        "Close": official_close,
        "Volume": base_volume,
    }

    if target_idx > last_idx:
        df.loc[target_idx] = new_row
    elif target_idx == last_idx:
        df.loc[df.index[-1], ["Open", "High", "Low", "Close", "Volume"]] = [
            new_row["Open"], new_row["High"], new_row["Low"], new_row["Close"], new_row["Volume"]
        ]
    df = df.sort_index()
    return normalize_df(df)


def _patch_today_nan_row(symbol: str, df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    last_close = df["Close"].iloc[-1] if "Close" in df.columns else None
    try:
        if last_close is not None and pd.notna(last_close):
            return df
    except Exception:
        return df

    def _intraday_patch():
        ticker = yf.Ticker(symbol.strip())
        fi = ticker.fast_info
        price = getattr(fi, "last_price", None) or getattr(fi, "lastPrice", None)
        if price is None or not isinstance(price, (int, float)) or price <= 0:
            return df
        intra = ticker.history(period="1d", interval="1m")
        if intra is not None and not intra.empty:
            today_open = float(intra["Open"].iloc[0])
            today_high = float(intra["High"].max())
            today_low = float(intra["Low"].min())
            today_close = float(intra["Close"].iloc[-1])
            today_vol = int(intra["Volume"].sum())
        else:
            today_open = price
            today_high = price
            today_low = price
            today_close = price
            today_vol = 0
        out = df.copy()
        out.loc[out.index[-1], ["Open", "High", "Low", "Close", "Volume"]] = [
            today_open, today_high, today_low, today_close, today_vol
        ]
        return out

    try:
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_intraday_patch)
            return fut.result(timeout=14.0)
    except Exception:
        return df


def _download_symbol_pipeline(symbol: str) -> pd.DataFrame:
    sym = symbol.strip()
    with _YFIN_DOWNLOAD_LOCK:
        df = yf.download(sym, period="6mo", interval="1d", auto_adjust=False, progress=False, threads=False)
        if df is not None and not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
            df = _patch_today_nan_row(sym, df)
    if df is None:
        df = pd.DataFrame()
    df = normalize_df(df)
    return _apply_official_latest_bar(sym, df)


@st.cache_data(ttl=600, show_spinner=False)
def download_symbol(symbol: str) -> pd.DataFrame:
    sym = symbol.strip()
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_download_symbol_pipeline, sym)
        try:
            return fut.result(timeout=_YF_DOWNLOAD_TIMEOUT_SEC)
        except FuturesTimeoutError:
            raise TimeoutError(f"yfinance 逾時（>{_YF_DOWNLOAD_TIMEOUT_SEC:.0f}s）：{sym}")


def resolve_symbol(raw_input: str, name_map: dict | None = None):
    raw = raw_input.strip().upper()
    if not raw:
        return None, pd.DataFrame()
    nm_keys = {str(k).strip().upper() for k in (name_map or {}) if str(k).strip()}
    if raw.endswith(".TW") or raw.endswith(".TWO"):
        candidates = [raw]
    elif raw.isdigit() and len(raw) == 4:
        tw_s = f"{raw}.TW"
        two_s = f"{raw}.TWO"
        in_tw = tw_s in nm_keys
        in_two = two_s in nm_keys
        if in_tw and not in_two:
            candidates = [tw_s]
        elif in_two and not in_tw:
            candidates = [two_s]
        else:
            candidates = [tw_s, two_s]
    else:
        candidates = [raw, f"{raw}.TW", f"{raw}.TWO"]
    fallback = candidates[0]
    for sym in candidates:
        try:
            df = download_symbol(sym)
        except Exception:  # 包含 TimeoutError, OSError, URLError, HTTPError 等
            df = pd.DataFrame()
        if df is not None and not df.empty and len(df) >= 20:
            return sym, df.copy()
    return fallback, pd.DataFrame()


def _download_twii_pipeline() -> pd.DataFrame:
    with _YFIN_DOWNLOAD_LOCK:
        df = yf.download("^TWII", period="6mo", interval="1d", auto_adjust=False, progress=False, threads=False)
        if df is not None and not df.empty:
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    if df is None:
        return pd.DataFrame()
    return normalize_df(df)


@st.cache_data(ttl=_MARKET_DATA_CACHE_TTL_SEC, show_spinner=False)
def get_market_data():
    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_download_twii_pipeline)
        try:
            return fut.result(timeout=_YF_DOWNLOAD_TIMEOUT_SEC)
        except FuturesTimeoutError:
            return pd.DataFrame()

TWSE_OPENAPI_BASE = "https://openapi.twse.com.tw/v1"

def parse_num(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, (int, float)):
        num = float(value)
        try:
            if pd.isna(num):
                return None
        except Exception:
            pass
        return num
    s = str(value).strip().replace(",", "")
    if s.lower() in ["", "--", "---", "x", "除權息", "n/a", "nan", "none", "null"]:
        return None
    try:
        num = float(s)
        try:
            if pd.isna(num):
                return None
        except Exception:
            pass
        return num
    except Exception:
        return None


def safe_int_or_none(value):
    # parse_num 已保證回傳值不含 NaN/None，無需再次 isna 檢查
    num = parse_num(value)
    if num is None:
        return None
    try:
        return int(round(num))
    except Exception:
        return None


def safe_float_or_none(value):
    # parse_num 已保證回傳值不含 NaN/None，直接回傳即可
    return parse_num(value)


def _normalize_label(text) -> str:
    """標準化欄位名稱：去空白、全形轉半形、常用別名統一。"""
    if text is None:
        return ""
    s = str(text).strip()
    # 全形數字/英文 → 半形
    s = s.translate(str.maketrans(
        "０１２３４５６７８９ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ　",
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz "
    ))
    return s.replace(" ", "").replace("　", "").lower()


def _normalize_label_loose(text) -> str:
    s = _normalize_label(text)
    return re.sub(r"[^0-9a-z一-鿿]+", "", s)


def _inst_search_value(row: dict, exact_keys=None, contains_patterns=None):
    if not isinstance(row, dict):
        return None
    exact_keys = exact_keys or []
    contains_patterns = contains_patterns or []
    val = _inst_row_value(row, *exact_keys)
    if val not in [None, "", "--", "---"]:
        return val
    loose_map = {}
    for k, v in row.items():
        nk = _normalize_label_loose(k)
        if nk and nk not in loose_map:
            loose_map[nk] = v
    for pat in contains_patterns:
        npat = _normalize_label_loose(pat)
        if not npat:
            continue
        for nk, v in loose_map.items():
            if npat in nk:
                return v
    return None


def _twse_openapi_debug_store(endpoint: str, debug: dict):
    """將最近一次 OpenAPI 呼叫偵錯寫入 session（依 endpoint 分開）。"""
    try:
        ep = str(endpoint).lstrip("/")
        root = st.session_state.setdefault("twse_openapi_debug_by_endpoint", {})
        dbg = {**debug, "stored_at": datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d %H:%M:%S %Z")}
        root[ep] = dbg
        st.session_state["twse_openapi_last_debug"] = {"endpoint": ep, **dbg}
    except Exception:
        pass


def _is_twse_ssl_verify_failure(exc: BaseException) -> bool:
    """辨識證交所 OpenAPI 常見的憑證鏈／自簽導致無法 verify 的情況。"""
    try:
        import requests as _rq
        if isinstance(exc, _rq.exceptions.SSLError):
            return True
    except Exception:
        pass
    cur: BaseException | None = exc
    seen: set[int] = set()
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, ssl.SSLError):
            return True
        s = str(cur).lower()
        if "certificate_verify_failed" in s or "self-signed certificate" in s:
            return True
        cur = cur.__cause__ or cur.__context__
    return False


def _twse_openapi_rows_from_json_text(text: str, debug: dict, via: str) -> list:
    """解析 OpenAPI JSON body；via 為 'requests' 或 'urllib'（影響 debug 欄位與 final_status）。"""
    rows: list = []
    text = (text or "").strip()
    if not text:
        if via == "requests":
            debug["response_empty"] = True
            debug["requests_error"] = debug.get("requests_error") or "requests 成功但 body 為空"
            debug["final_status"] = "response_body_empty"
        else:
            debug["urllib_error"] = "urllib 成功但 body 為空"
            if debug["final_status"] == "empty":
                debug["final_status"] = "urllib_body_empty"
        return rows
    if not debug.get("response_text_preview"):
        debug["response_text_preview"] = text[:480] + ("…" if len(text) > 480 else "")
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        if via == "requests":
            debug["json_error"] = f"JSON 解析失敗（requests 路徑）：{e}"
            debug["final_status"] = "json_decode_requests"
        else:
            err = f"JSON 解析失敗（urllib 路徑）：{e}"
            debug["urllib_error"] = err
            if debug.get("json_error") is None:
                debug["json_error"] = err
            debug["final_status"] = "json_decode_urllib"
        return rows
    if via == "urllib":
        debug["parsed_top_type_urllib"] = type(data).__name__
    else:
        debug["parsed_top_type"] = type(data).__name__
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict):
        for key in ["data", "rows", "items", "result", "results"]:
            val = data.get(key)
            if isinstance(val, list):
                rows = val
                if via == "urllib":
                    debug["parsed_list_key_urllib"] = key
                else:
                    debug["parsed_list_key"] = key
                break
        if not rows:
            rows = [data]
            if via == "requests":
                debug["parsed_list_key"] = "(whole_dict_as_single_row)"
    if rows:
        debug["rows_count"] = len(rows)
        debug["final_status"] = "ok_urllib" if via == "urllib" else "ok"
    else:
        debug["final_status"] = "parsed_but_no_list" if via == "requests" else "urllib_parsed_no_rows"
    return rows


def _fetch_twse_openapi_impl(endpoint: str) -> tuple[list, dict]:
    """實際 HTTP／JSON 解析；回傳 (rows, debug_dict)，不寫入 session。"""
    ep = str(endpoint).lstrip("/")
    url = f"{TWSE_OPENAPI_BASE}/{ep}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    }
    debug = {
        "endpoint": ep,
        "url": url,
        "requests_http_status": None,
        "requests_error": None,
        "ssl_verify_relaxed": False,
        "requests_ssl_verify_error": None,
        "response_empty": False,
        "response_text_preview": None,
        "json_error": None,
        "parsed_top_type": None,
        "rows_count": 0,
        "urllib_error": None,
        "urllib_ssl_relaxed": False,
        "final_status": "empty",
    }
    rows: list = []

    try:
        resp = requests.get(url, headers=headers, timeout=15, verify=True)
        debug["requests_http_status"] = int(resp.status_code)
        if not resp.ok:
            debug["requests_error"] = f"HTTP {resp.status_code}（requests）"
            debug["final_status"] = "requests_http_not_ok"
        else:
            rows = _twse_openapi_rows_from_json_text(resp.text or "", debug, "requests")
    except Exception as e:
        if _is_twse_ssl_verify_failure(e):
            debug["requests_ssl_verify_error"] = f"{type(e).__name__}: {e}"
            try:
                resp = requests.get(url, headers=headers, timeout=15, verify=False)
                debug["ssl_verify_relaxed"] = True
                debug["requests_http_status"] = int(resp.status_code)
                debug["requests_error"] = None
                if not resp.ok:
                    debug["requests_error"] = f"HTTP {resp.status_code}（requests，verify=False）"
                    debug["final_status"] = "requests_http_not_ok"
                else:
                    rows = _twse_openapi_rows_from_json_text(resp.text or "", debug, "requests")
            except Exception as e2:
                debug["requests_error"] = f"requests（放寬 SSL 後仍失敗）：{type(e2).__name__}: {e2}"
                debug["final_status"] = "requests_exception"
        else:
            debug["requests_error"] = f"requests 例外：{type(e).__name__}: {e}"
            debug["final_status"] = "requests_exception"

    if not rows:
        debug["urllib_attempted"] = True
        req = Request(url, headers=headers)
        try:
            with urlopen(req, timeout=15) as resp:
                payload = resp.read().decode("utf-8", errors="ignore")
            text = (payload or "").strip()
            rows = _twse_openapi_rows_from_json_text(text, debug, "urllib")
        except Exception as e:
            err0 = f"urllib 例外：{type(e).__name__}: {e}"
            debug["urllib_error"] = err0
            if debug["final_status"] == "empty":
                debug["final_status"] = "urllib_exception"
            if _is_twse_ssl_verify_failure(e):
                try:
                    ctx = ssl.create_default_context()
                    ctx.check_hostname = False
                    ctx.verify_mode = ssl.CERT_NONE
                    debug["urllib_ssl_relaxed"] = True
                    with urlopen(req, timeout=15, context=ctx) as resp:
                        payload = resp.read().decode("utf-8", errors="ignore")
                    text = (payload or "").strip()
                    rows = _twse_openapi_rows_from_json_text(text, debug, "urllib")
                    if rows:
                        debug["urllib_error"] = None
                except Exception as e2:
                    debug["urllib_error"] = f"{err0}；urllib（放寬 SSL）：{type(e2).__name__}: {e2}"

    if not rows and debug["final_status"] == "empty":
        debug["final_status"] = "no_rows_both_paths_failed"
    elif not rows and debug["rows_count"] == 0:
        debug["rows_count"] = 0

    return rows, debug


def fetch_twse_openapi(endpoint: str):
    """呼叫 openapi.twse.com.tw/v1；失敗時仍回傳 []，但會寫入 session 偵錯（不靜默）。"""
    rows, dbg = _fetch_twse_openapi_impl(endpoint)
    _twse_openapi_debug_store(endpoint, dbg)
    return rows


# ─────────────────────────────────────────────────────────────────
#  模組 1：大盤統計
#  端點：/v1/exchangeReport/MI_INDEX（中文欄位）
#  備用：/v1/exchangeReport/STOCK_DAY_ALL 只有個股，不含大盤
# ─────────────────────────────────────────────────────────────────
def fetch_twse_market_index():
    """
    大盤加權指數 + 成交金額 + 成交股數
    端點：/v1/exchangeReport/MI_INDEX  ← 欄位全是中文
    欄位：指數、收盤指數、漲跌、漲跌點數
    附帶 debug：供畫面顯示為何未採用證交所收盤（不另做 cache，避免掩蓋失敗原因）。
    """
    mi_ep = "exchangeReport/MI_INDEX"
    rows = fetch_twse_openapi(mi_ep)
    try:
        mi_openapi_dbg = dict(st.session_state.get("twse_openapi_debug_by_endpoint", {}).get(mi_ep, {}))
    except Exception:
        mi_openapi_dbg = {}

    dbg = {
        "twse_rows_count": len(rows),
        "mi_openapi": mi_openapi_dbg,
        "chosen_index_name": None,
        "chosen_index_value_raw": None,
        "chosen_index_value_parsed": None,
        "best_prio": None,
        "fallback_row_index": None,
        "source_reason": "",
        "reject_reason": None,
        "effective_date": None,
        "note_cache": "fetch_twse_market_index 未使用 @st.cache_data，每次呼叫皆重新打 MI_INDEX。",
    }

    result = {
        "index_value": None,
        "index_change": None,
        "market_amount": None,
        "market_shares": None,
        "effective_date": None,
        "raw": rows[:8],
        "debug": dbg,
    }

    if not rows:
        dbg["source_reason"] = "MI_INDEX 回傳 0 筆（rows 為空）。請看 mi_openapi：requests／urllib／JSON 哪一層失敗。"
        return result

    if mi_openapi_dbg.get("final_status") not in ("ok", "ok_urllib"):
        dbg["source_reason"] = (
            f"OpenAPI 連線／解析未標記為成功（final_status={mi_openapi_dbg.get('final_status')!r}），"
            "但仍嘗試解析已回傳之 rows。"
        )

    # 優先對準「發行量加權股價指數」本列
    best_row = None
    best_prio = -1
    for row in rows:
        name = str(row.get("指數") or row.get("Index") or "")
        prio = 0
        if "發行量加權股價指數" in name:
            prio = 4
        elif name.strip() == "加權指數":
            prio = 3
        elif "發行量加權" in name and "報酬" not in name:
            prio = 2
        elif "加權" in name and "報酬" not in name:
            prio = 1
        if prio > best_prio:
            best_prio = prio
            best_row = row
    dbg["best_prio"] = best_prio

    if best_row is not None:
        raw_close = best_row.get("收盤指數") if best_row.get("收盤指數") is not None else best_row.get("ClosingIndex")
        dbg["chosen_index_name"] = str(best_row.get("指數") or best_row.get("Index") or "")
        dbg["chosen_index_value_raw"] = raw_close
        parsed = parse_num(raw_close)
        dbg["chosen_index_value_parsed"] = parsed
        if parsed is None:
            dbg["source_reason"] = (
                f"有命中優先列（prio={best_prio}，指數名稱：{dbg['chosen_index_name']!r}），"
                f"但 parse_num(收盤指數) 失敗；原始欄位值：{raw_close!r}。"
            )
        else:
            dbg["source_reason"] = f"命中優先列（prio={best_prio}）並成功解析收盤指數。"
        result["index_value"] = parsed
        result["index_change"] = parse_num(best_row.get("漲跌點數") or best_row.get("Change"))
        result["market_amount"] = parse_num(best_row.get("成交金額") or best_row.get("TradeValue"))
        result["market_shares"] = parse_num(best_row.get("成交股數") or best_row.get("TradeVolume"))

    if result["index_value"] is None and rows:
        for i, r in enumerate(rows):
            raw_c = r.get("收盤指數") if r.get("收盤指數") is not None else r.get("ClosingIndex")
            v = parse_num(raw_c)
            if v is not None:
                dbg["fallback_row_index"] = i
                dbg["chosen_index_name"] = str(r.get("指數") or r.get("Index") or "") or dbg.get("chosen_index_name")
                dbg["chosen_index_value_raw"] = raw_c
                dbg["chosen_index_value_parsed"] = v
                dbg["source_reason"] = f"優先列無有效收盤，改採第 {i} 筆第一個可解析之「收盤指數」。"
                result["index_value"] = v
                result["index_change"] = parse_num(r.get("漲跌點數") or r.get("Change"))
                result["market_amount"] = parse_num(r.get("成交金額") or r.get("TradeValue"))
                result["market_shares"] = parse_num(r.get("成交股數") or r.get("TradeVolume"))
                break

    if result["index_value"] is None:
        dbg["source_reason"] = dbg["source_reason"] or "有 rows 但無法從任何列解析出收盤指數（欄位名稱或格式與預期不符）。"
        if rows and isinstance(rows[0], dict):
            dbg["first_row_keys_sample"] = list(rows[0].keys())[:24]

    iv = result["index_value"]
    if iv is not None:
        try:
            ivf = float(iv)
        except (TypeError, ValueError):
            ivf = 0.0
        if ivf < 5000:
            dbg["reject_reason"] = f"解析值 {ivf} 低於內定門檻 5000，視為異常，不採用為加權收盤。"
            dbg["source_reason"] = (dbg.get("source_reason") or "") + " " + dbg["reject_reason"]

    if result["market_amount"] is None or result["market_shares"] is None:
        day_rows = fetch_twse_openapi("exchangeReport/STOCK_DAY_ALL")
        total_val = sum(parse_num(r.get("TradeValue")) or 0 for r in day_rows)
        total_vol = sum(parse_num(r.get("TradeVolume")) or 0 for r in day_rows)
        if total_val > 0 and result["market_amount"] is None:
            result["market_amount"] = total_val
        if total_vol > 0 and result["market_shares"] is None:
            result["market_shares"] = total_vol

    if rows and isinstance(rows[0], dict):
        result["effective_date"] = str(rows[0].get("日期") or rows[0].get("Date") or "")
    dbg["effective_date"] = result["effective_date"]
    return result


@st.cache_data(ttl=180, show_spinner=False)
def fetch_twse_market_stats():
    """
    上漲/下跌家數
    端點：/v1/exchangeReport/MI_INDEX（同一端點，找含家數的列）
    備用：全日行情 STOCK_DAY_ALL 自行統計漲跌家數
    """
    rows = fetch_twse_openapi("exchangeReport/MI_INDEX")
    up, down = None, None
    # MI_INDEX 中某幾筆包含家數，欄位中文
    for row in rows:
        up_v  = parse_num(row.get("上漲家數") or row.get("UpCount"))
        dn_v  = parse_num(row.get("下跌家數") or row.get("DownCount"))
        if up_v is not None and up is None:
            up = up_v
        if dn_v is not None and down is None:
            down = dn_v
        if up is not None and down is not None:
            break
    # 備援：直接從 STOCK_DAY_ALL 的 Change 欄位統計
    if up is None or down is None:
        day_rows = fetch_twse_openapi("exchangeReport/STOCK_DAY_ALL")
        cnt_up, cnt_dn = 0, 0
        for r in day_rows:
            chg_str = str(r.get("Change") or "").strip()
            if chg_str.startswith("+") or (chg_str and chg_str[0].isdigit() and float(chg_str.replace(",","") or 0) > 0):
                cnt_up += 1
            elif chg_str.startswith("-"):
                cnt_dn += 1
        if cnt_up > 0 and up is None:
            up = cnt_up
        if cnt_dn > 0 and down is None:
            down = cnt_dn
    return {"up_count": up, "down_count": down, "raw": rows[:10]}


# ─────────────────────────────────────────────────────────────────
#  整合後的 get_twse_market_snapshot_v2
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=180, show_spinner=False)
def get_twse_market_snapshot_v2():
    idx_data = fetch_twse_market_index()
    ms_data  = fetch_twse_market_stats()
    snapshot = {
        "index_name":    "加權指數",
        "index_value":   idx_data.get("index_value"),
        "index_change":  idx_data.get("index_change"),
        "market_amount": idx_data.get("market_amount"),
        "market_shares": idx_data.get("market_shares"),
        "up_count":      ms_data.get("up_count"),
        "down_count":    ms_data.get("down_count"),
        "effective_date": idx_data.get("effective_date"),
        "source_endpoint": "TWSE_OPENAPI_V1/MI_INDEX",
        "raw": (idx_data.get("raw") or []) + (ms_data.get("raw") or []),
        "source_rows": len((idx_data.get("raw") or []) + (ms_data.get("raw") or [])),
        "taiex_debug": idx_data.get("debug"),
    }
    return snapshot


# ─────────────────────────────────────────────────────────────────
#  模組 2：個股本益比 / 殖利率 / 股價淨值比
#  端點：/v1/exchangeReport/BWIBBU_ALL  ← 英文 CamelCase 欄位
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_twse_valuation_all():
    """
    端點：/v1/exchangeReport/BWIBBU_ALL
    欄位（英文）：Code, Name, PEratio, DividendYield, PBratio
    """
    rows = fetch_twse_openapi("exchangeReport/BWIBBU_ALL")
    result = {}
    for r in rows:
        # 確認用英文 key（已驗證）
        code = str(r.get("Code") or "").strip()
        if not code:
            continue
        result[code] = {
            "name":      str(r.get("Name") or ""),
            "pe":        parse_num(r.get("PEratio")),
            "yield_pct": parse_num(r.get("DividendYield")),
            "pb":        parse_num(r.get("PBratio")),
        }
    return result


def get_stock_valuation(code_4digit: str, valuation_map: dict) -> dict:
    return valuation_map.get(code_4digit) or valuation_map.get(f"{code_4digit}.TW") or {}


def _inst_row_value(row: dict, *keys):
    if not isinstance(row, dict):
        return None
    normalized = {}
    for k, v in row.items():
        nk = _normalize_label(k)
        if nk and nk not in normalized:
            normalized[nk] = v
    for key in keys:
        if key in row:
            return row.get(key)
        nk = _normalize_label(key)
        if nk in normalized:
            return normalized.get(nk)
    return None


# ─────────────────────────────────────────────────────────────────
#  模組 3：三大法人買賣超
#  V137：直接升級成「抓取 + HTTP偵錯」整包，不再先拆小修
# ─────────────────────────────────────────────────────────────────
def _http_attempt(url: str, accept: str = "text/html,application/json,*/*", parse: str = "text"):
    attempts = []

    def _record(method: str, ok: bool, status_code=None, preview: str = "", error: str = ""):
        attempts.append({
            "method": method,
            "ok": ok,
            "status_code": status_code,
            "preview": (preview or "")[:180].replace("\n", " ").replace("\r", " "),
            "error": error,
            "url": url,
        })

    for verify_flag in [True, False]:
        method = f"requests({'verify' if verify_flag else 'noverify'})"
        try:
            resp = requests.get(url, headers=_http_headers(accept), timeout=15, verify=verify_flag)
            body = resp.text or ""
            _record(method, bool(resp.ok and body.strip()), resp.status_code, body[:180], "")
            if resp.ok and body.strip():
                if parse == "json":
                    try:
                        return json.loads(body), attempts
                    except Exception as e:
                        _record(method + '-json-parse', False, resp.status_code, body[:180], f'{type(e).__name__}: {e}')
                else:
                    return body, attempts
        except Exception as e:
            _record(method, False, None, "", f"{type(e).__name__}: {e}")

    try:
        req = Request(url, headers=_http_headers(accept))
        with urlopen(req, timeout=15) as resp:
            body = resp.read().decode('utf-8', errors='ignore')
            status = getattr(resp, 'status', None)
        _record('urllib', bool(body.strip()), status, body[:180], "")
        if body.strip():
            if parse == 'json':
                try:
                    return json.loads(body), attempts
                except Exception as e:
                    _record('urllib-json-parse', False, status, body[:180], f'{type(e).__name__}: {e}')
            else:
                return body, attempts
    except Exception as e:
        _record('urllib', False, None, "", f"{type(e).__name__}: {e}")

    return None, attempts


def _pick_attempt_preview(attempts):
    for a in attempts:
        if a.get('preview'):
            return a.get('preview', '')
    return ''


def _pick_attempt_error(attempts):
    errs = [a.get('error', '') for a in attempts if a.get('error')]
    return ' | '.join(errs[:3])


def _fetch_twse_t86_json_recent_v137(days_back: int = 14):
    all_attempts = []
    for delta in range(days_back + 1):
        d = datetime.now() - timedelta(days=delta)
        date_str = d.strftime('%Y%m%d')
        urls = [
            f'https://www.twse.com.tw/rwd/zh/fund/T86?response=json&date={date_str}&selectType=ALLBUT0999',
            f'https://www.twse.com.tw/fund/T86?response=json&date={date_str}&selectType=ALLBUT0999',
        ]
        for url in urls:
            payload, attempts = _http_attempt(url, 'application/json,text/plain,*/*', parse='json')
            all_attempts.extend(attempts)
            if not isinstance(payload, dict):
                continue
            fields = payload.get('fields') or []
            data_rows = payload.get('data') or []
            parsed = []
            if isinstance(fields, list) and isinstance(data_rows, list):
                for raw in data_rows:
                    if isinstance(raw, list):
                        parsed.append({str(fields[i]): raw[i] if i < len(raw) else None for i in range(len(fields))})
                    elif isinstance(raw, dict):
                        parsed.append(raw)
            if parsed:
                return parsed, all_attempts
    return [], all_attempts


def _fetch_twse_t86_html_recent_v137(days_back: int = 14):
    all_attempts = []
    for delta in range(days_back + 1):
        d = datetime.now() - timedelta(days=delta)
        date_str = d.strftime('%Y%m%d')
        urls = [
            f'https://www.twse.com.tw/fund/T86?date={date_str}&selectType=ALLBUT0999',
            f'https://www.twse.com.tw/fund/T86?response=html&date={date_str}&selectType=ALLBUT0999',
            f'https://www.twse.com.tw/rwd/zh/fund/T86?date={date_str}&selectType=ALLBUT0999',
            f'https://www.twse.com.tw/rwd/zh/fund/T86?response=html&date={date_str}&selectType=ALLBUT0999',
        ]
        for url in urls:
            html, attempts = _http_attempt(url, 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8', parse='text')
            all_attempts.extend(attempts)
            if not html or str(html).startswith('[FETCH_ERROR]'):
                continue
            try:
                tables = pd.read_html(StringIO(html))
            except Exception:
                tables = []
            for df in tables:
                try:
                    df2 = df.copy()
                    if isinstance(df2.columns, pd.MultiIndex):
                        df2.columns = [str(c[-1] if isinstance(c, tuple) else c) for c in df2.columns]
                    df2 = df2.fillna('')
                    columns = [str(c).strip() for c in df2.columns]
                    if not any('證券代號' in c for c in columns):
                        continue
                    parsed = []
                    for raw in df2.astype(str).values.tolist():
                        parsed.append({columns[i]: raw[i] if i < len(raw) else None for i in range(len(columns))})
                    if parsed:
                        return parsed, all_attempts
                except Exception:
                    continue
    return [], all_attempts



def _fetch_twse_t86_csv_recent_v137(days_back: int = 14):
    all_attempts = []
    for delta in range(days_back + 1):
        d = datetime.now() - timedelta(days=delta)
        date_str = d.strftime('%Y%m%d')
        urls = [
            f'https://www.twse.com.tw/fund/T86?response=csv&date={date_str}&selectType=ALLBUT0999',
            f'https://www.twse.com.tw/rwd/zh/fund/T86?response=csv&date={date_str}&selectType=ALLBUT0999',
        ]
        for url in urls:
            text_payload, attempts = _http_attempt(url, 'text/csv,text/plain,*/*', parse='text')
            all_attempts.extend(attempts)
            if not text_payload or str(text_payload).startswith('[FETCH_ERROR]'):
                continue
            try:
                lines = []
                for line in str(text_payload).splitlines():
                    s = str(line).strip()
                    if not s:
                        continue
                    if s.startswith('=') or '證券代號' in s or s.startswith('"') or s[0].isdigit():
                        lines.append(s)
                if not lines:
                    continue
                df = pd.read_csv(StringIO('\n'.join(lines)))
                if df.empty:
                    continue
                df.columns = [str(c).strip() for c in df.columns]
                if '證券代號' not in df.columns:
                    continue
                parsed = []
                for raw in df.fillna('').astype(str).values.tolist():
                    parsed.append({df.columns[i]: raw[i] if i < len(df.columns) else None for i in range(len(df.columns))})
                if parsed:
                    return parsed, all_attempts
            except Exception:
                continue
    return [], all_attempts


def _fetch_twse_openapi_rows_v137(endpoint: str):
    url = f"{TWSE_OPENAPI_BASE}/{endpoint.lstrip('/')}"
    payload, attempts = _http_attempt(url, 'application/json,text/plain,*/*', parse='json')
    rows = []
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        for key in ['data', 'rows', 'items', 'result', 'results']:
            val = payload.get(key)
            if isinstance(val, list):
                rows = val
                break
        if not rows:
            rows = [payload]
    return rows, attempts


def _parse_institutional_result(rows):
    result = {}
    matched_rows = 0
    sample_codes = []
    for r in rows:
        if not isinstance(r, dict):
            continue

        code = str(_inst_search_value(
            r,
            exact_keys=['證券代號', 'Code', '證券名稱代號', '股票代號', 'SecuritiesCompanyCode', 'SecurityCode'],
            contains_patterns=['securitiescompanycode', 'securitycode', '證券代號', '股票代號']
        ) or '').strip()
        if not code:
            continue

        foreign = parse_num(_inst_search_value(
            r,
            exact_keys=[
                '外陸資買賣超股數(不含外資自營商)',
                '外資及陸資(不含外資自營商)買賣超股數',
                '外資及陸資淨買股數',
                '外資買賣超',
                'foreigninvestors',
                '外資及陸資買賣超股數',
                'Foreign Investors include Mainland Area Investors (Foreign Dealers excluded)-Difference',
                'Net Foreign & Mainland Chinese Purchase (share)',
            ],
            contains_patterns=[
                'foreigninvestorsincludemainlandareainvestorsforeigndealersexcludeddifference',
                'netforeignmainlandchinesepurchaseshare',
                '外資及陸資淨買股數',
                '外資買賣超'
            ]
        ))

        trust = parse_num(_inst_search_value(
            r,
            exact_keys=[
                '投信買賣超股數', '投信淨買股數', 'InvestmentTrust',
                'Securities Investment Trust Companies-Difference',
                'Net Securities Investment Co. Purchase (share)',
            ],
            contains_patterns=[
                'securitiesinvestmenttrustcompaniesdifference',
                'netsecuritiesinvestmentcopurchaseshare',
                '投信淨買股數', '投信買賣超股數'
            ]
        ))

        dealer_direct = parse_num(_inst_search_value(
            r,
            exact_keys=[
                '自營商買賣超股數', '自營商淨買股數', 'Dealer', '自營商買賣超股數(自行買賣+避險)',
                'Dealers Total-Difference', 'Total Net Dealers Purchase (Share)', 'Net Dealers Purchase (share)'
            ],
            contains_patterns=[
                'dealerstotaldifference', 'totalnetdealerspurchaseshare', 'netdealerspurchaseshare',
                '自營商買賣超股數自行買賣避險', '自營商淨買股數'
            ]
        ))
        dealer_self = parse_num(_inst_search_value(
            r,
            exact_keys=['自營商買賣超股數(自行買賣)', 'Dealers (Proprietary)-Difference', 'Net Dealers (proprietary) Purchase (share)'],
            contains_patterns=['dealersproprietarydifference', 'netdealersproprietarypurchaseshare', '自營商買賣超股數自行買賣']
        ))
        dealer_hedge = parse_num(_inst_search_value(
            r,
            exact_keys=['自營商買賣超股數(避險)', 'Dealers (Hedge)-Difference', 'Net Dealers (hedge) Purchase (share)'],
            contains_patterns=['dealershedgedifference', 'netdealershedgepurchaseshare', '自營商買賣超股數避險']
        ))
        if dealer_direct is None and (dealer_self is not None or dealer_hedge is not None):
            dealer_direct = (dealer_self or 0) + (dealer_hedge or 0)

        total = parse_num(_inst_search_value(
            r,
            exact_keys=[
                '三大法人買賣超股數', '三大法人買賣超股數合計', 'Total', '三大法人買賣超',
                'Total Difference', 'Total Net Purchase (Share)'
            ],
            contains_patterns=[
                'totaldifference', 'totalnetpurchaseshare', '三大法人買賣超股數合計', '三大法人買賣超股數'
            ]
        ))
        if total is None:
            parts = [x for x in [foreign, trust, dealer_direct] if x is not None]
            if parts:
                total = sum(parts)
        if all(x is None for x in [foreign, trust, dealer_direct, total]):
            continue

        matched_rows += 1
        if len(sample_codes) < 8:
            sample_codes.append(code)
        result[code] = {
            'name': str(_inst_search_value(
                r,
                exact_keys=['證券名稱', 'Name', '股票名稱', 'CompanyName'],
                contains_patterns=['companyname', '證券名稱', '股票名稱']
            ) or ''),
            'foreign': foreign,
            'trust': trust,
            'dealer': dealer_direct,
            'total': total,
        }
    return result, matched_rows, sample_codes


def _roc_date_string(dt):
    year = dt.year - 1911
    return f"{year:03d}/{dt.month:02d}/{dt.day:02d}"


def _flatten_columns(df: pd.DataFrame):
    cols = []
    for c in df.columns:
        if isinstance(c, tuple):
            parts = [str(x).strip() for x in c if str(x).strip() and str(x).strip().lower() != 'nan']
            cols.append(' '.join(parts))
        else:
            cols.append(str(c).strip())
    return cols


def _parse_tpex_institutional_html_rows(text_payload: str):
    if not text_payload:
        return []
    try:
        tables = pd.read_html(StringIO(text_payload))
    except Exception:
        return []
    parsed = []
    for df in tables:
        try:
            df = df.copy()
            df.columns = _flatten_columns(df)
            df = df.fillna('')
        except Exception:
            continue
        norm_cols = {_normalize_label(c): c for c in df.columns}
        code_col = None
        name_col = None
        for nk, orig in norm_cols.items():
            if ('代號' in nk or 'code' == nk) and code_col is None:
                code_col = orig
            if ('名稱' in nk or 'name' == nk) and name_col is None:
                name_col = orig
        def pick(*patterns):
            for pat in patterns:
                npat = _normalize_label(pat)
                for nk, orig in norm_cols.items():
                    if npat and npat in nk:
                        return orig
            return None
        foreign_col = pick('外資及陸資淨買股數', '外資及陸資(不含外資自營商)買賣超股數', '外資及陸資買賣超股數', '外資買賣超')
        trust_col = pick('投信淨買股數', '投信買賣超股數')
        dealer_col = pick('自營商淨買股數', '自營商買賣超股數', '自營商買賣超股數(自行買賣+避險)')
        dealer_self_col = pick('自營商(自行買賣)淨買股數', '自營商自行買賣淨買股數')
        dealer_hedge_col = pick('自營商(避險)淨買股數', '自營商避險淨買股數')
        total_col = pick('三大法人買賣超股數合計', '三大法人買賣超股數', '三大法人買賣超')
        if not code_col or not (foreign_col or trust_col or dealer_col or total_col):
            continue
        for _, row in df.iterrows():
            code = str(row.get(code_col, '')).strip()
            if not re.fullmatch(r'\d{4,6}[A-Z]?', code):
                continue
            dealer_direct = parse_num(row.get(dealer_col, None))
            dealer_self = parse_num(row.get(dealer_self_col, None)) if dealer_self_col else None
            dealer_hedge = parse_num(row.get(dealer_hedge_col, None)) if dealer_hedge_col else None
            if dealer_direct is None and (dealer_self is not None or dealer_hedge is not None):
                dealer_direct = (dealer_self or 0) + (dealer_hedge or 0)
            item = {
                '證券代號': code,
                '證券名稱': str(row.get(name_col, '')).strip() if name_col else '',
                '外資及陸資(不含外資自營商)買賣超股數': parse_num(row.get(foreign_col, None)) if foreign_col else None,
                '投信買賣超股數': parse_num(row.get(trust_col, None)) if trust_col else None,
                '自營商買賣超股數': dealer_direct,
                '三大法人買賣超股數': parse_num(row.get(total_col, None)) if total_col else None,
            }
            parsed.append(item)
        if parsed:
            return parsed
    return parsed


def _parse_tpex_institutional_text_rows(text_payload: str):
    if not text_payload:
        return []
    parsed = []
    for raw_line in str(text_payload).splitlines():
        line = str(raw_line).strip().replace('　', ' ')
        if not line:
            continue
        if not re.match(r'^\d{4,6}[A-Z]?\s+', line):
            continue
        parts = re.split(r'\s+', line)
        if len(parts) < 24:
            continue
        code = parts[0].strip()
        name = parts[1].strip()
        nums = [parse_num(x) for x in parts[2:]]
        if len(nums) < 22:
            continue
        parsed.append({
            '證券代號': code,
            '證券名稱': name,
            '外資及陸資(不含外資自營商)買賣超股數': nums[2],
            '投信買賣超股數': nums[11],
            '自營商買賣超股數': nums[20],
            '三大法人買賣超股數': nums[21],
        })
    return parsed


def _fetch_tpex_web_3insti_recent_v149(days_back: int = 14):
    all_attempts = []
    fixed_urls = [
        'https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=htm',
        'https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/detail/day.html',
    ]
    for url in fixed_urls:
        text_payload, attempts = _http_attempt(url, 'text/html,text/csv,text/plain,*/*', parse='text')
        all_attempts.extend(attempts)
        if not text_payload or str(text_payload).startswith('[FETCH_ERROR]'):
            continue
        rows = _parse_tpex_institutional_html_rows(str(text_payload))
        if not rows:
            rows = _parse_tpex_institutional_text_rows(str(text_payload))
        if rows:
            return rows, all_attempts

    for delta in range(days_back + 1):
        d = datetime.now() - timedelta(days=delta)
        roc_date = _roc_date_string(d)
        urls = [
            f'https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?d={roc_date}&l=zh-tw&o=htm',
            f'https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?d={roc_date}&l=zh-tw&o=htm&s=0&se=EW&t=D',
            f'https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?d={roc_date}&l=zh-tw&o=csv',
            f'https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?d={roc_date}&l=zh-tw&o=csv&s=0&se=EW&t=D',
        ]
        for url in urls:
            text_payload, attempts = _http_attempt(url, 'text/html,text/csv,text/plain,*/*', parse='text')
            all_attempts.extend(attempts)
            if not text_payload or str(text_payload).startswith('[FETCH_ERROR]'):
                continue
            rows = _parse_tpex_institutional_html_rows(str(text_payload))
            if not rows:
                rows = _parse_tpex_institutional_text_rows(str(text_payload))
            if rows:
                return rows, all_attempts
    return [], all_attempts


def _fetch_tpex_mainboard_daily_html_recent_v149(days_back: int = 14):
    all_attempts = []
    urls = [
        'https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/detail/day.html',
        'https://www.tpex.org.tw/zh-tw/mainboard/trading/major-institutional/detail/day',
    ]
    for url in urls:
        text_payload, attempts = _http_attempt(url, 'text/html,text/plain,*/*', parse='text')
        all_attempts.extend(attempts)
        if not text_payload or str(text_payload).startswith('[FETCH_ERROR]'):
            continue
        rows = _parse_tpex_institutional_html_rows(str(text_payload))
        if not rows:
            rows = _parse_tpex_institutional_text_rows(str(text_payload))
        if rows:
            return rows, all_attempts
    return [], all_attempts


def _fetch_tpex_openapi_rows_v149(endpoint: str):
    urls = [
        f'https://www.tpex.org.tw/openapi/v1/{endpoint.lstrip("/")}',
        f'https://www.tpex.org.tw/openapi/v1/{endpoint.lstrip("/")}/',
    ]
    all_attempts = []
    for url in urls:
        payload, attempts = _http_attempt(url, 'application/json,text/plain,*/*', parse='json')
        all_attempts.extend(attempts)
        rows = []
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict):
            for key in ['data', 'rows', 'items', 'result', 'results']:
                val = payload.get(key)
                if isinstance(val, list):
                    rows = val
                    break
            if not rows and payload:
                rows = [payload]
        if rows:
            return rows, all_attempts
    return [], all_attempts


def _fetch_tpex_openapi_three_insti_recent_v149(days_back: int = 14):
    candidates = [
        'tpex_3insti_daily_trading',
        'tpex_mainboard_3insti_daily_trading',
    ]
    all_attempts = []
    for endpoint in candidates:
        rows, attempts = _fetch_tpex_openapi_rows_v149(endpoint)
        all_attempts.extend(attempts)
        if rows:
            return rows, all_attempts
    return [], all_attempts


@st.cache_data(ttl=120, show_spinner=False)
def fetch_institutional_bundle_all():
    import time as _inst_time
    _inst_deadline = _inst_time.time() + 15
    source_groups = [
        ('TWSE', [
            ('TWSE_T86_JSON', lambda: _fetch_twse_t86_json_recent_v137(5)),
            ('TWSE_T86_CSV', lambda: _fetch_twse_t86_csv_recent_v137(5)),
            ('TWSE_OPENAPI_TWT38U_ALL', lambda: _fetch_twse_openapi_rows_v137('fund/TWT38U_ALL')),
        ]),
        ('TPEx', [
            ('TPEX_OPENAPI_3INSTI', lambda: _fetch_tpex_openapi_three_insti_recent_v149(5)),
            ('TPEX_3INSTI_WEB', lambda: _fetch_tpex_web_3insti_recent_v149(5)),
        ]),
    ]
    debug_rows = []
    final_maps = {'TWSE': {}, 'TPEx': {}}
    chosen_sources = {'TWSE': 'NONE', 'TPEx': 'NONE'}
    for market_name, attempts in source_groups:
        for source_name, loader in attempts:
            if final_maps[market_name]:
                break
            if _inst_time.time() > _inst_deadline:
                debug_rows.append({'市場': market_name, '來源': source_name, '狀態': '超時跳過', '原始筆數': 0, '解析命中': 0, '示例代碼': '', 'URL': '', 'HTTP': '', '預覽': '', '錯誤': '15s deadline exceeded'})
                continue
            loader_error = ''
            fetch_attempts = []
            try:
                rows, fetch_attempts = loader()
                rows = rows or []
            except Exception as e:
                rows = []
                loader_error = f'{type(e).__name__}: {e}'
            parsed_map, matched_rows, sample_codes = _parse_institutional_result(rows)
            raw_count = len(rows) if isinstance(rows, list) else 0
            debug_rows.append({
                '市場': market_name,
                '來源': source_name,
                '原始筆數': raw_count,
                '解析命中': matched_rows,
                '示例代碼': ', '.join(sample_codes[:5]) if sample_codes else '',
                '狀態': '命中' if matched_rows > 0 else ('有回應未命中' if raw_count > 0 else ('錯誤' if loader_error or _pick_attempt_error(fetch_attempts) else '無資料')),
                'URL': fetch_attempts[0].get('url', '') if fetch_attempts else '',
                'HTTP': ' | '.join([f"{a.get('method')}:{a.get('status_code') if a.get('status_code') is not None else '-'}" for a in fetch_attempts[:3]]),
                '預覽': _pick_attempt_preview(fetch_attempts),
                '錯誤': loader_error or _pick_attempt_error(fetch_attempts),
            })
            if parsed_map and not final_maps[market_name]:
                final_maps[market_name] = parsed_map
                chosen_sources[market_name] = source_name
    merged = {}
    merged.update(final_maps.get('TWSE', {}))
    merged.update(final_maps.get('TPEx', {}))
    return {'map': merged, 'market_maps': final_maps, 'sources': chosen_sources, 'debug': debug_rows}


def fetch_twse_institutional_bundle():
    return fetch_institutional_bundle_all()


def fetch_twse_institutional_all():
    return fetch_institutional_bundle_all().get('map', {})


def get_stock_institutional(stock_code: str, inst_map: dict) -> dict:
    raw = str(stock_code or '').strip().upper()
    base = raw.split('.')[0]
    return (
        inst_map.get(raw)
        or inst_map.get(base)
        or inst_map.get(f"{base}.TW")
        or inst_map.get(f"{base}.TWO")
        or {}
    )


def resolve_institutional_context(stock_code: str, base_row: dict | pd.Series | None, bundle: dict | None = None) -> dict:
    bundle = bundle or fetch_institutional_bundle_all()
    inst_map = bundle.get('map', {}) if isinstance(bundle, dict) else {}
    market_maps = bundle.get('market_maps', {}) if isinstance(bundle, dict) else {}
    sources = bundle.get('sources', {}) if isinstance(bundle, dict) else {}

    row_dict = dict(base_row) if base_row is not None else {}
    raw = str(stock_code or '').strip().upper()
    base = raw.split('.')[0]

    hit_market = None
    if raw in market_maps.get('TWSE', {}) or base in market_maps.get('TWSE', {}) or f"{base}.TW" in market_maps.get('TWSE', {}):
        hit_market = 'TWSE'
    elif raw in market_maps.get('TPEx', {}) or base in market_maps.get('TPEx', {}) or f"{base}.TWO" in market_maps.get('TPEx', {}):
        hit_market = 'TPEx'

    if hit_market == 'TWSE':
        chosen_source = sources.get('TWSE', 'NONE')
    elif hit_market == 'TPEx':
        chosen_source = sources.get('TPEx', 'NONE')
    else:
        chosen_source = 'NONE'

    inst = get_stock_institutional(stock_code, inst_map)
    vol = safe_float_or_none(row_dict.get('成交量', None))
    close_price = safe_float_or_none(row_dict.get('收盤', None))
    prev_close = safe_float_or_none(row_dict.get("昨收", None))
    if prev_close is None:
        prev_close = safe_float_or_none(row_dict.get("前收", None))
    if prev_close is None and close_price is not None:
        try:
            chg = safe_float_or_none(row_dict.get("TWSE漲跌價差", None))
            if chg is not None:
                prev_close = float(close_price) - float(chg)
        except Exception:
            prev_close = None

    if inst:
        signal = build_institutional_signal(stock_code, inst_map, vol, close_price, prev_close)
        row_dict.update(signal)
        row_dict['法人命中代碼'] = '是'
        row_dict['法人資料源'] = chosen_source
    else:
        row_dict['法人方向'] = '資料不足'
        row_dict['法人共振'] = row_dict.get('法人共振', '中性') or '中性'
        row_dict['法人摘要'] = '三大法人資料不足'
        row_dict['法人命中代碼'] = '否'
        row_dict['法人資料源'] = 'NONE'
        for k in ['三大法人買賣超', '外資買賣超', '投信買賣超', '自營商買賣超', '法人佔成交量比%', '法人分數']:
            row_dict[k] = None if k != '法人分數' else 0

    return row_dict


def classify_institutional_direction(total: float | None, ratio_pct: float | None) -> str:
    if total is None or ratio_pct is None:
        return '資料不足'
    if total >= 0:
        if ratio_pct >= 8:
            return '偏多'
        if ratio_pct >= 2:
            return '小偏多'
        return '中性'
    if ratio_pct <= -8:
        return '偏空'
    if ratio_pct <= -2:
        return '小偏空'
    return '中性'


def build_institutional_signal(code_4digit: str, inst_map: dict, volume: float | int | None, close_price: float | None = None, prev_close: float | None = None) -> dict:
    inst = get_stock_institutional(code_4digit, inst_map) or {}
    total = inst.get('total', None)
    foreign = inst.get('foreign', None)
    trust = inst.get('trust', None)
    dealer = inst.get('dealer', None)
    ratio_pct = None
    if volume not in [None, 0, ''] and total not in [None, '']:
        try:
            ratio_pct = round(float(total) / float(volume) * 100, 2)
        except Exception:
            ratio_pct = None
    direction = classify_institutional_direction(total, ratio_pct)
    price_dir = None
    if close_price not in [None, ''] and prev_close not in [None, '', 0]:
        try:
            if float(close_price) > float(prev_close):
                price_dir = 'up'
            elif float(close_price) < float(prev_close):
                price_dir = 'down'
            else:
                price_dir = 'flat'
        except Exception:
            price_dir = None
    resonance = '中性'
    score = 0
    if direction in ['偏多', '小偏多']:
        score += 4 if direction == '偏多' else 2
        if price_dir == 'up':
            resonance = '偏多共振'
            score += 2
    elif direction in ['偏空', '小偏空']:
        score += -4 if direction == '偏空' else -2
        if price_dir == 'down':
            resonance = '偏空共振'
            score -= 2
    if direction == '資料不足':
        summary = '三大法人資料不足'
    else:
        ratio_text = f'，佔量 {ratio_pct:.2f}%' if ratio_pct is not None else ''
        safe_total = safe_int_or_none(total)
        summary = f'三大法人 {direction}（{fmt_lots(safe_total)}{ratio_text}）' if safe_total is not None else f'三大法人 {direction}{ratio_text}'
    return {
        '三大法人買賣超': safe_int_or_none(total),
        '法人佔成交量比%': ratio_pct,
        '法人方向': direction,
        '法人共振': resonance,
        '法人分數': int(score),
        '法人摘要': summary,
        '外資買賣超': safe_int_or_none(foreign),
        '投信買賣超': safe_int_or_none(trust),
        '自營商買賣超': safe_int_or_none(dealer),
    }


def fetch_twse_margin_all():
    """
    端點：/v1/exchangeReport/MI_MARGN
    欄位（中文）：
      股票代號、股票名稱
      融資買進、融資賣出、融資今日餘額、融資前日餘額
      融券買進、融券賣出、融券今日餘額、融券前日餘額
    """
    rows = fetch_twse_openapi("exchangeReport/MI_MARGN")
    result = {}
    for r in rows:
        code = str(r.get("股票代號") or r.get("Code") or "").strip()
        if not code:
            continue
        margin_today = parse_num(r.get("融資今日餘額") or r.get("MarginPurchaseRemainingShares"))
        margin_prev  = parse_num(r.get("融資前日餘額"))
        short_today  = parse_num(r.get("融券今日餘額") or r.get("ShortSaleRemainingShares"))
        short_prev   = parse_num(r.get("融券前日餘額"))
        margin_chg = None
        short_chg  = None
        if margin_today is not None and margin_prev is not None:
            margin_chg = margin_today - margin_prev
        if short_today is not None and short_prev is not None:
            short_chg = short_today - short_prev
        result[code] = {
            "name":              str(r.get("股票名稱") or r.get("Name") or ""),
            "margin_buy":        margin_today,
            "short_sell":        short_today,
            "margin_buy_change": margin_chg,
            "short_sell_change": short_chg,
        }
    return result


def get_stock_margin(code_4digit: str, margin_map: dict) -> dict:
    return margin_map.get(code_4digit) or margin_map.get(f"{code_4digit}.TW") or {}


# ─────────────────────────────────────────────────────────────────
#  模組 5：借券放空（SBL）
#  端點：/v1/exchangeReport/TWTASU（融券賣出與借券賣出，中文欄位）
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_twse_sbl_all():
    """
    端點：/v1/exchangeReport/TWTASU
    欄位（中文）：
      股票代號、股票名稱
      借券賣出成交數量、借券賣出當日餘額
      融券賣出成交數量、融券賣出當日餘額
    """
    rows = fetch_twse_openapi("exchangeReport/TWTASU")
    result = {}
    for r in rows:
        code = str(r.get("股票代號") or r.get("Code") or "").strip()
        if not code:
            continue
        result[code] = {
            "name":        str(r.get("股票名稱") or r.get("Name") or ""),
            "sbl_balance": parse_num(r.get("借券賣出當日餘額") or r.get("SBLBalance")),
            "sbl_sale":    parse_num(r.get("借券賣出成交數量") or r.get("SBLSaleVolume")),
            "short_bal":   parse_num(r.get("融券賣出當日餘額")),
            "short_sale":  parse_num(r.get("融券賣出成交數量")),
        }
    return result


# ─────────────────────────────────────────────────────────────────
#  模組 6：上市公司基本資料
#  端點：/v1/opendata/t187ap03_L（中文欄位）
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=86400, show_spinner=False)
def fetch_twse_company_info_all():
    """
    端點：/v1/opendata/t187ap03_L
    欄位（中文）：
      公司代號、公司名稱、公司簡稱
      產業類別、上市日期、實收資本額(元)
    """
    rows = fetch_twse_openapi("opendata/t187ap03_L")
    result = {}
    for r in rows:
        code = str(r.get("公司代號") or "").strip()
        if not code:
            continue
        result[code] = {
            "name":        str(r.get("公司簡稱") or r.get("公司名稱") or ""),
            "industry":    str(r.get("產業類別") or r.get("產業別") or ""),
            "capital":     parse_num(r.get("實收資本額(元)") or r.get("實收資本額（元）")),
            "listed_date": str(r.get("上市日期") or ""),
        }
    return result


# ─────────────────────────────────────────────────────────────────
#  模組 7：重大公告
#  端點：/v1/opendata/t187ap04_L（上市公司每日重大訊息，中文欄位）
#  備用：/v1/news/newsList
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=1800, show_spinner=False)
def fetch_twse_announcements():
    """
    端點：/v1/opendata/t187ap04_L
    欄位（中文）：
      出表日期、發言日期、發言時間
      公司代號、公司名稱、主旨、說明
    """
    rows = fetch_twse_openapi("opendata/t187ap04_L")
    if not rows:
        rows = fetch_twse_openapi("news/newsList")
    result = []
    for r in rows[:40]:
        result.append({
            "date":  str(r.get("出表日期") or r.get("發言日期") or r.get("Date") or r.get("date") or ""),
            "code":  str(r.get("公司代號") or r.get("CompanyCode") or r.get("code") or ""),
            "name":  str(r.get("公司名稱") or r.get("CompanyName") or r.get("name") or ""),
            "title": str(r.get("主旨") or r.get("Title") or r.get("title") or r.get("Subject") or ""),
            "detail":str(r.get("說明") or ""),
            "url":   str(r.get("Url") or r.get("url") or r.get("URL") or ""),
        })
    return result


# ─────────────────────────────────────────────────────────────────
#  模組 8：成交量值排行
#  端點：/v1/exchangeReport/MI_INDEX20（中文欄位）
#  備用：/v1/exchangeReport/STOCK_DAY_ALL 自行排序
# ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=600, show_spinner=False)
def fetch_twse_top20_volume():
    """
    端點：/v1/exchangeReport/MI_INDEX20
    欄位（中文）：排名、證券代號、證券名稱、成交股數、成交筆數、成交金額、
                  開盤價、最高價、最低價、收盤價、漲跌(+/-)、漲跌價差
    備援：STOCK_DAY_ALL 英文欄位 TradeValue 排序
    """
    rows = fetch_twse_openapi("exchangeReport/MI_INDEX20")
    # 先嘗試中文欄位（MI_INDEX20）
    result = []
    for r in rows[:20]:
        code = str(r.get("證券代號") or r.get("Code") or "")
        name = str(r.get("證券名稱") or r.get("Name") or "")
        vol  = parse_num(r.get("成交股數") or r.get("TradeVolume"))
        val  = parse_num(r.get("成交金額") or r.get("TradeValue"))
        close= parse_num(r.get("收盤價") or r.get("ClosingPrice"))
        chg_dir = str(r.get("漲跌(+/-)") or r.get("漲跌") or "")
        chg_val = parse_num(r.get("漲跌價差") or r.get("Change"))
        if chg_val is not None and chg_dir == "-":
            chg_val = -abs(chg_val)
        if code or name:
            result.append({"code": code, "name": name, "volume": vol,
                           "value": val, "close": close, "change": chg_val})
    # 備援：STOCK_DAY_ALL（英文欄位），取成交值前 20
    if not result:
        day_rows = fetch_twse_openapi("exchangeReport/STOCK_DAY_ALL")
        parsed = []
        for r in day_rows:
            val = parse_num(r.get("TradeValue"))
            if val:
                parsed.append({
                    "code":   str(r.get("Code") or ""),
                    "name":   str(r.get("Name") or ""),
                    "volume": parse_num(r.get("TradeVolume")),
                    "value":  val,
                    "close":  parse_num(r.get("ClosingPrice")),
                    "change": parse_num(r.get("Change")),
                })
        parsed.sort(key=lambda x: x.get("value") or 0, reverse=True)
        result = parsed[:20]
    return result


def _http_headers(accept: str):
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
        "Accept": accept,
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
        "Referer": "https://www.twse.com.tw/",
        "Origin": "https://www.twse.com.tw",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
    }


def fetch_json_url(url: str):
    errors = []
    try:
        resp = requests.get(url, headers=_http_headers("application/json,text/plain,*/*"), timeout=15)
        if resp.ok and resp.text.strip():
            try:
                return json.loads(resp.text)
            except Exception as e:
                preview = resp.text[:400].replace("\n", " ")
                errors.append(f"requests-json-parse:{type(e).__name__}:{e}:preview={preview}")
        else:
            preview = (resp.text or "")[:400].replace("\n", " ")
            errors.append(f"requests-status:{resp.status_code}:preview={preview}")
    except Exception as e:
        errors.append(f"requests:{type(e).__name__}:{e}")

    try:
        req = Request(url, headers=_http_headers("application/json,text/plain,*/*"))
        with urlopen(req, timeout=15) as resp:
            payload = resp.read().decode("utf-8", errors="ignore")
        try:
            return json.loads(payload)
        except Exception as e:
            preview = payload[:400].replace("\n", " ")
            errors.append(f"urllib-json-parse:{type(e).__name__}:{e}:preview={preview}")
    except Exception as e:
        errors.append(f"urllib:{type(e).__name__}:{e}")
    return {"_fetch_error": " | ".join(errors), "_url": url}


@st.cache_data(ttl=300, show_spinner=False)
def fetch_text_url(url: str):
    errors = []
    try:
        resp = requests.get(url, headers=_http_headers("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"), timeout=15)
        if resp.ok and resp.text:
            return resp.text
        errors.append(f"requests-status:{resp.status_code}")
    except Exception as e:
        errors.append(f"requests:{type(e).__name__}:{e}")

    try:
        req = Request(url, headers=_http_headers("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"))
        with urlopen(req, timeout=15) as resp:
            return resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        errors.append(f"urllib:{type(e).__name__}:{e}")
    return "[FETCH_ERROR] " + " | ".join(errors)


def _extract_rows_from_rwd_json(data):
    rows = []
    if not isinstance(data, dict):
        return rows

    def append_rows(fields, data_rows, table_title=""):
        if not isinstance(fields, list) or not isinstance(data_rows, list):
            return
        for raw in data_rows:
            if isinstance(raw, list):
                row = {str(fields[i]): raw[i] if i < len(raw) else None for i in range(len(fields))}
                if table_title:
                    row["表名"] = table_title
                rows.append(row)
            elif isinstance(raw, dict):
                row = dict(raw)
                if table_title and "表名" not in row:
                    row["表名"] = table_title
                rows.append(row)

    append_rows(data.get("fields"), data.get("data"), data.get("title", ""))
    for table in data.get("tables", []) if isinstance(data.get("tables"), list) else []:
        if isinstance(table, dict):
            append_rows(table.get("fields"), table.get("data"), table.get("title", ""))
    return rows


def _html_to_text(html):
    text = re.sub(r'<script[\s\S]*?</script>', ' ', html, flags=re.I)
    text = re.sub(r'<style[\s\S]*?</style>', ' ', text, flags=re.I)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = htmllib.unescape(text)
    text = re.sub(r'\s+', ' ', text)
    return text


def _parse_number_near_keywords(text, keywords, max_gap=18):
    if not text:
        return None
    for kw in keywords:
        pattern = rf'{re.escape(kw)}[^0-9\-+]{{0,{max_gap}}}([+\-]?[0-9,]+(?:\.[0-9]+)?)'
        m = re.search(pattern, text, flags=re.I)
        if m:
            return parse_num(m.group(1))
    return None


def _parse_number_from_html_tables(html, keywords):
    if not html:
        return None
    try:
        tables = pd.read_html(StringIO(html))
    except Exception:
        tables = []
    for df in tables:
        try:
            df = df.fillna("").astype(str)
        except Exception:
            continue
        vals = df.values.tolist()
        for row in vals:
            for idx, cell in enumerate(row):
                cell_text = _normalize_label(cell)
                if any(kw in cell_text for kw in keywords):
                    for j in range(idx + 1, min(len(row), idx + 4)):
                        num = parse_num(row[j])
                        if num is not None:
                            return num
    text = _html_to_text(html)
    return _parse_number_near_keywords(text, keywords, max_gap=20)


def _merge_snapshot(base, extra):
    if not isinstance(extra, dict):
        return base
    for key in ["index_name", "index_value", "index_change", "market_amount", "market_shares", "up_count", "down_count"]:
        if base.get(key) is None and extra.get(key) is not None:
            base[key] = extra.get(key)
        elif key == "index_name" and base.get(key) == "加權指數" and extra.get(key):
            base[key] = extra.get(key)
    if extra.get("raw"):
        base["raw"].extend(extra.get("raw", []))
    return base


def _extract_tables_from_twse_payload(data):
    tables = []
    if not isinstance(data, dict):
        return tables

    for key in ["tables", "dataTables", "tableData", "reportData", "data"]:
        val = data.get(key)
        if isinstance(val, list):
            for item in val:
                if isinstance(item, dict):
                    title = item.get("title") or item.get("name") or item.get("tableName") or item.get("label") or ""
                    fields = item.get("fields") or item.get("columnNames") or item.get("columns") or []
                    rows = item.get("data") or item.get("rows") or item.get("items") or []
                    if isinstance(rows, list):
                        tables.append({"title": title, "fields": fields, "rows": rows})

    if not tables:
        fields = data.get("fields") or data.get("columnNames") or []
        rows = data.get("data") or data.get("rows") or []
        if isinstance(rows, list) and rows:
            tables.append({"title": data.get("title") or "", "fields": fields, "rows": rows})
    return tables


def _rows_from_tables(tables, source_tag=""):
    rows = []
    for table in tables:
        title = str(table.get("title") or "")
        fields = table.get("fields") or []
        raw_rows = table.get("rows") or []
        for raw in raw_rows:
            row = {}
            if isinstance(raw, list):
                for i, field in enumerate(fields):
                    row[str(field)] = raw[i] if i < len(raw) else None
            elif isinstance(raw, dict):
                row = {str(k): v for k, v in raw.items()}
            else:
                continue
            if title:
                row["表名"] = title
            if source_tag:
                row["來源"] = source_tag
            rows.append(row)
    return rows


def _find_row_value(rows, keywords):
    keys = [_normalize_label(k) for k in keywords]
    for row in rows:
        if not isinstance(row, dict):
            continue
        items = list(row.items())
        for idx, (k, v) in enumerate(items):
            k_norm = _normalize_label(k)
            v_norm = _normalize_label(v)
            if not any(kw in k_norm or kw in v_norm for kw in keys):
                continue
            direct = parse_num(v)
            if direct is not None:
                return direct
            for _, nxt in items[idx + 1:]:
                num = parse_num(nxt)
                if num is not None:
                    return num
            nums = [parse_num(x) for _, x in items]
            nums = [x for x in nums if x is not None]
            if nums:
                return nums[0]
    return None


def _find_index_change_from_rows(rows):
    for row in rows:
        if not isinstance(row, dict):
            continue
        items = list(row.items())
        row_text = " ".join(str(v) for _, v in items)
        if "發行量加權股價指數" in row_text or "加權指數" in row_text:
            nums = [parse_num(x) for _, x in items]
            nums = [x for x in nums if x is not None]
            if len(nums) >= 2:
                return nums[1]
    return _find_row_value(rows, ["漲跌點數", "漲跌指數", "漲跌"])


def _snapshot_from_twse_json_recent(days_back=14):
    """
    V101：不再直接呼叫 www.twse.com.tw/rwd（SSL 問題），
    改由 get_twse_market_snapshot_v2() 統一走 openapi.twse.com.tw/v1。
    保留此函數名稱以確保既有呼叫點不壞。
    """
    v2 = get_twse_market_snapshot_v2()
    return {
        "index_name": v2.get("index_name", "加權指數"),
        "index_value": v2.get("index_value"),
        "index_change": v2.get("index_change"),
        "market_amount": v2.get("market_amount"),
        "market_shares": v2.get("market_shares"),
        "up_count": v2.get("up_count"),
        "down_count": v2.get("down_count"),
        "raw": v2.get("raw", []),
        "source": v2.get("source_endpoint", "TWSE_OPENAPI_V1"),
        "effective_date": v2.get("effective_date"),
        "debug": [],
    }


def _snapshot_from_twse_html_recent(days_back=14):
    snapshot = {
        "index_name": "加權指數",
        "index_value": None,
        "index_change": None,
        "market_amount": None,
        "market_shares": None,
        "up_count": None,
        "down_count": None,
        "raw": [],
        "source": None,
        "effective_date": None,
    }

    for delta in range(days_back + 1):
        d = datetime.now() - timedelta(days=delta)
        date_str = d.strftime("%Y%m%d")
        url = f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=html&date={date_str}&type=ALLBUT0999"
        html = fetch_text_url(url)
        if not html:
            continue
        try:
            tables = pd.read_html(StringIO(html))
        except Exception:
            tables = []
        raw_rows = []
        for ti, df in enumerate(tables):
            try:
                df2 = df.copy().fillna("").astype(str)
            except Exception:
                continue
            raw_rows.append({
                "來源": "TWSE_HTML",
                "日期": date_str,
                "表序": ti,
                "欄位": " | ".join(map(str, df2.columns.tolist()[:12])),
                "前兩列": " || ".join(" | ".join(map(str, r[:8])) for r in df2.values.tolist()[:2])
            })
            rows = []
            cols = [str(c) for c in df2.columns]
            for raw in df2.values.tolist():
                row = {cols[i]: raw[i] if i < len(raw) else None for i in range(len(cols))}
                rows.append(row)
            snapshot["index_value"] = snapshot["index_value"] or _find_row_value(rows, ["發行量加權股價指數", "加權指數", "收盤指數"])
            snapshot["index_change"] = snapshot["index_change"] or _find_index_change_from_rows(rows)
            snapshot["market_amount"] = snapshot["market_amount"] or _find_row_value(rows, ["成交金額", "成交值", "總成交金額"])
            snapshot["market_shares"] = snapshot["market_shares"] or _find_row_value(rows, ["成交股數", "成交數量", "成交量", "總成交股數"])
            snapshot["up_count"] = snapshot["up_count"] or _find_row_value(rows, ["上漲家數", "上漲(漲停)"])
            snapshot["down_count"] = snapshot["down_count"] or _find_row_value(rows, ["下跌家數", "下跌(跌停)"])

        if raw_rows:
            snapshot["raw"] = raw_rows[:80]
            snapshot["source"] = "TWSE_HTML"
            snapshot["effective_date"] = date_str
        if snapshot["index_value"] is not None or snapshot["market_amount"] is not None or snapshot["up_count"] is not None:
            return snapshot

    return snapshot


def _snapshot_from_twse_homepage_text():
    snapshot = {
        "index_name": "加權指數",
        "index_value": None,
        "index_change": None,
        "market_amount": None,
        "market_shares": None,
        "up_count": None,
        "down_count": None,
        "raw": [],
        "source": None,
        "effective_date": None,
    }
    html = fetch_text_url("https://www.twse.com.tw/zh/") or fetch_text_url("https://www.twse.com.tw/")
    if not html:
        return snapshot
    text = _html_to_text(html)
    snapshot["raw"] = [{"來源": "TWSE首頁", "前四百字": text[:400]}]
    snapshot["source"] = "TWSE_HOME"
    m = re.search(r"(?:加權指數|TAIEX)\s*([0-9,]+(?:\.[0-9]+)?)", text)
    if m:
        snapshot["index_value"] = parse_num(m.group(1))
    m = re.search(r"成交金額[^0-9]{0,10}([0-9,]+(?:\.[0-9]+)?)", text)
    if m:
        val = parse_num(m.group(1))
        if val is not None:
            snapshot["market_amount"] = val * 100000000 if val < 1000000 else val
    m = re.search(r"成交股數[^0-9]{0,10}([0-9,]+(?:\.[0-9]+)?)", text)
    if m:
        val = parse_num(m.group(1))
        if val is not None:
            snapshot["market_shares"] = val * 100000000 if val < 1000000 else val
    return snapshot


def get_twse_market_snapshot():
    """V101：直接回傳 OpenAPI 版快照，不再走舊 RWD 路徑。"""
    return get_twse_market_snapshot_v2()


def format_amount_yi(value):
    if value is None:
        return "--"
    try:
        return f"{value / 100000000:.2f} 億"
    except Exception:
        return str(value)


def format_amount_yi_shares(value):
    if value is None:
        return "--"
    try:
        return f"{value / 100000000:.2f} 億股"
    except Exception:
        return str(value)


def _parse_rss_feed(url: str, source_name: str, max_items: int = 8) -> list[dict]:
    """抓取 RSS XML 並解析成標準格式，失敗回傳空 list。"""
    import xml.etree.ElementTree as ET
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if not resp.ok:
            return []
        root = ET.fromstring(resp.content)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        items = []
        # 標準 RSS 2.0
        for item in root.findall(".//item")[:max_items]:
            title = (item.findtext("title") or "").strip()
            link  = (item.findtext("link")  or "").strip()
            pub   = (item.findtext("pubDate") or "").strip()
            if not title:
                continue
            time_str = ""
            if pub:
                try:
                    from email.utils import parsedate_to_datetime
                    dt = parsedate_to_datetime(pub)
                    tw_tz = ZoneInfo("Asia/Taipei")
                    time_str = dt.astimezone(tw_tz).strftime("%m/%d %H:%M")
                except Exception:
                    time_str = pub[:16]
            items.append({"title": title, "link": link, "publisher": source_name, "time": time_str})
        # Atom feed 備援
        if not items:
            for entry in root.findall(".//atom:entry", ns)[:max_items]:
                title = (entry.findtext("atom:title", namespaces=ns) or "").strip()
                link_el = entry.find("atom:link", ns)
                link = (link_el.get("href", "") if link_el is not None else "").strip()
                pub = (entry.findtext("atom:updated", namespaces=ns) or "").strip()
                if not title:
                    continue
                time_str = pub[:16].replace("T", " ") if pub else ""
                items.append({"title": title, "link": link, "publisher": source_name, "time": time_str})
        return items
    except Exception:
        return []


@st.cache_data(ttl=300, show_spinner=False)
def fetch_tw_stock_news() -> list[dict]:
    """
    雙來源台股新聞：
    1. 鉅亨網台股新聞 RSS（主要）
    2. Yahoo 奇摩股市 RSS（備援）
    5 分鐘快取，按需更新，不頻繁重抓。
    """
    # 來源 A：鉅亨網台股
    items = _parse_rss_feed(
        "https://www.cnyes.com/rss/cat/tw_stock",
        source_name="鉅亨網",
    )
    # 來源 B：Yahoo 奇摩股市（備援）
    if not items:
        items = _parse_rss_feed(
            "https://tw.stock.yahoo.com/rss",
            source_name="Yahoo 奇摩股市",
        )
    return items


@st.cache_data(ttl=900, show_spinner=False)
def get_stock_news(symbol: str):
    """個股新聞（單股詳細頁用），保留原有 yfinance 路徑。"""
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


TAIEX_YAHOO_FALLBACK_NOTICE = (
    "在「靜態收盤基準日」對齊前提下，本輪未能採用證交所官方加權收盤，"
    "因此暫以 Yahoo ^TWII **日線收盤**備援（非盤中即時），數值可能與正式收盤略有差異。"
)

# 台股國定假日等：可逐年擴充；未列入者僅以週末判斷非交易日
_TWSE_EXTRA_HOLIDAYS: frozenset[date] = frozenset()


def _is_twse_trading_day(d: date) -> bool:
    if d.weekday() >= 5:
        return False
    return d not in _TWSE_EXTRA_HOLIDAYS


def _previous_twse_trading_day(d: date, max_scan: int = 400) -> date:
    cur = d - timedelta(days=1)
    for _ in range(max_scan):
        if _is_twse_trading_day(cur):
            return cur
        cur -= timedelta(days=1)
    return d - timedelta(days=1)


def _taiex_static_display_context(now_tpe: datetime | None = None) -> dict:
    """
    靜態加權基準：交易日上午 9:00～13:30 顯示「上一交易日」收盤；
    該日 13:30 後至次一交易日 09:00 前顯示「當日（剛收盤之交易日）」收盤；
    非交易日全日沿用最近交易日收盤（與收盤後視窗相同語意）。
    """
    tpe = ZoneInfo("Asia/Taipei")
    nt = now_tpe or datetime.now(tpe)
    if nt.tzinfo is None:
        nt = nt.replace(tzinfo=tpe)
    else:
        nt = nt.astimezone(tpe)
    d = nt.date()
    t = nt.time()
    open_t = time(9, 0)
    sess_end = time(13, 30)
    if _is_twse_trading_day(d):
        if open_t <= t < sess_end:
            anchor = _previous_twse_trading_day(d)
            label = "上一交易日收盤加權"
            mode_key = "prev_session"
        elif t >= sess_end:
            anchor = d
            label = "當日收盤加權"
            mode_key = "same_day_close"
        else:
            anchor = _previous_twse_trading_day(d)
            label = "當日收盤加權"
            mode_key = "overnight_last_close"
    else:
        anchor = _previous_twse_trading_day(d)
        label = "當日收盤加權"
        mode_key = "non_trading_day"
    caption = f"**{label}**（基準日 {anchor.isoformat()}）"
    return {
        "anchor_date": anchor,
        "label": label,
        "mode_key": mode_key,
        "caption": caption,
    }


def _parse_twse_calendar_date(s: str) -> date | None:
    """
    將證交所常見日期字串轉成西元 date。
    支援：1150408（民國115年04月08日）、20260408（西元）、114/04/08、114年4月8日 等。
    """
    if s is None:
        return None
    raw = str(s).strip()
    if not raw:
        return None

    digits_only = re.sub(r"\D", "", raw)
    # 西元 YYYYMMDD（8 碼）
    if len(digits_only) == 8:
        try:
            y = int(digits_only[:4])
            mo = int(digits_only[4:6])
            da = int(digits_only[6:8])
            if y >= 1900:
                return date(y, mo, da)
        except ValueError:
            pass
    # 民國 YYYMMDD（7 碼，例如 1150408 → 2026-04-08）
    if len(digits_only) == 7:
        try:
            roc_y = int(digits_only[:3])
            mo = int(digits_only[3:5])
            da = int(digits_only[5:7])
            gy = roc_y + 1911
            return date(gy, mo, da)
        except ValueError:
            pass
    # 民國 YYMMDD（6 碼，較少見）
    if len(digits_only) == 6:
        try:
            roc_y = int(digits_only[:2])
            mo = int(digits_only[2:4])
            da = int(digits_only[4:6])
            gy = roc_y + 1911
            return date(gy, mo, da)
        except ValueError:
            pass

    norm = raw.replace("年", "/").replace("月", "/").replace("日", "").replace("-", "/")
    nums = re.findall(r"\d+", norm)
    if len(nums) >= 3:
        y, mo, da = int(nums[0]), int(nums[1]), int(nums[2])
        if y < 400:
            y += 1911
        try:
            return date(y, mo, da)
        except ValueError:
            return None
    return None


def _pack_taiex_resolve_debug(extra: dict | None) -> dict:
    if not extra:
        return {}
    return {
        "base_date": extra.get("base_date"),
        "raw_effective_date": extra.get("raw_effective_date"),
        "normalized_effective_date": extra.get("normalized_effective_date"),
        "date_match_result": extra.get("date_match_result"),
        "fallback_trigger_reason": extra.get("fallback_trigger_reason"),
    }


def _index_row_date_tpe(ts) -> date:
    ts_dt = pd.Timestamp(ts)
    if ts_dt.tzinfo is not None:
        return ts_dt.tz_convert(ZoneInfo("Asia/Taipei")).date()
    return ts_dt.date()


def _slice_twii_through_anchor(df: pd.DataFrame, anchor: date) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    mask = []
    for idx in df.index:
        try:
            mask.append(_index_row_date_tpe(idx) <= anchor)
        except Exception:
            mask.append(True)
    sub = df.loc[mask]
    return sub if sub is not None and not sub.empty else df.iloc[0:0]


def _close_from_yahoo_daily_for_date(df: pd.DataFrame, anchor: date) -> float | None:
    """取 Yahoo 日線中「基準日」當日收盤；尚無該日棒時改取不晚於基準日之最近一筆（仍為日線收盤）。"""
    if df is None or df.empty or "Close" not in df.columns:
        return None
    exact = None
    best_d: date | None = None
    best_c = None
    for i in range(len(df)):
        d = _index_row_date_tpe(df.index[i])
        try:
            c = float(df["Close"].iloc[i])
        except Exception:
            continue
        if d == anchor:
            exact = c
        if d <= anchor and c >= 5000 and (best_d is None or d > best_d):
            best_d = d
            best_c = c
    if exact is not None and exact >= 5000:
        return float(exact)
    if best_c is not None:
        return float(best_c)
    return None


def _resolve_taiex_static(anchor_date: date, yahoo_df: pd.DataFrame, tw_pack: dict | None = None) -> tuple[float, str, dict]:
    """手動 > 證交所 OpenAPI（effective_date 正規化後須與基準日一致）> Yahoo 日線；日期已對齊時不得改走 Yahoo。"""
    if tw_pack is None:
        tw_pack = fetch_twse_market_index()
    raw_eff = tw_pack.get("effective_date")
    ed = _parse_twse_calendar_date(str(raw_eff or ""))
    twse_date_matches = ed is not None and ed == anchor_date

    extra: dict = {
        "twse_market": tw_pack,
        "anchor_date": anchor_date.isoformat(),
        "base_date": anchor_date.isoformat(),
        "raw_effective_date": raw_eff,
        "normalized_effective_date": ed.isoformat() if ed else None,
        "date_match_result": twse_date_matches,
        "fallback_trigger_reason": None,
    }

    try:
        man = float(st.session_state.get("manual_taiex_override") or 0)
    except (TypeError, ValueError):
        man = 0.0
    if man >= 5000:
        extra["fallback_trigger_reason"] = "手動覆寫加權指數（略過證交所／Yahoo）"
        return man, "manual", extra

    iv = tw_pack.get("index_value")
    ivf = None
    if iv is not None:
        try:
            ivf = float(iv)
        except (TypeError, ValueError):
            ivf = None

    if ivf is not None and ivf >= 5000 and twse_date_matches:
        extra["date"] = raw_eff
        extra["fallback_trigger_reason"] = "無（證交所資料日與靜態基準日一致，採用 OpenAPI）"
        return ivf, "twse", extra

    if ivf is not None and ivf >= 5000 and ed is None:
        extra["fallback_trigger_reason"] = (
            f"證交所 index_value 有效但 effective_date 無法正規化（原文：{raw_eff!r}），改採 Yahoo 日線"
        )
    elif ivf is not None and ivf >= 5000:
        extra["fallback_trigger_reason"] = (
            f"證交所資料日 {ed} 與靜態基準日 {anchor_date} 不一致，改採 Yahoo 日線"
        )
    elif ivf is None or ivf < 5000:
        extra["fallback_trigger_reason"] = "證交所 index_value 缺失或低於門檻（<5000），改採 Yahoo 日線"

    y = _close_from_yahoo_daily_for_date(yahoo_df, anchor_date)
    if y is not None and y >= 5000:
        extra["static_fallback_note"] = extra.get("fallback_trigger_reason") or "Yahoo ^TWII 日線備援"
        return float(y), "yahoo", extra

    if ivf is not None and ivf >= 5000:
        extra["date"] = raw_eff
        extra["fallback_trigger_reason"] = (
            (extra.get("fallback_trigger_reason") or "") + "；Yahoo 無對應日線，暫用 OpenAPI 數值（請核對日期）"
        )
        return ivf, "twse", extra

    extra["fallback_trigger_reason"] = extra.get("fallback_trigger_reason") or "無可用加權收盤"
    return 0.0, "yahoo", extra


def render_market_taiex_diagnostics(market_info: dict):
    """市場概況：加權來源＋TWSE 偵錯（含 OpenAPI 層級錯誤）。"""
    tw = market_info.get("twse_market_probe") or {}
    dbg = tw.get("debug") or market_info.get("taiex_debug") or {}
    if not tw and not dbg:
        return
    src = market_info.get("taiex_source")
    mi = dbg.get("mi_openapi") or {}
    iv_raw = tw.get("index_value")
    eff = tw.get("effective_date") or dbg.get("effective_date")

    if src == "manual":
        st.success("**加權收盤來源：手動覆寫**（側欄「加權指數來源／手動修正」）。證交所若連線失敗不影響目前顯示數字。")
    elif src == "twse":
        st.info(f"**加權收盤來源：證交所 OpenAPI**（effective_date：**{eff or '—'}**；index_value：**{iv_raw}**）。")
    elif src == "yahoo":
        st.warning(TAIEX_YAHOO_FALLBACK_NOTICE)
    elif src is None:
        st.caption("**大盤 K 線資料不足**，以下仍顯示本次證交所 MI_INDEX 連線偵錯。")

    ad = market_info.get("taiex_anchor_date")
    ad_s = ad.isoformat() if hasattr(ad, "isoformat") else (ad or "—")
    trd = market_info.get("taiex_static_resolve_debug") or {}
    lines = [
        f"- **靜態基準**：{market_info.get('taiex_static_label') or '—'}（基準日 `{ad_s}`）",
        f"- **base_date（靜態基準日）**：`{trd.get('base_date') or '—'}`",
        f"- **raw_effective_date（TWSE 原文）**：`{trd.get('raw_effective_date')!r}`",
        f"- **normalized_effective_date**：`{trd.get('normalized_effective_date') or '—'}`",
        f"- **date_match_result**：`{trd.get('date_match_result')}`",
        f"- **fallback_trigger_reason**：{trd.get('fallback_trigger_reason') or '—'}",
        f"- **taiex_source**：`{src}`",
        f"- **TWSE MI_INDEX rows 筆數**：{dbg.get('twse_rows_count', '—')}",
        f"- **TWSE index_value（解析後）**：{iv_raw}",
        f"- **effective_date**：{eff or '—'}",
        f"- **chosen_index_name**：{dbg.get('chosen_index_name')!r}",
        f"- **chosen_index_value_raw**：{dbg.get('chosen_index_value_raw')!r}",
        f"- **chosen_index_value_parsed**：{dbg.get('chosen_index_value_parsed')}",
        f"- **source_reason**：{dbg.get('source_reason') or '—'}",
        f"- **reject_reason**：{dbg.get('reject_reason') or '—'}",
        f"- **best_prio**：{dbg.get('best_prio')}",
        f"- **fallback_row_index**：{dbg.get('fallback_row_index')}",
    ]
    openapi_status = mi.get("final_status")
    lines.append(f"- **OpenAPI final_status**：`{openapi_status}`（HTTP {mi.get('requests_http_status', '—')}）")
    if mi.get("requests_error"):
        lines.append(f"- **requests 錯誤**：{mi.get('requests_error')}")
    if mi.get("urllib_error"):
        lines.append(f"- **urllib 錯誤**：{mi.get('urllib_error')}")
    if mi.get("json_error"):
        lines.append(f"- **JSON 錯誤**：{mi.get('json_error')}")
    if mi.get("response_empty"):
        lines.append("- **response_empty**：True")

    _openapi_bad = mi.get("final_status") not in ("ok", "ok_urllib")
    with st.expander(
        "加權指數／TWSE OpenAPI 偵錯詳情",
        expanded=False,  # 預設收起；有問題時請手動展開查看
    ):
        st.markdown("\n".join(lines))
        if mi:
            st.caption("以下為本次 MI_INDEX 之 `fetch_twse_openapi` 完整偵錯（含 body 預覽）。")
            st.json(mi)
        if dbg.get("first_row_keys_sample"):
            st.caption("MI_INDEX 第一筆欄位鍵名（供對照欄位是否改版）：")
            st.code(", ".join(dbg["first_row_keys_sample"]))


def _patch_twii_dataframe_close(df: pd.DataFrame, resolved_close: float) -> pd.DataFrame:
    """將最後一日 Close（必要時連動 High/Low）改為官方／手動加權收盤，以利均線與濾網一致。"""
    if df is None or df.empty or "Close" not in df.columns:
        return df
    try:
        y = float(df["Close"].iloc[-1])
    except Exception:
        return df
    try:
        rc = float(resolved_close)
    except (TypeError, ValueError):
        return df
    if abs(rc - y) < 1e-4:
        return df
    out = df.copy()
    ci = out.columns.get_loc("Close")
    out.iloc[-1, ci] = rc
    if "High" in out.columns:
        hi = out.columns.get_loc("High")
        h = float(out.iloc[-1, hi])
        if rc > h:
            out.iloc[-1, hi] = rc
    if "Low" in out.columns:
        lo = out.columns.get_loc("Low")
        l = float(out.iloc[-1, lo])
        if rc < l:
            out.iloc[-1, lo] = rc
    return out


def _ensure_twii_bar_through_anchor(df: pd.DataFrame, anchor: date, resolved_close: float) -> pd.DataFrame:
    """僅用日線：截斷至基準日，必要時補上基準日單棒，並將該日 Close 對齊解析後之靜態收盤。"""
    sub = _slice_twii_through_anchor(df, anchor)
    if sub is None or sub.empty:
        return df
    try:
        rc = float(resolved_close)
    except (TypeError, ValueError):
        return sub
    if rc < 5000:
        return sub
    try:
        last_d = _index_row_date_tpe(sub.index[-1])
    except Exception:
        return sub
    out = sub.copy()
    if last_d < anchor:
        bar = pd.Timestamp(datetime.combine(anchor, time(0, 0)))
        out.loc[bar, ["Open", "High", "Low", "Close", "Volume"]] = [rc, rc, rc, rc, 0.0]
        out = out.sort_index()
        return normalize_df(out)
    return _patch_twii_dataframe_close(out, rc)


def market_filter():
    tctx = _taiex_static_display_context()
    anchor: date = tctx["anchor_date"]
    tw_pack = fetch_twse_market_index()
    df = get_market_data()
    if df.empty or len(df) < 20:
        raw_eff = tw_pack.get("effective_date")
        ed = _parse_twse_calendar_date(str(raw_eff or ""))
        preview_dbg = {
            "base_date": anchor.isoformat(),
            "raw_effective_date": raw_eff,
            "normalized_effective_date": ed.isoformat() if ed else None,
            "date_match_result": bool(ed is not None and ed == anchor),
            "fallback_trigger_reason": "大盤 ^TWII 日線不足（<20 根），尚未完成來源決定",
        }
        return {
            "label": "資料不足",
            "score_adj": 0,
            "text": "大盤資料不足。",
            "close": None,
            "taiex_source": None,
            "twse_market_probe": tw_pack,
            "taiex_debug": tw_pack.get("debug"),
            "taiex_yahoo_notice": None,
            "taiex_static_caption": tctx["caption"],
            "taiex_static_label": tctx["label"],
            "taiex_anchor_date": anchor,
            "taiex_mode_key": tctx["mode_key"],
            "taiex_static_resolve_debug": preview_dbg,
        }
    resolved, src, extra = _resolve_taiex_static(anchor, df, tw_pack)
    resolve_dbg = _pack_taiex_resolve_debug(extra)
    tws = extra.get("twse_market") or tw_pack
    df = _ensure_twii_bar_through_anchor(df, anchor, resolved)
    if df.empty or len(df) < 20:
        return {
            "label": "資料不足",
            "score_adj": 0,
            "text": "大盤資料不足。",
            "close": None,
            "taiex_source": None,
            "twse_market_probe": tw_pack,
            "taiex_debug": tw_pack.get("debug"),
            "taiex_yahoo_notice": None,
            "taiex_static_caption": tctx["caption"],
            "taiex_static_label": tctx["label"],
            "taiex_anchor_date": anchor,
            "taiex_mode_key": tctx["mode_key"],
            "taiex_static_resolve_debug": {**resolve_dbg, "fallback_trigger_reason": "截斷／補棒後日線不足"},
        }
    df = indicators(df)
    eff_close = float(resolved) if resolved >= 5000 else float(df["Close"].iloc[-1])
    close = round(eff_close, 2) if eff_close >= 5000 else None
    ma20 = float(df["ma20"].iloc[-1]) if pd.notna(df["ma20"].iloc[-1]) else eff_close
    ma5 = float(df["ma5"].iloc[-1]) if pd.notna(df["ma5"].iloc[-1]) else eff_close
    prev5 = float(df["Close"].iloc[-5]) if len(df) >= 5 else eff_close
    if src == "manual":
        src_hint = "加權收盤：手動覆寫。"
    elif src == "twse":
        src_hint = f"加權收盤：證交所 OpenAPI（資料日 {extra.get('date') or '—'}）。"
    else:
        src_hint = "加權收盤：Yahoo ^TWII 日線（備援）。"
    if extra.get("static_fallback_note"):
        src_hint = f"{src_hint} {extra['static_fallback_note']}"
    base = {
        "close": close,
        "taiex_source": src,
        "taiex_hint": src_hint,
        "twse_market_probe": tws,
        "taiex_debug": tws.get("debug") if isinstance(tws, dict) else {},
        "taiex_yahoo_notice": TAIEX_YAHOO_FALLBACK_NOTICE if src == "yahoo" else None,
        "taiex_static_caption": tctx["caption"],
        "taiex_static_label": tctx["label"],
        "taiex_anchor_date": anchor,
        "taiex_mode_key": tctx["mode_key"],
        "taiex_static_resolve_debug": resolve_dbg,
    }
    if eff_close > ma20 and ma5 >= ma20 and eff_close > prev5:
        return {**base, "label": "偏多", "score_adj": 8, "text": "大盤站上月線且短線偏強。"}
    if eff_close < ma20 and ma5 < ma20 and eff_close < prev5:
        return {**base, "label": "偏空", "score_adj": -10, "text": "大盤偏弱，建議保守。"}
    return {**base, "label": "中性", "score_adj": 0, "text": "大盤方向不明，宜選強不選弱。"}


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

def build_operation_rating_breakdown(df: pd.DataFrame, row: dict, market_info: dict | None = None):
    market_info = market_info or {}
    close = float(df["Close"].iloc[-1]) if not df.empty else float(row.get("收盤", 0) or 0)
    prev5 = float(df["Close"].iloc[-5]) if len(df) >= 5 else close
    prev10 = float(df["Close"].iloc[-10]) if len(df) >= 10 else close
    ma5 = float(df["ma5"].iloc[-1]) if ("ma5" in df.columns and pd.notna(df["ma5"].iloc[-1])) else close
    ma20 = float(df["ma20"].iloc[-1]) if ("ma20" in df.columns and pd.notna(df["ma20"].iloc[-1])) else close
    vol = float(df["Volume"].iloc[-1]) if ("Volume" in df.columns and not df.empty) else 0
    vol5 = float(df["vol5"].iloc[-1]) if ("vol5" in df.columns and pd.notna(df["vol5"].iloc[-1])) else vol
    rsi = float(df["rsi"].iloc[-1]) if ("rsi" in df.columns and pd.notna(df["rsi"].iloc[-1])) else 50
    k = float(df["k"].iloc[-1]) if ("k" in df.columns and pd.notna(df["k"].iloc[-1])) else 50
    d = float(df["d"].iloc[-1]) if ("d" in df.columns and pd.notna(df["d"].iloc[-1])) else 50

    base_items = []
    def add_item(name, score, condition, note):
        base_items.append({"項目": name, "加減分": score if condition else 0, "判斷": note if condition else f"未達：{note}"})

    add_item("站上月線", 18, close > ma20, f"收盤 {close:.2f} {'>' if close > ma20 else '<='} MA20 {ma20:.2f}")
    add_item("站上5日線", 10, close > ma5, f"收盤 {close:.2f} {'>' if close > ma5 else '<='} MA5 {ma5:.2f}")
    add_item("高於5日前", 10, close > prev5, f"收盤 {close:.2f} {'>' if close > prev5 else '<='} 5日前 {prev5:.2f}")
    add_item("高於10日前", 6, close > prev10, f"收盤 {close:.2f} {'>' if close > prev10 else '<='} 10日前 {prev10:.2f}")
    add_item("量能大於5日均量", 14, vol > vol5, f"成交量 {vol:,.0f} {'>' if vol > vol5 else '<='} 均量 {vol5:,.0f}")
    add_item("RSI健康區", 8, 45 <= rsi <= 75, f"RSI {rsi:.1f} 落在 45~75")
    add_item("KD偏多", 5, k > d, f"K {k:.1f} {'>' if k > d else '<='} D {d:.1f}")

    base_score = int(sum(item["加減分"] for item in base_items))
    market_adj = int(market_info.get("score_adj", 0) or 0)
    market_label = str(market_info.get("label", "中性"))

    extra_items = []
    breakout_status = str(row.get("突破狀態", ""))
    breakout_strength = str(row.get("突破強度", ""))
    rr = float(row.get("風報比", 0) or 0)
    bias = str(row.get("盤前建議", row.get("盤前建議", "")) or "")

    if market_adj != 0:
        extra_items.append({"項目": "大盤加權", "加減分": market_adj, "判斷": f"大盤濾網：{market_label}"})
    else:
        extra_items.append({"項目": "大盤加權", "加減分": 0, "判斷": f"大盤濾網：{market_label}"})

    if breakout_status == "已突破":
        extra_items.append({"項目": "突破狀態", "加減分": 12, "判斷": "已突破"})
    elif breakout_status == "假突破風險":
        extra_items.append({"項目": "突破狀態", "加減分": -18, "判斷": "假突破風險"})
    else:
        extra_items.append({"項目": "突破狀態", "加減分": -4, "判斷": breakout_status or "尚未突破"})

    if breakout_strength == "強":
        extra_items.append({"項目": "突破強度", "加減分": 6, "判斷": breakout_strength})
    elif breakout_strength == "弱":
        extra_items.append({"項目": "突破強度", "加減分": -6, "判斷": breakout_strength})
    else:
        extra_items.append({"項目": "突破強度", "加減分": 0, "判斷": breakout_strength or "普通"})

    if rr >= 2.0:
        rr_score = 8
    elif rr >= 1.5:
        rr_score = 4
    elif rr < 1.0:
        rr_score = -8
    else:
        rr_score = 0
    extra_items.append({"項目": "風報比", "加減分": rr_score, "判斷": f"風報比 {rr:.2f}"})

    if bias == "偏多（可進場）":
        bias_score = 5
    elif bias == "偏空（觀望）":
        bias_score = -8
    else:
        bias_score = 0
    extra_items.append({"項目": "盤前偏向", "加減分": bias_score, "判斷": bias or "中性（等待確認）"})

    inst_score = int(row.get("法人分數", 0) or 0)
    inst_direction = str(row.get("法人方向", "資料不足") or "資料不足")
    inst_ratio = row.get("法人佔成交量比%", None)
    inst_ratio_text = f" / 佔量 {float(inst_ratio):.2f}%" if inst_ratio not in [None, "", "--"] else ""
    extra_items.append({
        "項目": "三大法人",
        "加減分": inst_score,
        "判斷": f"{inst_direction}{inst_ratio_text}｜{row.get('法人共振', '中性')}"
    })

    pre_adjusted_score = max(0, min(100, base_score + market_adj + inst_score))
    raw_total = base_score + market_adj + sum(x["加減分"] for x in extra_items[1:])
    final_score = max(0, min(100, raw_total))
    star, action = decision_star_and_action(pre_adjusted_score, breakout_status, breakout_strength, rr, bias)

    return {
        "base_items": base_items,
        "extra_items": extra_items,
        "base_score": base_score,
        "market_adj": market_adj,
        "pre_adjusted_score": pre_adjusted_score,
        "raw_total": raw_total,
        "final_score": final_score,
        "star": star,
        "action": action,
    }


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

    reasons = []
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
        return "價跌量縮", "黃燈", ["價格回落且量能收斂", "偏空但動能不足／跌勢可能趨緩", "空方當沖不宜作主要依據，宜觀察"]

    if gap_down and price_down:
        return "跳空下跌", "紅燈", ["開盤跳空轉弱", "市場承接偏弱", "先保守看待"]

    return "量價中性", "黃燈", ["價量沒有明顯共振", "暫以觀察為主", "等待更多確認"]

def build_judgement_reasons(row: dict):
    reasons = []
    pv = row.get("價量結論", "")
    if pv == "價跌量縮":
        reasons.append("價量：價跌量縮（偏空但動能不足／跌勢可能趨緩，不宜作空方當沖主依據）")
    elif pv == "價漲量縮":
        reasons.append(
            "價量：價漲量縮（黃燈）— 價格上漲但量能未跟上，屬「續航待確認、不宜過度追價」，"
            "不是綠燈強勢訊號；與下方 KD「加分」不同，此條不代表自動挑股評分已額外加點。"
        )
    elif pv == "價漲量增":
        reasons.append("價量：價漲量增（綠燈）— 價量同步轉強，偏多解讀。")
    elif pv == "價跌量增":
        reasons.append("價量：價跌量增（紅燈）— 下跌伴放量，賣壓偏強。")
    elif pv in ("開高走低爆量", "跳空上漲爆量", "跳空下跌", "量價中性"):
        reasons.append(f"價量：{pv}（詳見價量燈號與摘要）")
    elif pv:
        reasons.append(f"價量：{pv}")
    if row.get("壓力驗證", "") == "有效突破":
        reasons.append("壓力有效突破，加分")
    elif row.get("壓力驗證", "") == "遇壓回落":
        reasons.append("遇壓回落，扣分")
    if row.get("支撐驗證", "") == "跌破支撐":
        reasons.append("跌破支撐，扣分")
    elif row.get("支撐驗證", "") == "守住支撐":
        reasons.append("守住支撐，維持結構")
    if row.get("停損觸發", "") == "已觸發":
        reasons.append("停損已觸發，扣分")
    try:
        kd_k = float(row.get("KD_K", 0)); kd_d = float(row.get("KD_D", 0))
        if kd_k > kd_d:
            reasons.append("KD 黃金交叉/偏多，加分")
        else:
            reasons.append("KD 未轉強，保守看待")
    except Exception:
        pass
    try:
        rr = float(row.get("風報比", 0))
        if rr >= 1.8:
            reasons.append("風報比達標，加分")
        elif rr < 1.2:
            reasons.append("風報比偏弱，扣分")
    except Exception:
        pass
    inst_dir = str(row.get("法人方向", "") or "")
    inst_sum = row.get("三大法人買賣超", None)
    if inst_dir in ["偏多", "小偏多"]:
        reasons.append(f"三大法人偏多{f'（{fmt_lots(inst_sum)}）' if fmt_lots(inst_sum) != '--' else ''}")
    elif inst_dir in ["偏空", "小偏空"]:
        reasons.append(f"三大法人偏空{f'（{fmt_lots(inst_sum)}）' if fmt_lots(inst_sum) != '--' else ''}")
    if not reasons:
        reasons.append("目前沒有足夠理由，暫以觀察處理")
    return reasons[:4]

def render_reason_block(row: dict, title="判斷理由"):
    reasons = build_judgement_reasons(row)
    html = "".join(f"<li>{html_escape(x)}</li>" for x in reasons)
    st.markdown(f"""
    <div style="border:1px solid #334155;border-radius:12px;padding:12px 14px;background:#0b1220;margin-top:8px;">
      <div style="font-size:15px;font-weight:700;color:#e2e8f0;margin-bottom:6px;">{html_escape(title)}</div>
      <ul style="margin:0;padding-left:18px;color:#cbd5e1;line-height:1.7;">{html}</ul>
    </div>
    """, unsafe_allow_html=True)

def compare_price_change(pre_close, post_close):
    try:
        pre = float(pre_close); post = float(post_close)
        if post > pre:
            return "上漲"
        elif post < pre:
            return "下跌"
        return "持平"
    except Exception:
        return "資料不足"


def build_pre_post_compare_table(pre_row: dict, post_row: dict) -> pd.DataFrame:
    compare_fields = [
        ("收盤", True), ("進場", True), ("停損", True), ("短期壓力", True),
        ("中繼目標", True), ("突破目標", True), ("風報比", True),
        ("結論", False), ("交易訊號", False), ("趨勢燈號", False),
        ("進場燈號", False), ("量能燈號", False),
    ]
    compare_rows = []
    for field, numeric in compare_fields:
        pre_val = pre_row.get(field, "")
        post_val = post_row.get(field, "")
        changed = str(pre_val) != str(post_val)
        if numeric:
            try:
                pre_num = float(pre_val)
                post_num = float(post_val)
                diff = round(post_num - pre_num, 2)
                diff_text = f"+{diff}" if diff > 0 else str(diff)
            except Exception:
                diff_text = "—"
        else:
            diff_text = "有變化" if changed else "不變"
        compare_rows.append({
            "欄位": field,
            "盤前": pre_val,
            "盤後": post_val,
            "差異": diff_text,
            "是否變化": "是" if changed else "否"
        })
    return pd.DataFrame(compare_rows)


def build_pair_structure_label(pre_row: dict, post_row: dict) -> str:
    score = 0
    con_rank = {"看多": 4, "中性偏多": 3, "中性": 2, "中性偏空": 1, "看空": 0}
    sig_rank = {"🔥進場": 4, "👀觀察": 3, "⏳等待": 2, "❌不進": 0}
    try:
        score += 1 if con_rank.get(str(post_row.get("結論", "")), 1) > con_rank.get(str(pre_row.get("結論", "")), 1) else 0
        score -= 1 if con_rank.get(str(post_row.get("結論", "")), 1) < con_rank.get(str(pre_row.get("結論", "")), 1) else 0
        score += 1 if sig_rank.get(str(post_row.get("交易訊號", "")), 1) > sig_rank.get(str(pre_row.get("交易訊號", "")), 1) else 0
        score -= 1 if sig_rank.get(str(post_row.get("交易訊號", "")), 1) < sig_rank.get(str(pre_row.get("交易訊號", "")), 1) else 0
    except Exception:
        pass
    try:
        pre_rr = float(pre_row.get("風報比", 0) or 0)
        post_rr = float(post_row.get("風報比", 0) or 0)
        if post_rr - pre_rr > 0.15:
            score += 1
        elif post_rr - pre_rr < -0.15:
            score -= 1
    except Exception:
        pass
    if score >= 1:
        return "變強"
    if score <= -1:
        return "變弱"
    return "持平"


def build_batch_field_diff_summary(compare_batch_df: pd.DataFrame) -> pd.DataFrame:
    if compare_batch_df is None or compare_batch_df.empty:
        return pd.DataFrame()

    field_pairs = [
        ("收盤", "盤前收盤", "盤後收盤"),
        ("風報比", "盤前風報比", "盤後風報比"),
        ("結論", "盤前結論", "盤後結論"),
        ("訊號", "盤前訊號", "盤後訊號"),
        ("價格變化", "價格變化", "價格變化"),
        ("結構變化", "結構變化", "結構變化"),
        ("支撐驗證", "支撐驗證", "支撐驗證"),
        ("壓力驗證", "壓力驗證", "壓力驗證"),
        ("停損觸發", "停損觸發", "停損觸發"),
    ]

    rows = []
    total = len(compare_batch_df)
    for label, pre_col, post_col in field_pairs:
        if pre_col not in compare_batch_df.columns or post_col not in compare_batch_df.columns:
            continue
        if pre_col == post_col:
            series = compare_batch_df[pre_col].astype(str).fillna("").str.strip()
            changed_count = int((series != "") .sum())
            sample_text = "、".join(list(dict.fromkeys([x for x in series.tolist() if x]))[:3])
            rows.append({
                "欄位": label,
                "變動檔數": changed_count,
                "變動比例%": round(changed_count / total * 100, 1) if total else 0.0,
                "重點預覽": sample_text or "—"
            })
            continue

        pre_series = compare_batch_df[pre_col].astype(str).fillna("").str.strip()
        post_series = compare_batch_df[post_col].astype(str).fillna("").str.strip()
        coverage_mask = (pre_series != "") | (post_series != "")
        changed_mask = coverage_mask & (pre_series != post_series)
        changed_count = int(changed_mask.sum())
        preview_df = compare_batch_df.loc[changed_mask, ["股票", pre_col, post_col]].head(3) if "股票" in compare_batch_df.columns else pd.DataFrame()
        preview_items = []
        if not preview_df.empty:
            for _, r in preview_df.iterrows():
                preview_items.append(f"{r.get('股票','')}：{r.get(pre_col,'')}→{r.get(post_col,'')}")
        rows.append({
            "欄位": label,
            "變動檔數": changed_count,
            "變動比例%": round(changed_count / max(int(coverage_mask.sum()), 1) * 100, 1),
            "重點預覽": "；".join(preview_items) if preview_items else "—"
        })

    result = pd.DataFrame(rows)
    if result.empty:
        return result
    return result.sort_values(["變動檔數", "欄位"], ascending=[False, True]).reset_index(drop=True)


CANONICAL_FLOW_COLUMNS = ["收盤", "進場", "停損", "短期壓力", "中繼目標", "突破目標", "風報比", "結論", "交易訊號"]


def get_analysis_core_columns(mobile: bool = False):
    ordered = ["股票", "族群", *CANONICAL_FLOW_COLUMNS, "法人方向", "法人共振", "三大法人買賣超", "法人佔成交量比%", "TWSE命中", "TWSE收盤", "星級", "操作評級", "趨勢燈號", "進場燈號", "量能燈號", "價量燈號"]
    if mobile:
        return ["股票", "收盤", "進場", "停損", "風報比", "結論", "交易訊號"]
    return ordered


def render_info_card_grid(cards):
    if not cards:
        return
    items = []
    for item in cards:
        label = html_escape(item.get("label", ""))
        value = html_escape(item.get("value", "--"))
        sub = html_escape(item.get("sub", ""))
        items.append(f'<div class="mini-batch-card"><div class="mini-batch-label">{label}</div><div class="mini-batch-value">{value}</div>' + (f'<div class="mini-batch-sub">{sub}</div>' if sub else '') + '</div>')
    st.markdown('<div class="mini-batch-grid">' + ''.join(items) + '</div>', unsafe_allow_html=True)


def render_field_highlight_cards(field_summary: pd.DataFrame, top_n: int = 3):
    if field_summary is None or field_summary.empty:
        st.caption("目前沒有可顯示的差異摘要。")
        return
    cards = []
    for idx, (_, row) in enumerate(field_summary.head(top_n).iterrows(), start=1):
        cards.append(
            f'<div class="diff-top-card">'
            f'<div class="diff-top-rank">TOP {idx}</div>'
            f'<div class="diff-top-title">{html_escape(row.get("欄位", "--"))}</div>'
            f'<div class="diff-top-main">{int(row.get("變動檔數", 0))} 檔</div>'
            f'<div class="diff-top-sub">變動比例 {html_escape(str(row.get("變動比例%", 0)))}%<br>{html_escape(str(row.get("重點預覽", "—"))[:120])}</div>'
            f'</div>'
        )
    st.markdown('<div class="diff-top-grid">' + ''.join(cards) + '</div>', unsafe_allow_html=True)


def render_section_divider(title: str, note: str = ""):
    st.markdown(
        f'<div class="section-divider-card"><div class="section-divider-title">{html_escape(title)}</div>'
        + (f'<div class="section-divider-note">{html_escape(note)}</div>' if note else '')
        + '</div>',
        unsafe_allow_html=True,
    )


def render_batch_compare_detail(detail_row: dict, key_prefix: str = "batch_detail"):

    if not detail_row:
        st.caption("目前沒有可顯示的單股細節。")
        return

    render_compare_status_cards(detail_row)
    render_compare_chips(detail_row)
    render_compare_matrix(detail_row)

    detail_code = str(detail_row.get("股票代碼", "")).strip() or str(detail_row.get("股票", "")).split("（")[0].strip()
    default_entry_val = detail_row.get("模擬進場價", detail_row.get("盤前建議進場", ""))
    try:
        default_entry_num = float(default_entry_val)
    except Exception:
        default_entry_num = 0.0

    custom_entry_price = st.number_input(
        "自訂模擬進場價",
        min_value=0.0,
        value=round(float(default_entry_num), 2),
        step=0.1,
        format="%.2f",
        key=f"custom_entry_{key_prefix}_{detail_code}"
    )
    sim_data = simulate_entry_from_intraday(detail_code, float(custom_entry_price), detail_row.get("盤前停損", ""))
    render_sim_matrix(
        sim_data.get("進場時間", "--"),
        sim_data.get("模擬收盤價", ""),
        sim_data.get("收盤模擬結果", "資料不足"),
        sim_data.get("收盤模擬損益", ""),
        sim_data.get("收盤模擬報酬率%", ""),
        sim_data.get("模擬最高價", ""),
        sim_data.get("最高模擬結果", "資料不足"),
        sim_data.get("最高模擬損益", ""),
        sim_data.get("最高模擬報酬率%", ""),
        sim_data.get("模擬最低價", ""),
        sim_data.get("最低模擬結果", "資料不足"),
        sim_data.get("最低模擬損益", ""),
        sim_data.get("最低模擬報酬率%", ""),
    )

    left_detail, right_detail = st.columns(2)
    with left_detail:
        st.markdown("#### 盤前資料")
        pre_a, pre_b, pre_c = st.columns(3)
        pre_a.metric("盤前收盤", fmt_price(detail_row.get("盤前收盤", "")))
        pre_b.metric("盤前支撐", fmt_price(detail_row.get("盤前支撐", "")))
        pre_c.metric("盤前壓力", fmt_price(detail_row.get("盤前壓力", "")))
        pre_d, pre_e, pre_f = st.columns(3)
        pre_d.metric("建議進場", fmt_price(detail_row.get("盤前建議進場", "")))
        pre_e.metric("盤前停損", fmt_price(detail_row.get("盤前停損", "")))
        pre_f.metric("盤前風報比", fmt_price(detail_row.get("盤前風報比", "")))
        pre_g, pre_h = st.columns(2)
        pre_g.metric("盤前結論", fmt_text(detail_row.get("盤前結論", "")))
        pre_h.metric("盤前訊號", fmt_text(detail_row.get("盤前訊號", "")))

    with right_detail:
        st.markdown("#### 盤後資料")
        post_a, post_b, post_c = st.columns(3)
        post_a.metric("盤後收盤", fmt_price(detail_row.get("盤後收盤", "")))
        post_b.metric("盤後最高", fmt_price(detail_row.get("盤後最高", "")))
        post_c.metric("盤後最低", fmt_price(detail_row.get("盤後最低", "")))
        post_d, post_e, post_f = st.columns(3)
        post_d.metric("盤後風報比", fmt_price(detail_row.get("盤後風報比", "")))
        post_e.metric("盤後結論", fmt_text(detail_row.get("盤後結論", "")))
        post_f.metric("盤後訊號", fmt_text(detail_row.get("盤後訊號", "")))
        reason_source = detail_row.copy()
        reason_source["風報比"] = detail_row.get("盤後風報比", detail_row.get("盤前風報比", ""))
        render_reason_block(reason_source, "這檔為何這樣判斷")

def render_compare_status_cards(detail_row: dict, title: str = "對照摘要"):
    a, b, c = st.columns(3)
    a.metric("價量結論", fmt_text(detail_row.get("價量結論", "")) or "--")
    b.metric("價格變化", fmt_text(detail_row.get("價格變化", detail_row.get("價格狀態", ""))) or "--")
    b.caption("單純比較盤前收盤 vs 目前/盤後收盤")
    c.metric("結構變化", fmt_text(detail_row.get("結構變化", detail_row.get("變化判斷", ""))) or "--")
    c.caption("綜合支撐、壓力、訊號、風報比後的結構判讀")


def build_short_strategy(row: dict):
    """
    V178：空方總表當沖化
    - 空方短期壓力 / 建議進場 / 停損 / 中繼目標 / 跌破目標
      全部限制在以收盤為基準的次日可交易邊界內
    - 不再讓空方總表跑出明顯超過日內 10% 的波段型數值
    """
    try:
        close = float(row.get("收盤", 0) or 0)
        support = float(row.get("支撐", 0) or 0)
        resistance = float(row.get("短期壓力", 0) or 0)
        breakout = float(row.get("突破目標", 0) or 0)
        mid = float(row.get("中繼目標", 0) or 0)
    except Exception:
        return {
            "空方短期壓力": "",
            "空方短期支撐": "",
            "空方建議進場": "",
            "空方停損": "",
            "空方中繼目標": "",
            "空方跌破目標": ""
        }

    if close <= 0:
        return {
            "空方短期壓力": "",
            "空方短期支撐": "",
            "空方建議進場": "",
            "空方停損": "",
            "空方中繼目標": "",
            "空方跌破目標": ""
        }

    upper_limit = round(close * 1.10, 2)
    lower_limit = round(max(close * 0.90, 0.01), 2)

    def _clamp(value, low, high):
        try:
            return max(low, min(float(value), high))
        except Exception:
            return low

    # 空方短期壓力：至少高於收盤，且不得超過次日上緣
    raw_resistance = resistance if resistance > 0 else close * 1.02
    if raw_resistance <= close:
        raw_resistance = close * 1.02
    short_resistance = _clamp(raw_resistance, close, upper_limit)

    # 空方短期支撐：至少低於收盤，且不得跌破次日下緣
    raw_support = support if support > 0 else close * 0.98
    if raw_support >= close:
        raw_support = close * 0.98
    short_support = _clamp(raw_support, lower_limit, close)

    bounce_span = max(short_resistance - close, max(close * 0.01, 0.2))

    # 空方建議進場：靠近壓力，不再用偏波段的寬中段
    short_entry = close + bounce_span * 0.72
    short_entry = _clamp(short_entry, close, short_resistance)

    # 空方停損：壓力上方極小緩衝，且硬限制在漲停邊界內
    short_stop = max(short_resistance * 1.004, short_entry * 1.008, short_resistance + max(close * 0.002, 0.2))
    short_stop = _clamp(short_stop, short_entry, upper_limit)

    # 空方中繼目標：優先原支撐；若原支撐太遠，壓回到日內合理區
    short_mid = min(short_support, short_entry - max(close * 0.015, bounce_span * 0.55))
    short_mid = _clamp(short_mid, lower_limit, short_entry)

    # 空方跌破目標：在中繼目標下方再留一段，但不得跌破跌停邊界
    short_break = short_mid - max(close * 0.012, bounce_span * 0.45)
    short_break = _clamp(short_break, lower_limit, short_mid)

    # 原本波段結構位只保留作為約束，不再直接決定當沖停損 / 目標
    if breakout > 0:
        short_stop = min(short_stop, _clamp(breakout, short_entry, upper_limit))
    if mid > 0:
        short_mid = min(short_mid, _clamp(mid, lower_limit, short_entry))

    return {
        "空方短期壓力": round(short_resistance, 2),
        "空方短期支撐": round(short_support, 2),
        "空方建議進場": round(short_entry, 2),
        "空方停損": round(short_stop, 2),
        "空方中繼目標": round(short_mid, 2),
        "空方跌破目標": round(short_break, 2)
    }

def build_liquidity_profile(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {
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
            "流動性結論": "資料不足",
            "流動性摘要": "資料不足",
            "流動性排除原因": "資料不足",
        }

    vol_shares = pd.to_numeric(df.get("Volume"), errors="coerce").fillna(0)
    close = pd.to_numeric(df.get("Close"), errors="coerce").fillna(0)
    high = pd.to_numeric(df.get("High"), errors="coerce").fillna(0)
    low = pd.to_numeric(df.get("Low"), errors="coerce").fillna(0)

    vol_lots = vol_shares / 1000.0
    trade_value = close * vol_shares
    amp_pct = ((high - low) / close.replace(0, pd.NA) * 100).fillna(0)

    tail20_vol = vol_lots.tail(20)
    tail5_vol = vol_lots.tail(5)
    tail20_val = trade_value.tail(20)
    tail20_amp = amp_pct.tail(20)

    avg_vol20 = float(tail20_vol.mean()) if len(tail20_vol) else 0.0
    avg_value20 = float(tail20_val.mean() / 100000000.0) if len(tail20_val) else 0.0
    avg_amp20 = float(tail20_amp.mean()) if len(tail20_amp) else 0.0
    avg_vol5 = float(tail5_vol.mean()) if len(tail5_vol) else 0.0
    low_days5 = int((tail5_vol < LIQ_LOW_DAY5_LOTS).sum()) if len(tail5_vol) else 0
    stable_ratio = round(avg_vol5 / max(avg_vol20, 1.0), 2) if avg_vol20 else 0.0
    median_vol20 = float(tail20_vol.median()) if len(tail20_vol) else 0.0
    burst_ratio = round(float(tail5_vol.max() / max(median_vol20, 1.0)), 2) if len(tail5_vol) else 0.0
    burst_dry = bool(median_vol20 < LIQ_BURST_MEDIAN20_LIMIT and burst_ratio >= LIQ_BURST_RATIO_LIMIT)

    reasons = []
    passed = True
    if avg_vol20 < LIQ_MIN_AVG_VOL20_LOTS:
        reasons.append(f"20日均量 {avg_vol20:,.0f} 張 < {LIQ_MIN_AVG_VOL20_LOTS:,.0f} 張")
        passed = False
    if avg_value20 < LIQ_MIN_AVG_VALUE20_EOK:
        reasons.append(f"20日均值 {avg_value20:.2f} 億 < {LIQ_MIN_AVG_VALUE20_EOK:.1f} 億")
        passed = False
    if avg_amp20 < LIQ_MIN_AVG_AMP20_PCT:
        reasons.append(f"20日均振幅 {avg_amp20:.2f}% < {LIQ_MIN_AVG_AMP20_PCT:.1f}%")
        passed = False
    if low_days5 > LIQ_MAX_LOW_DAYS5:
        reasons.append(f"近5日低量天數 {low_days5} 天 > {LIQ_MAX_LOW_DAYS5} 天")
        passed = False
    if stable_ratio < LIQ_MIN_STABLE_RATIO:
        reasons.append(f"近5日量能穩定比 {stable_ratio:.2f} < {LIQ_MIN_STABLE_RATIO:.2f}")
        passed = False
    if burst_dry:
        reasons.append(f"單日爆量比 {burst_ratio:.2f} 倍，但20日中位量僅 {median_vol20:,.0f} 張")
        passed = False

    score = 0.0
    score += min(avg_vol20 / 2000.0, 20.0)
    score += min(avg_value20 * 2.5, 20.0)
    score += min(avg_amp20 * 2.5, 15.0)
    score += min(max(stable_ratio, 0.0) * 12.0, 12.0)
    score += max(0, (LIQ_MAX_LOW_DAYS5 - low_days5 + 1)) * 3.0
    if burst_dry:
        score -= 12.0
    if avg_vol20 >= 25000:
        score += 8.0
    if avg_value20 >= 10:
        score += 8.0
    if avg_amp20 >= 4.5:
        score += 5.0
    score = round(max(0.0, min(score, 100.0)), 2)

    if passed and score >= 70:
        conclusion = "流動性佳"
    elif passed:
        conclusion = "流動性合格"
    else:
        conclusion = "流動性不足"

    if reasons:
        exclude_reason = "；".join(reasons)
    else:
        exclude_reason = "符合盤前流動性門檻"

    summary = f"20日均量 {avg_vol20:,.0f} 張｜20日均值 {avg_value20:.2f} 億｜20日均振幅 {avg_amp20:.2f}%｜近5日低量 {low_days5} 天"
    return {
        "流動性20日均量張": round(avg_vol20, 2),
        "流動性20日均值億": round(avg_value20, 2),
        "流動性20日均振幅%": round(avg_amp20, 2),
        "流動性5日均量張": round(avg_vol5, 2),
        "流動性5日低量天數": low_days5,
        "流動性5日穩定比": round(stable_ratio, 2),
        "流動性20日中位量張": round(median_vol20, 2),
        "流動性爆量比": round(burst_ratio, 2),
        "流動性單日爆量偏乾": "是" if burst_dry else "否",
        "流動性達標": "是" if passed else "否",
        "流動性分數": score,
        "流動性結論": conclusion,
        "流動性摘要": summary,
        "流動性排除原因": exclude_reason,
    }


def liquidity_pass(item: dict) -> bool:
    return str(item.get("流動性達標", "否")) == "是"


def liquidity_penalty_reason(item: dict) -> str:
    return str(item.get("流動性排除原因", "流動性不足"))


def liquidity_auto_bonus(item: dict, mode: str = "general") -> float:
    liq_score = float(item.get("流動性分數", 0) or 0)
    avg_vol20 = float(item.get("流動性20日均量張", 0) or 0)
    avg_value20 = float(item.get("流動性20日均值億", 0) or 0)
    stable_ratio = float(item.get("流動性5日穩定比", 0) or 0)
    burst_dry = str(item.get("流動性單日爆量偏乾", "否")) == "是"
    avg_amp20 = float(item.get("流動性20日均振幅%", 0) or 0)

    score = 0.0
    if mode == "strict":
        score += liq_score * 0.55
        score += min(avg_value20, 12) * 1.8
        score += min(avg_vol20 / 4000.0, 8.0)
    elif mode == "short":
        score += liq_score * 0.35
        score += min(avg_amp20, 8) * 1.2
        score += min(avg_value20, 12) * 1.0
    else:
        score += liq_score * 0.45
        score += min(avg_value20, 12) * 1.4
        score += min(avg_vol20 / 5000.0, 7.0)

    if stable_ratio >= 1.0:
        score += 5.0
    elif stable_ratio < 0.7:
        score -= 4.0
    if burst_dry:
        score -= 8.0
    if mode == "short":
        pv = str(item.get("價量結論", "") or "")
        if pv == "價跌量縮":
            score -= 6.0
    return round(score, 2)

CANDIDATE_FAIL_COOLDOWN_MINUTES = 5


def _analysis_refresh_tag() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _candidate_pool_session_cache_tag() -> str:
    """自動挑股專用：複合標籤，避免僅用日曆日導致盤前整早重用同一批成功結果。"""
    meta = load_cached_official_meta()
    latest = str(meta.get("latest_date", "") or meta.get("date", "") or "").strip() or "na"
    try:
        tz = ZoneInfo("Asia/Taipei")
        now = datetime.now(tz)
    except Exception:
        now = datetime.now()
    hm = now.hour * 60 + now.minute
    if hm < 9 * 60:
        sess = "盤前"
        slot = f"H{now.hour:02d}"
    elif hm <= 13 * 60 + 30:
        sess = "交易中"
        slot = "sess"
    else:
        sess = "盤後"
        slot = "sess"
    return f"of:{latest}|{sess}|{slot}"


def _candidate_stock_cache_key(raw_stock: str, market_adj: int = 0, pool_tag: str | None = None):
    day = _analysis_refresh_tag()
    segment = f"{day}|{pool_tag}" if pool_tag else day
    return (str(raw_stock).strip().upper(), int(market_adj or 0), segment, APP_VERSION)


def _candidate_fail_key(raw_stock: str) -> str:
    return str(raw_stock).strip().upper()


def _candidate_stock_cache_get(raw_stock: str, market_adj: int = 0, pool_tag: str | None = None):
    cache = st.session_state.setdefault("_candidate_stock_cache", {})
    item = cache.get(_candidate_stock_cache_key(raw_stock, market_adj, pool_tag))
    return dict(item) if isinstance(item, dict) else None


def _candidate_stock_cache_set(raw_stock: str, market_adj: int, item: dict | None, pool_tag: str | None = None):
    if not isinstance(item, dict):
        return
    cache = st.session_state.setdefault("_candidate_stock_cache", {})
    cache[_candidate_stock_cache_key(raw_stock, market_adj, pool_tag)] = dict(item)
    if len(cache) > 1200:
        for old_key in list(cache.keys())[:300]:
            cache.pop(old_key, None)


def _candidate_fail_cooldown_active(raw_stock: str):
    fail_map = st.session_state.setdefault("_candidate_fail_cooldown", {})
    key = _candidate_fail_key(raw_stock)
    meta = fail_map.get(key)
    if not isinstance(meta, dict):
        return False, None
    until_iso = meta.get("until")
    try:
        until_dt = datetime.fromisoformat(str(until_iso))
    except Exception:
        fail_map.pop(key, None)
        return False, None
    if datetime.now() >= until_dt:
        fail_map.pop(key, None)
        return False, None
    return True, meta


def _candidate_register_failure(raw_stock: str, reason: str):
    fail_map = st.session_state.setdefault("_candidate_fail_cooldown", {})
    key = _candidate_fail_key(raw_stock)
    fail_map[key] = {
        "reason": str(reason or "資料不足"),
        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "until": (datetime.now() + timedelta(minutes=CANDIDATE_FAIL_COOLDOWN_MINUTES)).isoformat(timespec="seconds"),
    }


def analyze_one_cached(raw_stock: str, market_adj: int = 0, name_map: dict | None = None):
    shared = _candidate_stock_cache_get(raw_stock, market_adj)
    if shared is not None:
        return shared

    cache = st.session_state.setdefault("_manual_analysis_cache", {})
    key = (str(raw_stock).strip().upper(), int(market_adj or 0), _analysis_refresh_tag(), APP_VERSION)
    if key in cache:
        hit = cache[key]
        return dict(hit) if isinstance(hit, dict) else hit

    item = analyze_one(raw_stock, market_adj, name_map)
    if item is not None:
        cache[key] = dict(item)
        _candidate_stock_cache_set(raw_stock, market_adj, item)
    if len(cache) > 240:
        for old_key in list(cache.keys())[:60]:
            cache.pop(old_key, None)
    return item


def analyze_one(raw_stock: str, market_adj: int = 0, name_map: dict | None = None, indicator_df_store: dict | None = None):
    name_map = name_map or load_name_map()
    item = ce.analyze_one(
        raw_stock=raw_stock,
        market_adj=market_adj,
        name_map=name_map,
        resolve_symbol=lambda s: resolve_symbol(s, name_map),
        indicators=indicators,
        display_name_func=display_name,
        stock_sector=stock_sector,
        liquidity_builder=build_liquidity_profile,
        indicator_df_store=indicator_df_store,
    )
    return item


@st.cache_data(ttl=900, show_spinner=False)
def analyze_candidate_pool_cached(candidate_pool: tuple, market_adj: int, name_map: dict):
    codes = [str(x).strip() for x in candidate_pool if str(x).strip()]
    if not codes:
        return []
    max_workers = min(12, max(4, len(codes) // 20 + 2))
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(analyze_one, code, market_adj, name_map): code for code in codes}
        for fut in as_completed(futures):
            try:
                item = fut.result()
                if item is not None:
                    results.append(item)
            except Exception:
                continue
    return results


def render_auto_pick_engine_panel(engine_debug: dict):
    """分析中心自動挑股：本輪候選池／快取／成敗／流動性數字一次列齊。"""
    ed = engine_debug or {}
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("候選池總數", ed.get("candidate_count", 0))
    m2.metric("快取重用數", ed.get("cache_hits", 0))
    m3.metric("本輪新成功數", ed.get("new_success", 0))
    m4.metric("本輪新失敗數", ed.get("new_failures", 0))
    m5, m6, m7, m8 = st.columns(4)
    m5.metric("冷卻跳過數", ed.get("cooldown_skips", 0))
    m6.metric("最終有效樣本數", ed.get("result_count", 0))
    m7.metric("通過流動性門檻數", int(ed.get("liquidity_passed_count") or 0))
    m8.metric("補抓救回數", ed.get("retry_recovered", 0))
    st.caption(
        f"快取分段：{fmt_text(str(ed.get('cache_session_tag', '') or '')) or '--'} ｜ "
        f"技術資料重用 {ed.get('indicator_df_cached', 0)} 檔 ｜ "
        f"本輪需刷新 {ed.get('pending_fresh_count', 0)} 檔 ｜ "
        f"第一輪待補抓 {ed.get('retry_queue_size', 0)} 檔"
    )


def dataframe_from_analysis_records(records: list) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()
    df_result = pd.DataFrame(records)
    df_result = ensure_dataframe_columns(df_result, list(TWSE_RESULT_DEFAULTS.keys()))
    df_result["趨勢燈號"] = df_result.apply(lambda r: signal_light_pack(r)["趨勢燈號"], axis=1)
    df_result["進場燈號"] = df_result.apply(lambda r: signal_light_pack(r)["進場燈號"], axis=1)
    df_result["量能燈號"] = df_result.apply(lambda r: signal_light_pack(r)["量能燈號"], axis=1)
    return df_result


def _auto_pick_clear_backup_session():
    st.session_state.auto_pick_backup_rows = None
    st.session_state.auto_pick_backup_mode = None
    st.session_state.auto_pick_backup_detail_code = None


def _auto_pick_quality_blocked(debug: dict) -> tuple[bool, str]:
    """盤前自動挑股：本輪抓取品質不足時不輸出推薦。"""
    n = int(debug.get("candidate_count") or 0)
    if n <= 0:
        return False, ""
    nh = int(debug.get("cache_hits") or 0)
    ns = int(debug.get("new_success") or 0)
    nf = int(debug.get("new_failures") or 0)
    effective = nh + ns
    tried = ns + nf
    if nf > ns and tried >= 6:
        return True, "本輪盤前資料品質不足，暫不建議參考自動挑股結果（本輪新抓失敗筆數大於成功筆數）。"
    if tried >= 10 and ns < max(5, int(0.05 * n)):
        return True, "本輪盤前資料品質不足，暫不建議參考自動挑股結果（本輪新成功數過低）。"
    if n >= 40 and effective < max(12, int(0.25 * n)):
        return True, "本輪盤前資料品質不足，暫不建議參考自動挑股結果（最終有效候選佔比過低）。"
    if nh >= max(1, int(0.88 * n)) and ns == 0 and nf >= 6:
        return True, "本輪盤前資料品質不足，暫不建議參考自動挑股結果（幾乎全為快取命中且本輪無新成功、失敗偏多）。"
    return False, ""


def analyze_candidate_pool_session(candidate_pool: tuple, market_adj: int, name_map: dict, progress_hook=None):
    codes = [str(x).strip() for x in candidate_pool if str(x).strip()]
    empty_debug = {
        "candidate_count": 0,
        "cache_hits": 0,
        "new_success": 0,
        "new_failures": 0,
        "cooldown_skips": 0,
        "cooldown_rows": [],
        "failure_rows": [],
        "retry_recovered": 0,
        "retry_queue_size": 0,
        "cache_session_tag": "",
        "fresh_tried": 0,
    }
    if not codes:
        return [], empty_debug, {}

    pool_tag = _candidate_pool_session_cache_tag()
    results = []
    indicator_df_store: dict[str, pd.DataFrame] = {}
    debug = {
        "candidate_count": len(codes),
        "cache_hits": 0,
        "new_success": 0,
        "new_failures": 0,
        "cooldown_skips": 0,
        "cooldown_rows": [],
        "failure_rows": [],
        "retry_recovered": 0,
        "retry_queue_size": 0,
        "cache_session_tag": pool_tag,
        "fresh_tried": 0,
    }

    pending_codes = []
    seen = set()
    for code in codes:
        code_u = str(code).strip().upper()
        if not code_u or code_u in seen:
            continue
        seen.add(code_u)
        cached_item = _candidate_stock_cache_get(code_u, market_adj, pool_tag=pool_tag)
        if cached_item is not None:
            results.append(cached_item)
            debug["cache_hits"] += 1
            continue
        active, meta = _candidate_fail_cooldown_active(code_u)
        if active:
            debug["cooldown_skips"] += 1
            debug["cooldown_rows"].append({
                "股票代碼": code_u,
                "原因": meta.get("reason", "資料不足"),
                "冷卻到": meta.get("until", ""),
            })
            continue
        pending_codes.append(code_u)

    if progress_hook:
        progress_hook({
            "phase": "queued",
            "total_candidates": len(codes),
            "cache_hits": debug["cache_hits"],
            "cooldown_skips": debug["cooldown_skips"],
            "pending_fetch": len(pending_codes),
            "completed_fetch": debug["cache_hits"],
            "failures": 0,
            "pending_retry": 0,
            "batch_index": 0,
            "batch_total": max(1, (len(pending_codes) + 19) // 20) if pending_codes else 0,
        })

    retry_queue: list[tuple[str, str]] = []
    if pending_codes:
        import time as _time_mod
        batch_size = 20
        max_workers = 4
        num_batches = (len(pending_codes) + batch_size - 1) // batch_size
        for batch_idx, batch_start in enumerate(range(0, len(pending_codes), batch_size)):
            batch = pending_codes[batch_start:batch_start + batch_size]
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(analyze_one, code, market_adj, name_map, indicator_df_store): code for code in batch}
                for fut in as_completed(futures):
                    code_u = futures[fut]
                    try:
                        item = fut.result()
                    except Exception as e:
                        reason = f"{type(e).__name__}: {e}"
                        retry_queue.append((code_u, reason))
                        if progress_hook:
                            progress_hook({
                                "phase": "analyze",
                                "total_candidates": len(codes),
                                "cache_hits": debug["cache_hits"],
                                "cooldown_skips": debug["cooldown_skips"],
                                "pending_fetch": len(pending_codes),
                                "completed_fetch": debug["cache_hits"] + debug["new_success"] + len(retry_queue),
                                "failures": len(retry_queue),
                                "pending_retry": len(retry_queue),
                                "batch_index": batch_idx + 1,
                                "batch_total": num_batches,
                            })
                        continue
                    if item is None:
                        reason = "無有效日線或資料天數不足"
                        retry_queue.append((code_u, reason))
                        if progress_hook:
                            progress_hook({
                                "phase": "analyze",
                                "total_candidates": len(codes),
                                "cache_hits": debug["cache_hits"],
                                "cooldown_skips": debug["cooldown_skips"],
                                "pending_fetch": len(pending_codes),
                                "completed_fetch": debug["cache_hits"] + debug["new_success"] + len(retry_queue),
                                "failures": len(retry_queue),
                                "pending_retry": len(retry_queue),
                                "batch_index": batch_idx + 1,
                                "batch_total": num_batches,
                            })
                        continue
                    _candidate_stock_cache_set(code_u, market_adj, item, pool_tag=pool_tag)
                    results.append(item)
                    debug["new_success"] += 1
                    if progress_hook:
                        progress_hook({
                            "phase": "analyze",
                            "total_candidates": len(codes),
                            "cache_hits": debug["cache_hits"],
                            "cooldown_skips": debug["cooldown_skips"],
                            "pending_fetch": len(pending_codes),
                            "completed_fetch": debug["cache_hits"] + debug["new_success"] + len(retry_queue),
                            "failures": len(retry_queue),
                            "pending_retry": len(retry_queue),
                            "batch_index": batch_idx + 1,
                            "batch_total": num_batches,
                        })
            if batch_start + batch_size < len(pending_codes):
                _time_mod.sleep(0.5)

    debug["retry_queue_size"] = len(retry_queue)
    if retry_queue:
        if progress_hook:
            progress_hook({
                "phase": "retry",
                "total_candidates": len(codes),
                "cache_hits": debug["cache_hits"],
                "cooldown_skips": debug["cooldown_skips"],
                "pending_fetch": len(pending_codes),
                "completed_fetch": debug["cache_hits"] + debug["new_success"],
                "failures": 0,
                "pending_retry": len(retry_queue),
                "batch_index": num_batches,
                "batch_total": num_batches,
            })
        import time as _time_mod
        for code_u, r1_reason in retry_queue:
            _time_mod.sleep(0.4)
            fail_reason = None
            try:
                item = analyze_one(code_u, market_adj, name_map, indicator_df_store)
            except Exception as e:
                item = None
                fail_reason = f"{type(e).__name__}: {e}"
            if item is None:
                if fail_reason is None:
                    fail_reason = str(r1_reason or "無有效日線或資料天數不足")
                _candidate_register_failure(code_u, fail_reason)
                debug["new_failures"] += 1
                debug["failure_rows"].append({"股票代碼": code_u, "原因": f"補抓後仍失敗：{fail_reason}"})
            else:
                _candidate_stock_cache_set(code_u, market_adj, item, pool_tag=pool_tag)
                results.append(item)
                debug["new_success"] += 1
                debug["retry_recovered"] += 1
            if progress_hook:
                progress_hook({
                    "phase": "retry",
                    "total_candidates": len(codes),
                    "cache_hits": debug["cache_hits"],
                    "cooldown_skips": debug["cooldown_skips"],
                    "pending_fetch": len(pending_codes),
                    "completed_fetch": debug["cache_hits"] + debug["new_success"] + debug["new_failures"],
                    "failures": debug["new_failures"],
                    "pending_retry": 0,
                    "batch_index": num_batches,
                    "batch_total": num_batches,
                })

    debug["fresh_tried"] = int(debug["new_success"]) + int(debug["new_failures"])
    debug["pending_fresh_count"] = len(pending_codes)
    debug["result_count"] = len(results)
    debug["indicator_df_cached"] = len(indicator_df_store)
    return results, debug, indicator_df_store


def _indicator_df_for_auto_pick(item: dict, store: dict) -> pd.DataFrame | None:
    if not store:
        return None
    code = str(item.get("_code") or item.get("股票代碼") or "").strip().upper()
    if not code:
        return None
    df = store.get(code)
    if df is not None and not df.empty:
        return df
    base = code.split(".")[0]
    if re.fullmatch(r"\d{4}", base):
        for suf in (".TW", ".TWO"):
            d = store.get(base + suf)
            if d is not None and not d.empty:
                return d
    return None


@st.cache_data(ttl=600, show_spinner=False)
def get_symbol_indicator_chart(symbol: str) -> pd.DataFrame:
    """單股詳細分析專用：只抓單一股票並補齊技術指標，避免切換股票時整頁重算。"""
    nm = load_name_map()
    resolved_code, raw_df = resolve_symbol(symbol, nm)
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()
    try:
        df = indicators(raw_df)
    except Exception:
        return pd.DataFrame()
    return df if df is not None else pd.DataFrame()

def save_snapshot(snapshot_type: str, results, forced_mode: str | None = None):
    snaps = load_snapshots()
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for row in results:
        rec = materialize_snapshot_row(row, forced_mode=forced_mode)
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

def get_current_plan_for_stock(stock_code: str):
    q = stock_code.upper()
    results = st.session_state.get("results_data", [])
    for row in results:
        code = str(row.get("_code", "")).upper()
        name_text = str(row.get("股票", "")).upper()
        if code == q or code.startswith(q + ".") or q.startswith(code.split(".")[0]) or name_text.startswith(q):
            return {
                "計畫來源": "目前分析",
                "計畫進場": row.get("進場", ""),
                "計畫停損": row.get("停損", ""),
                "計畫短壓": row.get("短期壓力", ""),
                "計畫中繼": row.get("中繼目標", ""),
                "計畫突破": row.get("突破目標", ""),
                "計畫風報比": row.get("風報比", ""),
                "計畫結論": row.get("結論", ""),
                "計畫訊號": row.get("交易訊號", ""),
            }
    latest_snap = find_latest_snapshot_for_stock(q)
    if latest_snap:
        return {
            "計畫來源": f'最新{latest_snap.get("類型","快照")}',
            "計畫進場": latest_snap.get("進場", ""),
            "計畫停損": latest_snap.get("停損", ""),
            "計畫短壓": latest_snap.get("短期壓力", ""),
            "計畫中繼": latest_snap.get("中繼目標", ""),
            "計畫突破": latest_snap.get("突破目標", ""),
            "計畫風報比": latest_snap.get("風報比", ""),
            "計畫結論": latest_snap.get("結論", ""),
            "計畫訊號": latest_snap.get("交易訊號", ""),
        }
    return None


def get_latest_pre_post_snapshot(stock_code: str):
    snaps = load_snapshots()
    q = stock_code.upper()
    matched = []
    for s in snaps:
        code = str(s.get("股票代碼", "")).upper()
        stock_text = str(s.get("股票", "")).upper()
        if code == q or code.startswith(q + ".") or q.startswith(code.split(".")[0]) or stock_text.startswith(q):
            matched.append(s)
    if not matched:
        return None, None
    matched = sorted(matched, key=lambda x: x.get("時間", ""), reverse=True)
    pre = next((x for x in matched if x.get("類型") == "盤前"), None)
    post = next((x for x in matched if x.get("類型") == "盤後"), None)
    return pre, post


def normalize_trade_side(side: str) -> str:
    text = str(side or "").strip()
    return "做空" if "空" in text else "做多"

def build_trade_context(stock_code: str, side: str, name_map: dict):
    plan = get_current_plan_for_stock(stock_code)
    pre, post = get_latest_pre_post_snapshot(stock_code)
    context = {
        "計畫來源": "", "計畫進場": "", "計畫停損": "", "計畫短壓": "", "計畫中繼": "",
        "計畫突破": "", "計畫風報比": "", "計畫結論": "", "計畫訊號": "",
        "盤前快照時間": "", "盤前快照結論": "", "盤前快照訊號": "", "盤前快照風報比": "",
        "盤後快照時間": "", "盤後快照結論": "", "盤後快照訊號": "", "盤後快照風報比": "",
        "法人方向": "", "法人共振": "", "快照邏輯": "空方" if normalize_trade_side(side) == "做空" else "多方",
    }
    if plan:
        context.update({
            "計畫來源": plan.get("計畫來源",""),
            "計畫進場": plan.get("計畫進場",""),
            "計畫停損": plan.get("計畫停損",""),
            "計畫短壓": plan.get("計畫短壓",""),
            "計畫中繼": plan.get("計畫中繼",""),
            "計畫突破": plan.get("計畫突破",""),
            "計畫風報比": plan.get("計畫風報比",""),
            "計畫結論": plan.get("計畫結論",""),
            "計畫訊號": plan.get("計畫訊號",""),
        })
    if pre:
        context.update({
            "盤前快照時間": pre.get("時間",""),
            "盤前快照結論": pre.get("結論",""),
            "盤前快照訊號": pre.get("交易訊號",""),
            "盤前快照風報比": pre.get("風報比",""),
            "快照邏輯": pre.get("快照邏輯", context.get("快照邏輯","")),
        })
    if post:
        context.update({
            "盤後快照時間": post.get("時間",""),
            "盤後快照結論": post.get("結論",""),
            "盤後快照訊號": post.get("交易訊號",""),
            "盤後快照風報比": post.get("風報比",""),
        })
    try:
        row = analyze_one(stock_code, market_filter()["score_adj"], name_map)
        if row:
            row2 = try_apply_twse_hybrid([row])[0][0]
            context["法人方向"] = row2.get("法人方向","")
            context["法人共振"] = row2.get("法人共振","")
    except Exception:
        pass
    return context

def pair_trades(trades):
    grouped, closed, open_pos = {}, [], []
    for t in trades:
        stock_key = str(t.get("股票", ""))
        side_key = normalize_trade_side(t.get("方向", "做多"))
        grouped.setdefault((stock_key, side_key), []).append(t)

    for (code, side), rows in grouped.items():
        opens = []
        open_action = "買進" if side == "做多" else "賣出"
        close_action = "賣出" if side == "做多" else "買進"

        for r in sorted(rows, key=lambda x: str(x.get("時間", ""))):
            act = str(r.get("動作", ""))
            try:
                qty = int(float(r.get("數量", 0) or 0))
            except (ValueError, TypeError):
                continue
            if qty <= 0:
                continue

            if act == open_action:
                rec = dict(r)
                try:
                    rec["剩餘數量"] = int(float(rec.get("剩餘數量", qty) or qty))
                except (ValueError, TypeError):
                    rec["剩餘數量"] = qty
                opens.append(rec)
            elif act == close_action:
                qty_to_close = qty
                while qty_to_close > 0 and opens:
                    op = opens[0]
                    matched = min(qty_to_close, int(op.get("剩餘數量", 0) or 0))
                    open_price = float(op.get("價格", 0) or 0)
                    close_price = float(r.get("價格", 0) or 0)
                    if side == "做多":
                        pnl = (close_price - open_price) * matched
                        ret = ((close_price - open_price) / open_price * 100) if open_price else 0
                    else:
                        pnl = (open_price - close_price) * matched
                        ret = ((open_price - close_price) / open_price * 100) if open_price else 0
                    closed.append({
                        "股票": code, "方向": side,
                        "開倉時間": op.get("時間",""), "平倉時間": r.get("時間",""),
                        "開倉價": open_price, "平倉價": close_price, "數量": matched,
                        "損益": round(pnl, 2), "報酬率%": round(ret, 2),
                        "買進時間": op.get("時間","") if side == "做多" else r.get("時間",""),
                        "賣出時間": r.get("時間","") if side == "做多" else op.get("時間",""),
                        "買進價": close_price if side == "做空" else open_price,
                        "賣出價": open_price if side == "做空" else close_price,
                        "計畫來源": op.get("計畫來源",""),
                        "計畫進場": op.get("計畫進場",""),
                        "計畫停損": op.get("計畫停損",""),
                        "計畫短壓": op.get("計畫短壓",""),
                        "計畫中繼": op.get("計畫中繼",""),
                        "計畫突破": op.get("計畫突破",""),
                        "計畫風報比": op.get("計畫風報比",""),
                        "計畫結論": op.get("計畫結論",""),
                        "計畫訊號": op.get("計畫訊號",""),
                        "法人方向": op.get("法人方向",""),
                        "法人共振": op.get("法人共振",""),
                        "盤前快照時間": op.get("盤前快照時間",""),
                        "盤前快照結論": op.get("盤前快照結論",""),
                        "盤前快照訊號": op.get("盤前快照訊號",""),
                        "盤前快照風報比": op.get("盤前快照風報比",""),
                        "盤後快照時間": op.get("盤後快照時間",""),
                        "盤後快照結論": op.get("盤後快照結論",""),
                        "盤後快照訊號": op.get("盤後快照訊號",""),
                        "盤後快照風報比": op.get("盤後快照風報比",""),
                    })
                    op["剩餘數量"] = int(op.get("剩餘數量", 0) or 0) - matched
                    qty_to_close -= matched
                    if int(op.get("剩餘數量", 0) or 0) <= 0:
                        opens.pop(0)

        for op in opens:
            remaining = int(op.get("剩餘數量", 0) or 0)
            if remaining > 0:
                open_pos.append({
                    "股票": code, "方向": side, "開倉時間": op.get("時間",""), "開倉價": float(op.get("價格", 0) or 0),
                    "剩餘數量": remaining,
                    "計畫來源": op.get("計畫來源",""),
                    "計畫進場": op.get("計畫進場",""),
                    "計畫停損": op.get("計畫停損",""),
                    "計畫短壓": op.get("計畫短壓",""),
                    "計畫中繼": op.get("計畫中繼",""),
                    "計畫突破": op.get("計畫突破",""),
                    "計畫風報比": op.get("計畫風報比",""),
                    "計畫結論": op.get("計畫結論",""),
                    "計畫訊號": op.get("計畫訊號",""),
                    "法人方向": op.get("法人方向",""),
                    "法人共振": op.get("法人共振",""),
                    "盤前快照時間": op.get("盤前快照時間",""),
                    "盤前快照結論": op.get("盤前快照結論",""),
                    "盤前快照訊號": op.get("盤前快照訊號",""),
                    "盤前快照風報比": op.get("盤前快照風報比",""),
                    "盤後快照時間": op.get("盤後快照時間",""),
                    "盤後快照結論": op.get("盤後快照結論",""),
                    "盤後快照訊號": op.get("盤後快照訊號",""),
                    "盤後快照風報比": op.get("盤後快照風報比",""),
                })
    return pd.DataFrame(closed), pd.DataFrame(open_pos)

def signal_stats(df_closed):
    if df_closed is None or df_closed.empty:
        return pd.DataFrame()
    work = df_closed.copy()
    out = []
    for side in ["做多", "做空"]:
        sub = work[work["方向"] == side]
        if sub.empty:
            continue
        wins = int((sub["損益"] > 0).sum())
        total = int(len(sub))
        out.append({
            "方向": side,
            "完成筆數": total,
            "勝率%": round(wins / total * 100, 2) if total else 0,
            "累計損益": round(sub["損益"].sum(), 2),
            "平均報酬率%": round(sub["報酬率%"].mean(), 2) if total else 0,
        })
    return pd.DataFrame(out)

def summary_stats(df_closed):
    if df_closed.empty:
        return 0.0, 0, 0.0, 0.0
    wins = (df_closed["損益"] > 0).sum()
    return round(df_closed["損益"].sum(), 2), int(len(df_closed)), round(wins / len(df_closed) * 100, 2), round(df_closed["報酬率%"].mean(), 2)

def classify_compare_outcome(row: dict) -> str:
    stop = str(row.get("停損觸發", "") or "")
    change = str(row.get("結構變化", row.get("變化判斷", "")) or "")
    if stop == "已觸發":
        return "失真"
    if change == "變強":
        return "命中"
    if change == "持平":
        return "中性"
    if change == "變弱":
        return "失真"
    return "中性"

def build_snapshot_validation_summary(compare_df: pd.DataFrame) -> dict:
    if compare_df is None or compare_df.empty:
        return {}
    work = compare_df.copy()
    work["驗證結果"] = [classify_compare_outcome(r) for r in work.to_dict(orient="records")]
    mode = str(work.get("快照邏輯", pd.Series([""])).iloc[0] if not work.empty else "")
    hit = int((work["驗證結果"] == "命中").sum())
    neutral = int((work["驗證結果"] == "中性").sum())
    miss = int((work["驗證結果"] == "失真").sum())
    effective = hit + miss
    hit_rate = round(hit / effective * 100, 2) if effective else None
    top_hit = "、".join(work.loc[work["驗證結果"] == "命中", "股票"].astype(str).head(3).tolist()) if "股票" in work.columns else ""
    top_miss = "、".join(work.loc[work["驗證結果"] == "失真", "股票"].astype(str).head(3).tolist()) if "股票" in work.columns else ""
    return {
        "mode": mode or "多方",
        "total": int(len(work)),
        "hit": hit,
        "neutral": neutral,
        "miss": miss,
        "hit_rate": hit_rate,
        "top_hit": top_hit,
        "top_miss": top_miss,
        "result_df": work,
    }

def light_color(level: str) -> str:
    return {"綠燈": "#16a34a", "黃燈": "#eab308", "紅燈": "#dc2626"}.get(level, "#6b7280")

def signal_light_pack(row: pd.Series):
    trend_light = "綠燈" if row["結論"] in ["看多", "中性偏多"] else "黃燈" if row["結論"] == "中性" else "紅燈"
    entry_light = "綠燈" if row["交易訊號"] == "🔥進場" else "黃燈" if row["交易訊號"] in ["👀觀察", "⏳等待"] else "紅燈"
    volume_light = "綠燈" if row["量能變化"] == "量增" and row["量比5日"] >= 1 else "黃燈" if row["量比5日"] >= 0.8 else "紅燈"
    pv_light = row.get("價量燈號", "黃燈")
    kd_light = "紅燈" if row["KD_K"] >= 80 else "綠燈" if row["KD_K"] > row["KD_D"] else "黃燈"
    rr_light = "綠燈" if row["風報比"] >= 1.8 else "黃燈" if row["風報比"] >= 1.2 else "紅燈"
    return {
        "趨勢燈號": trend_light,
        "進場燈號": entry_light,
        "量能燈號": volume_light,
        "價量燈號": pv_light,
        "KD燈號": kd_light,
        "風報燈號": rr_light
    }

def render_signal_lights(row: pd.Series):
    lights = signal_light_pack(row)
    cols = st.columns(len(lights))
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


def _detail_select_label(row_like) -> str:
    # 「股票」欄已是「CODE（名稱）」格式，直接用即可，不重複加代碼
    return str(row_like.get("股票", row_like.get("_code", "")))


def move_selected(select_source: pd.DataFrame, step: int):
    if select_source.empty:
        return
    codes = select_source["_code"].tolist()
    labels = [_detail_select_label(r) for _, r in select_source.iterrows()]
    ensure_selected_code(select_source)
    try:
        idx = codes.index(st.session_state.selected_code)
    except ValueError:
        idx = 0
    new_idx = max(0, min(len(codes) - 1, idx + step))
    st.session_state.selected_code = codes[new_idx]
    # selectbox 用 index= 跟著 selected_code 自動對齊，不需要直接寫 widget key


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


def download_intraday(symbol: str):
    def _normalize_intraday(df):
        if df is None or df.empty:
            return pd.DataFrame()
        df = df.reset_index().copy()
        time_col = "Datetime" if "Datetime" in df.columns else ("Date" if "Date" in df.columns else df.columns[0])
        df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
        if hasattr(df[time_col].dt, "tz") and df[time_col].dt.tz is not None:
            try:
                df[time_col] = df[time_col].dt.tz_convert("Asia/Taipei").dt.tz_localize(None)
            except Exception:
                try:
                    df[time_col] = df[time_col].dt.tz_localize(None)
                except Exception:
                    pass
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            if col not in df.columns:
                df[col] = pd.NA
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=[time_col, "High", "Low", "Close"]).sort_values(time_col).reset_index(drop=True)
        if time_col != "Datetime":
            df = df.rename(columns={time_col: "Datetime"})
        return df

    try:
        t = yf.Ticker(symbol)

        # 優先抓 1 分 K，模擬進場較準；抓不到再退回 5 分 K
        for interval in ["1m", "2m", "5m"]:
            try:
                df = t.history(period="1d", interval=interval, auto_adjust=False)
                df = _normalize_intraday(df)
                if df is not None and not df.empty:
                    return df
            except Exception:
                continue
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

def simulate_entry_from_intraday(symbol: str, entry_price, stop_price=None):
    result = {
        "是否可成交": "資料不足",
        "進場時間": "--",
        "模擬收盤價": "",
        "收盤模擬損益": "",
        "收盤模擬報酬率%": "",
        "收盤模擬結果": "資料不足",
        "模擬最高價": "",
        "最高模擬損益": "",
        "最高模擬報酬率%": "",
        "最高模擬結果": "資料不足",
        "模擬最低價": "",
        "最低模擬損益": "",
        "最低模擬報酬率%": "",
        "最低模擬結果": "資料不足",
        "停損觸發": "資料不足",
    }
    try:
        entry_f = float(entry_price)
    except Exception:
        return result
    if entry_f <= 0:
        result["是否可成交"] = "未成交"
        result["收盤模擬結果"] = "未成交"
        result["最高模擬結果"] = "未成交"
        result["最低模擬結果"] = "未成交"
        result["停損觸發"] = "--"
        return result

    df_intra = download_intraday(symbol)
    if df_intra is None or df_intra.empty:
        return result

    hit_mask = df_intra["Low"] <= entry_f
    if not hit_mask.any():
        result["是否可成交"] = "未成交"
        result["收盤模擬結果"] = "未成交"
        result["最高模擬結果"] = "未成交"
        result["最低模擬結果"] = "未成交"
        result["停損觸發"] = "--"
        return result
    first_idx = hit_mask[hit_mask].index[0]
    entry_bar_df = df_intra.loc[[first_idx]].copy()
    after_df = df_intra.loc[first_idx + 1:].copy()

    entry_time = entry_bar_df.iloc[0]["Datetime"]
    if pd.notna(entry_time):
        try:
            entry_time_str = pd.to_datetime(entry_time).strftime("%H:%M")
        except Exception:
            entry_time_str = str(entry_time)
    else:
        entry_time_str = "--"
    # 這裡改用 Close 序列做模擬最高/最低，
    # 因為使用者實際對照的是分時成交走勢線，而不是每根K棒內部 High/Low。
    if after_df.empty:
        after_close = round(float(entry_f), 1)
        after_high = round(float(entry_f), 1)
        after_low = round(float(entry_f), 1)
    else:
        close_series = pd.to_numeric(after_df["Close"], errors="coerce").dropna()
        if close_series.empty:
            after_close = round(float(entry_f), 1)
            after_high = round(float(entry_f), 1)
            after_low = round(float(entry_f), 1)
        else:
            after_close = round(float(close_series.iloc[-1]), 1)
            after_high = round(float(max(entry_f, close_series.max())), 1)
            after_low = round(float(min(entry_f, close_series.min())), 1)

    close_pnl = round(after_close - entry_f, 2)
    close_ret = round((after_close - entry_f) / entry_f * 100, 2)
    high_pnl = round(after_high - entry_f, 2)
    high_ret = round((after_high - entry_f) / entry_f * 100, 2)
    low_pnl = round(after_low - entry_f, 2)
    low_ret = round((after_low - entry_f) / entry_f * 100, 2)

    result.update({
        "是否可成交": "可成交",
        "進場時間": entry_time_str,
        "模擬收盤價": after_close,
        "收盤模擬損益": close_pnl,
        "收盤模擬報酬率%": close_ret,
        "收盤模擬結果": "上漲" if close_pnl > 0 else ("下跌" if close_pnl < 0 else "持平"),
        "模擬最高價": after_high,
        "最高模擬損益": high_pnl,
        "最高模擬報酬率%": high_ret,
        "最高模擬結果": "上漲" if high_pnl > 0 else ("下跌" if high_pnl < 0 else "持平"),
        "模擬最低價": after_low,
        "最低模擬損益": low_pnl,
        "最低模擬報酬率%": low_ret,
        "最低模擬結果": "上漲" if low_pnl > 0 else ("下跌" if low_pnl < 0 else "持平"),
    })

    try:
        stop_f = float(stop_price)
        result["停損觸發"] = "已觸發" if after_low <= stop_f else "未觸發"
    except Exception:
        result["停損觸發"] = "--"

    return result


def simulate_short_entry_from_intraday(symbol: str, entry_price, stop_price=None):
    result = {
        "是否可成交": "資料不足",
        "進場時間": "--",
        "模擬收盤價": "",
        "收盤模擬損益": "",
        "收盤模擬報酬率%": "",
        "收盤模擬結果": "資料不足",
        "模擬最高價": "",
        "最高模擬損益": "",
        "最高模擬報酬率%": "",
        "最高模擬結果": "資料不足",
        "模擬最低價": "",
        "最低模擬損益": "",
        "最低模擬報酬率%": "",
        "最低模擬結果": "資料不足",
        "停損觸發": "資料不足",
    }
    try:
        entry_f = float(entry_price)
    except Exception:
        return result
    if entry_f <= 0:
        result["是否可成交"] = "未成交"
        result["收盤模擬結果"] = "未成交"
        result["最高模擬結果"] = "未成交"
        result["最低模擬結果"] = "未成交"
        result["停損觸發"] = "--"
        return result

    df_intra = download_intraday(symbol)
    if df_intra is None or df_intra.empty:
        return result

    hit_mask = pd.to_numeric(df_intra["High"], errors="coerce") >= entry_f
    if not hit_mask.any():
        result["是否可成交"] = "未成交"
        result["收盤模擬結果"] = "未成交"
        result["最高模擬結果"] = "未成交"
        result["最低模擬結果"] = "未成交"
        result["停損觸發"] = "--"
        return result

    first_idx = hit_mask[hit_mask].index[0]
    entry_bar_df = df_intra.loc[[first_idx]].copy()
    after_df = df_intra.loc[first_idx + 1:].copy()

    entry_time = entry_bar_df.iloc[0]["Datetime"]
    if pd.notna(entry_time):
        try:
            entry_time_str = pd.to_datetime(entry_time).strftime("%H:%M")
        except Exception:
            entry_time_str = str(entry_time)
    else:
        entry_time_str = "--"

    if after_df.empty:
        after_close = round(float(entry_f), 1)
        after_high = round(float(entry_f), 1)
        after_low = round(float(entry_f), 1)
    else:
        close_series = pd.to_numeric(after_df["Close"], errors="coerce").dropna()
        if close_series.empty:
            after_close = round(float(entry_f), 1)
            after_high = round(float(entry_f), 1)
            after_low = round(float(entry_f), 1)
        else:
            after_close = round(float(close_series.iloc[-1]), 1)
            after_high = round(float(max(entry_f, close_series.max())), 1)
            after_low = round(float(min(entry_f, close_series.min())), 1)

    close_pnl = round(entry_f - after_close, 2)
    close_ret = round((entry_f - after_close) / entry_f * 100, 2)
    high_pnl = round(entry_f - after_high, 2)
    high_ret = round((entry_f - after_high) / entry_f * 100, 2)
    low_pnl = round(entry_f - after_low, 2)
    low_ret = round((entry_f - after_low) / entry_f * 100, 2)

    result.update({
        "是否可成交": "可成交",
        "進場時間": entry_time_str,
        "模擬收盤價": after_close,
        "收盤模擬損益": close_pnl,
        "收盤模擬報酬率%": close_ret,
        "收盤模擬結果": "獲利" if close_pnl > 0 else ("虧損" if close_pnl < 0 else "持平"),
        "模擬最高價": after_high,
        "最高模擬損益": high_pnl,
        "最高模擬報酬率%": high_ret,
        "最高模擬結果": "獲利" if high_pnl > 0 else ("虧損" if high_pnl < 0 else "持平"),
        "模擬最低價": after_low,
        "最低模擬損益": low_pnl,
        "最低模擬報酬率%": low_ret,
        "最低模擬結果": "獲利" if low_pnl > 0 else ("虧損" if low_pnl < 0 else "持平"),
    })

    try:
        stop_f = float(stop_price)
        result["停損觸發"] = "已觸發" if after_high >= stop_f else "未觸發"
    except Exception:
        result["停損觸發"] = "--"

    return result


def make_intraday_figure(df_intra: pd.DataFrame, row: pd.Series):
    fig = go.Figure()
    if df_intra is None or df_intra.empty:
        fig.update_layout(
            template="plotly_dark",
            height=420 if st.session_state.get("mobile_mode", False) else 520,
            title="當日走勢圖（目前抓不到盤中資料）",
            margin=dict(l=20, r=20, t=50, b=20)
        )
        return fig

    time_col = "Datetime" if "Datetime" in df_intra.columns else df_intra.columns[0]
    vol_series = df_intra["Volume"] if "Volume" in df_intra.columns else pd.Series([0]*len(df_intra))
    close_series = df_intra["Close"] if "Close" in df_intra.columns else pd.Series([0]*len(df_intra))

    fig.add_trace(go.Scatter(
        x=df_intra[time_col], y=close_series,
        mode="lines", name="當日價格"
    ))
    fig.add_trace(go.Bar(
        x=df_intra[time_col], y=vol_series / 1000,
        name="分時量(張)", yaxis="y2", opacity=0.4
    ))

    prev_close = row["收盤"] if "收盤" in row else None
    if prev_close:
        fig.add_hline(y=float(prev_close), line_dash="dot", annotation_text="參考價")

    fig.update_layout(
        template="plotly_dark",
        height=420 if st.session_state.get("mobile_mode", False) else 520,
        margin=dict(l=20, r=20, t=50, b=20),
        legend=dict(orientation="h"),
        yaxis=dict(title="價格"),
        yaxis2=dict(title="分時量(張)", overlaying="y", side="right", showgrid=False),
        xaxis=dict(title="")
    )
    return fig

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



TWSE_LATEST_DIR = Path("twse_test_outputs/latest")
TWSE_LATEST_CLEAN_CSV = TWSE_LATEST_DIR / "twse_stock_day_latest_clean.csv"
TWSE_LATEST_META_JSON = TWSE_LATEST_DIR / "twse_stock_day_latest_meta.json"
OFFICIAL_COMBINED_CSV = TWSE_LATEST_DIR / "official_stock_day_latest_combined.csv"
OFFICIAL_COMBINED_META_JSON = TWSE_LATEST_DIR / "official_stock_day_latest_meta.json"
TPEX_DAILY_HTML_URL = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&o=htm&se=EW"
TPEX_DAILY_CSV_URL = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?l=zh-tw&o=csv&se=EW"


def _safe_twse_json(path: Path):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return {}


def _normalize_merge_code(value):
    text = str(value or "").strip().upper()
    if not text:
        return ""
    text = text.split("（")[0].strip()
    if "." in text:
        text = text.split(".", 1)[0].strip()
    return text


def _to_number_or_none(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        return float(value)
    s = str(value).strip().replace(",", "")
    if s in ["", "nan", "None", "N/A", "--", "---"]:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _roc_to_ad(date_text: str) -> str:
    s = str(date_text or "").strip()
    if not s:
        return ""
    if "/" in s:
        parts = [p for p in s.split("/") if p]
        if len(parts) == 3 and all(part.isdigit() for part in parts):
            y = int(parts[0])
            if y < 1911:
                y += 1911
            return f"{y:04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"
    digits = re.sub(r"\D", "", s)
    if len(digits) == 7:
        y = int(digits[:3]) + 1911
        return f"{y:04d}-{digits[3:5]}-{digits[5:7]}"
    if len(digits) == 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return s


def _flatten_columns_list(columns):
    flat = []
    for col in columns:
        if isinstance(col, tuple):
            pieces = [str(x).strip() for x in col if str(x).strip() and str(x).strip().lower() != "nan"]
            flat.append("".join(pieces))
        else:
            flat.append(str(col).strip())
    return flat


def _ensure_snapshot_types(df: pd.DataFrame):
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    if "code" not in df.columns:
        return pd.DataFrame()
    df["code"] = df["code"].astype(str).str.strip()
    if "date" in df.columns:
        df["date"] = df["date"].astype(str).map(_roc_to_ad).str.strip()
    else:
        df["date"] = ""
    if "name" in df.columns:
        df["name"] = df["name"].astype(str).str.strip()
    else:
        df["name"] = ""
    if "source" in df.columns:
        df["source"] = df["source"].astype(str).str.strip()
    else:
        df["source"] = ""
    numeric_cols = ["open", "high", "low", "close", "volume", "value", "change", "transactions"]
    for col in numeric_cols:
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["_merge_code"] = df["code"].astype(str).map(_normalize_merge_code)
    df = df[df["_merge_code"] != ""]
    df = df.drop_duplicates(subset=["_merge_code"], keep="last")
    return df


def _fetch_text_with_ssl(url: str, *, headers=None):
    headers = headers or {}
    meta = {"url": url, "status_code": None, "ssl_mode": "failed", "ssl_note": ""}
    try:
        resp = requests.get(url, headers=headers, timeout=20, verify=True)
        meta["status_code"] = resp.status_code
        if resp.ok and resp.text.strip():
            meta["ssl_mode"] = "strict"
            return resp.text, meta
        meta["ssl_note"] = f"HTTP {resp.status_code}"
    except Exception as e:
        meta["ssl_note"] = str(e)
    try:
        resp = requests.get(url, headers=headers, timeout=20, verify=False)
        meta["status_code"] = resp.status_code
        if resp.ok and resp.text.strip():
            meta["ssl_mode"] = "insecure_fallback"
            return resp.text, meta
        if not meta.get("ssl_note"):
            meta["ssl_note"] = f"HTTP {resp.status_code}"
    except Exception as e:
        if not meta.get("ssl_note"):
            meta["ssl_note"] = str(e)
    return "", meta


@st.cache_data(ttl=300, show_spinner=False)
def load_local_twse_snapshot():
    if not TWSE_LATEST_CLEAN_CSV.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(TWSE_LATEST_CLEAN_CSV, dtype={"code": str}, encoding="utf-8-sig")
    except Exception:
        try:
            df = pd.read_csv(TWSE_LATEST_CLEAN_CSV, dtype={"code": str})
        except Exception:
            return pd.DataFrame()
    return _ensure_snapshot_types(df)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_twse_live_snapshot():
    url = f"{TWSE_OPENAPI_BASE}/exchangeReport/STOCK_DAY_ALL"
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json, text/plain, */*", "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8"}
    text, conn = _fetch_text_with_ssl(url, headers=headers)
    meta = {"market": "TWSE", "endpoint": "exchangeReport/STOCK_DAY_ALL", "ssl_mode": conn.get("ssl_mode", "failed"), "ssl_note": conn.get("ssl_note", ""), "status_code": conn.get("status_code")}
    if not text:
        return pd.DataFrame(), meta
    try:
        data = json.loads(text)
    except Exception as e:
        meta["parse_error"] = str(e)
        return pd.DataFrame(), meta
    if isinstance(data, dict):
        for key in ["data", "rows", "items", "result", "results"]:
            if isinstance(data.get(key), list):
                data = data.get(key)
                break
        else:
            data = [data]
    if not isinstance(data, list):
        return pd.DataFrame(), meta
    df = pd.DataFrame(data)
    if df.empty:
        return pd.DataFrame(), meta
    rename_map = {"Date": "date", "Code": "code", "Name": "name", "TradeVolume": "volume", "TradeValue": "value", "OpeningPrice": "open", "HighestPrice": "high", "LowestPrice": "low", "ClosingPrice": "close", "Change": "change", "Transaction": "transactions"}
    existing = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=existing)
    keep = ["date", "code", "name", "volume", "value", "open", "high", "low", "close", "change", "transactions"]
    for col in keep:
        if col not in df.columns:
            df[col] = None
    df = df[keep].copy()
    df["source"] = "TWSE_STOCK_DAY_ALL"
    df = _ensure_snapshot_types(df)
    meta["rows"] = int(len(df))
    if not df.empty and "date" in df.columns:
        meta["latest_date"] = str(df["date"].dropna().astype(str).max())
    return df, meta


def _extract_tpex_date(text: str) -> str:
    m = re.search(r"資料日期[:：]\s*([0-9]{2,3}/[0-9]{2}/[0-9]{2})", text or "")
    if m:
        return _roc_to_ad(m.group(1))
    return ""


def _parse_tpex_html_table(text: str) -> pd.DataFrame:
    try:
        tables = pd.read_html(StringIO(text))
    except Exception:
        return pd.DataFrame()
    for t in tables:
        tt = t.copy()
        tt.columns = _flatten_columns_list(tt.columns)
        cols = set(tt.columns)
        if {"代號", "名稱", "收盤"}.issubset(cols) and ("成交股數" in cols or "成交股數(股)" in cols):
            return tt
    return pd.DataFrame()


def _parse_tpex_csv_table(text: str) -> pd.DataFrame:
    if not text:
        return pd.DataFrame()
    raw = text.replace("﻿", "")
    lines = [ln.strip() for ln in raw.splitlines() if ln.strip()]
    if not lines:
        return pd.DataFrame()
    header_idx = None
    for i, ln in enumerate(lines):
        if "代號" in ln and "名稱" in ln and "收盤" in ln:
            header_idx = i
            break
    if header_idx is None:
        return pd.DataFrame()
    rows = []
    import csv
    for ln in lines[header_idx:]:
        if ln.startswith("註") or ln.startswith("共"):
            break
        try:
            parsed = next(csv.reader([ln]))
        except Exception:
            continue
        if parsed:
            rows.append([cell.strip() for cell in parsed])
    if len(rows) <= 1:
        return pd.DataFrame()
    header = rows[0]
    body = rows[1:]
    width = len(header)
    body = [r[:width] + [""] * max(0, width - len(r)) for r in body if any(x.strip() for x in r)]
    try:
        return pd.DataFrame(body, columns=header)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def fetch_tpex_live_snapshot():
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,text/csv,*/*;q=0.8", "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8", "Referer": "https://www.tpex.org.tw/"}
    candidates = [
        (TPEX_DAILY_HTML_URL, "html"),
        (TPEX_DAILY_CSV_URL, "csv"),
        ("https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?d=&l=zh-tw&o=htm", "html_legacy"),
        ("https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php?d=&l=zh-tw&o=csv", "csv_legacy"),
        ("https://www.tpex.org.tw/zh-tw/mainboard/trading/info/mi-pricing.html", "html_page"),
    ]
    best_meta = {"market": "TPEx", "endpoint": TPEX_DAILY_HTML_URL, "ssl_mode": "failed", "ssl_note": "", "status_code": None}
    for url, kind in candidates:
        text, conn = _fetch_text_with_ssl(url, headers=headers)
        meta = {"market": "TPEx", "endpoint": url, "ssl_mode": conn.get("ssl_mode", "failed"), "ssl_note": conn.get("ssl_note", ""), "status_code": conn.get("status_code")}
        if not text:
            best_meta = meta
            continue
        page_date = _extract_tpex_date(text)
        if page_date:
            meta["latest_date"] = page_date
        target = _parse_tpex_html_table(text) if kind != "csv" else _parse_tpex_csv_table(text)
        if target.empty and kind == "html":
            target = _parse_tpex_csv_table(text)
        if target.empty:
            best_meta = meta
            continue
        rename_map = {"代號": "code", "名稱": "name", "收盤": "close", "漲跌": "change", "開盤": "open", "最高": "high", "最低": "low", "成交股數": "volume", "成交股數(股)": "volume", "成交金額(元)": "value", "成交金額": "value", "成交筆數": "transactions"}
        existing = {k: v for k, v in rename_map.items() if k in target.columns}
        df = target.rename(columns=existing)
        keep = ["code", "name", "close", "change", "open", "high", "low", "volume", "value", "transactions"]
        for col in keep:
            if col not in df.columns:
                df[col] = None
        df = df[keep].copy()
        df["date"] = page_date
        df["source"] = "TPEx_MI_PRICING"
        df = df[["date", "code", "name", "volume", "value", "open", "high", "low", "close", "change", "transactions", "source"]]
        df["code"] = df["code"].astype(str).str.strip()
        df = df[df["code"].str.match(r"^[0-9A-Z]{4,6}$", na=False)]
        df = _ensure_snapshot_types(df)
        if not df.empty:
            meta["rows"] = int(len(df))
            if not meta.get("latest_date") and "date" in df.columns and not df["date"].dropna().empty:
                meta["latest_date"] = str(df["date"].dropna().astype(str).max())
            return df, meta
        best_meta = meta
    return pd.DataFrame(), best_meta


def _build_combined_meta(local_meta, twse_meta, tpex_meta, combined_df):
    latest_dates = []
    for obj in [local_meta, twse_meta, tpex_meta]:
        if isinstance(obj, dict) and obj.get("latest_date"):
            latest_dates.append(str(obj.get("latest_date")))
    if combined_df is not None and not combined_df.empty and "date" in combined_df.columns:
        latest_dates.extend([d for d in combined_df["date"].astype(str).tolist() if d and d != "nan"])
    latest_date = max(latest_dates) if latest_dates else ""
    return {
        "latest_date": latest_date,
        "ssl_mode": f"TWSE:{(twse_meta or {}).get('ssl_mode', 'na')} / TPEx:{(tpex_meta or {}).get('ssl_mode', 'na')}",
        "twse_ssl_mode": (twse_meta or {}).get("ssl_mode", "na"),
        "tpex_ssl_mode": (tpex_meta or {}).get("ssl_mode", "na"),
        "sources": ["local_twse_latest_clean" if local_meta else "", "twse_live" if (twse_meta or {}).get("rows") else "", "tpex_live" if (tpex_meta or {}).get("rows") else ""],
        "twse_rows": int((twse_meta or {}).get("rows", 0) or 0),
        "tpex_rows": int((tpex_meta or {}).get("rows", 0) or 0),
        "local_latest_date": str((local_meta or {}).get("latest_date", "")),
        "twse_latest_date": str((twse_meta or {}).get("latest_date", "")),
        "tpex_latest_date": str((tpex_meta or {}).get("latest_date", "")),
        "twse_note": str((twse_meta or {}).get("ssl_note", "")),
        "tpex_note": str((tpex_meta or {}).get("ssl_note", "")),
        "combined_rows": int(len(combined_df)) if combined_df is not None else 0,
    }


@st.cache_data(ttl=300, show_spinner=False)
def load_latest_snapshot():
    local_df = load_local_twse_snapshot()
    local_meta = _safe_twse_json(TWSE_LATEST_META_JSON)
    twse_live_df, twse_meta = fetch_twse_live_snapshot()
    tpex_live_df, tpex_meta = fetch_tpex_live_snapshot()
    frames = []
    for df in [local_df, twse_live_df, tpex_live_df]:
        if df is not None and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined = _ensure_snapshot_types(combined)
    try:
        TWSE_LATEST_DIR.mkdir(parents=True, exist_ok=True)
        combined.to_csv(OFFICIAL_COMBINED_CSV, index=False, encoding="utf-8-sig")
        OFFICIAL_COMBINED_META_JSON.write_text(json.dumps(_build_combined_meta(local_meta, twse_meta, tpex_meta, combined), ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass
    return combined


@st.cache_data(ttl=300, show_spinner=False)
def load_latest_meta():
    local_meta = _safe_twse_json(TWSE_LATEST_META_JSON)
    cached_meta = _safe_twse_json(OFFICIAL_COMBINED_META_JSON)
    twse_live_df, twse_meta = fetch_twse_live_snapshot()
    tpex_live_df, tpex_meta = fetch_tpex_live_snapshot()
    frames = []
    for df in [load_local_twse_snapshot(), twse_live_df, tpex_live_df]:
        if df is not None and not df.empty:
            frames.append(df)
    combined = pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()
    meta = _build_combined_meta(local_meta, twse_meta, tpex_meta, combined)
    if cached_meta:
        meta.update({k: v for k, v in cached_meta.items() if k not in meta or meta.get(k) in [None, "", [], 0]})
    return meta if isinstance(meta, dict) else {}


def enrich_analysis_rows(rows, latest_snapshot_df: pd.DataFrame):
    if not rows:
        return rows
    if latest_snapshot_df is None or latest_snapshot_df.empty or "_merge_code" not in latest_snapshot_df.columns:
        enriched = []
        for row in rows:
            item = dict(row)
            item["TWSE命中"] = "否"
            item["TWSE日期"] = ""
            item["TWSE名稱"] = ""
            item["TWSE收盤差異"] = ""
            enriched.append(item)
        return enriched

    snap = latest_snapshot_df.set_index("_merge_code", drop=False)
    enriched = []
    for row in rows:
        item = dict(row)
        raw_code = item.get("_code") or item.get("股票代碼") or item.get("股票") or ""
        merge_code = _normalize_merge_code(raw_code)
        if merge_code in snap.index:
            s = snap.loc[merge_code]
            if isinstance(s, pd.DataFrame):
                s = s.iloc[-1]
            item["TWSE命中"] = "是"
            item["TWSE日期"] = str(s.get("date", "") or "")
            item["TWSE名稱"] = str(s.get("name", "") or "")
            item["TWSE開盤"] = s.get("open", "")
            item["TWSE最高"] = s.get("high", "")
            item["TWSE最低"] = s.get("low", "")
            item["TWSE收盤"] = s.get("close", "")
            item["TWSE成交量"] = s.get("volume", "")
            item["TWSE成交值"] = s.get("value", "")
            item["TWSE漲跌價差"] = s.get("change", "")
            item["TWSE成交筆數"] = s.get("transactions", "")
            item["TWSE來源"] = s.get("source", "")
            base_close = _to_number_or_none(item.get("收盤"))
            twse_close = _to_number_or_none(s.get("close"))
            item["TWSE收盤差異"] = round(twse_close - base_close, 2) if base_close is not None and twse_close is not None else ""
        else:
            item["TWSE命中"] = "否"
            item["TWSE日期"] = ""
            item["TWSE名稱"] = ""
            item["TWSE開盤"] = ""
            item["TWSE最高"] = ""
            item["TWSE最低"] = ""
            item["TWSE收盤"] = ""
            item["TWSE成交量"] = ""
            item["TWSE成交值"] = ""
            item["TWSE漲跌價差"] = ""
            item["TWSE成交筆數"] = ""
            item["TWSE來源"] = ""
            item["TWSE收盤差異"] = ""
        enriched.append(item)
    return enriched


TWSE_RESULT_DEFAULTS = {"TWSE命中": "否", "TWSE日期": "", "TWSE名稱": "", "TWSE開盤": "", "TWSE最高": "", "TWSE最低": "", "TWSE收盤": "", "TWSE成交量": "", "TWSE成交值": "", "TWSE漲跌價差": "", "TWSE成交筆數": "", "TWSE來源": "", "TWSE收盤差異": ""}


def ensure_twse_result_columns(rows):
    safe_rows = []
    for row in rows or []:
        item = dict(row)
        for k, v in TWSE_RESULT_DEFAULTS.items():
            if k not in item:
                item[k] = v
        safe_rows.append(item)
    return safe_rows


def ensure_dataframe_columns(df: pd.DataFrame, columns, fill_value=""):
    if df is None:
        return pd.DataFrame(columns=list(columns))
    df = df.copy()
    for col in columns:
        if col not in df.columns:
            df[col] = fill_value
    return df


def try_apply_twse_hybrid(rows):
    safe_rows = ensure_twse_result_columns(rows)
    meta = load_latest_meta()
    try:
        snap = load_latest_snapshot()
        if snap is None or snap.empty:
            return safe_rows, meta, 0
        enriched = enrich_analysis_rows(safe_rows, snap)
        enriched = ensure_twse_result_columns(enriched)
        hit_count = sum(1 for r in enriched if str(r.get("TWSE命中", "")) == "是")
        return enriched, meta, hit_count
    except Exception as e:
        meta = dict(meta or {})
        meta["hybrid_error"] = str(e)
        return safe_rows, meta, 0


def load_cached_official_meta():
    meta = _safe_twse_json(OFFICIAL_COMBINED_META_JSON)
    if not isinstance(meta, dict) or not meta:
        meta = _safe_twse_json(TWSE_LATEST_META_JSON)
    return meta if isinstance(meta, dict) else {}


def enrich_with_institutional(items: list[dict]) -> list[dict]:
    """
    批次將三大法人資料補進 analyze_one 產出的結果。
    bundle 由 fetch_institutional_bundle_all 提供（已有 ttl=120 快取）。
    失敗時靜默略過，不影響主流程。
    """
    if not items:
        return items
    try:
        bundle = fetch_institutional_bundle_all()
    except Exception:
        return items
    enriched = []
    for item in items:
        try:
            code = str(item.get("_code") or item.get("股票代碼") or "").strip()
            if code:
                item = resolve_institutional_context(code, item, bundle)
        except Exception:
            pass
        enriched.append(item)
    return enriched



ensure_names_file()
name_map = load_name_map()
if "favorites" not in st.session_state:
    st.session_state.favorites = load_favorites()
if "results_data" not in st.session_state:
    st.session_state.results_data = []
if "last_liquidity_summary" not in st.session_state:
    st.session_state.last_liquidity_summary = {}
if "selected_code" not in st.session_state:
    st.session_state.selected_code = None
if "input_value" not in st.session_state:
    st.session_state.input_value = ""
if "analysis_mode" not in st.session_state:
    st.session_state.analysis_mode = "idle"
if "selected_side" not in st.session_state:
    st.session_state.selected_side = "long"
elif st.session_state.selected_side not in ("long", "short"):
    st.session_state.selected_side = "long"
if "preset_strategy" not in st.session_state:
    st.session_state.preset_strategy = "none"
if "mobile_mode" not in st.session_state:
    st.session_state.mobile_mode = True
if "manual_taiex_override" not in st.session_state:
    st.session_state.manual_taiex_override = 0.0
if "user_list" not in st.session_state:
    st.session_state.user_list = load_users()
if "current_user" not in st.session_state:
    st.session_state.current_user = st.session_state.user_list[0] if st.session_state.user_list else DEFAULT_USERS[0]
if "current_page" not in st.session_state:
    st.session_state.current_page = "首頁"
if "position_result" not in st.session_state:
    st.session_state.position_result = None
if "position_stock_input" not in st.session_state:
    st.session_state.position_stock_input = ""

inject_responsive_css()


def render_landing_page():
    # ── Hero ──
    st.markdown("""
    <div class="lp-hero">
        <h1>台股短線系統</h1>
        <div class="lp-sub">短線交易決策分析平台</div>
        <div class="lp-tags">
            <span class="lp-tag">做多 / 做空雙模式</span>
            <span class="lp-tag">100 分評級</span>
            <span class="lp-tag">法人動態</span>
            <span class="lp-tag">快照驗證</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        if st.button("🚀  開始分析", use_container_width=True, type="primary"):
            st.session_state.current_page = "分析中心"
            st.rerun()
    with c2:
        if st.button("📊  市場總覽", use_container_width=True):
            st.session_state.current_page = "市場儀表板"
            st.rerun()

    # ── 今日策略總覽 ──
    st.markdown('<div class="lp-divider"></div>', unsafe_allow_html=True)
    st.markdown('<div class="lp-section-title">📡 今日策略總覽</div>', unsafe_allow_html=True)

    try:
        mi = market_filter()
        mkt_close = mi.get("close", 0)
        mkt_label = mi.get("label", "--")
        mkt_adj = mi.get("score_adj", 0)

        is_bull = "多" in mkt_label
        is_bear = "空" in mkt_label
        direction_color = "#4ade80" if is_bull else "#f87171" if is_bear else "#fbbf24"
        direction_text = mkt_label
        strategy_text = "做多優先" if is_bull else "做空優先" if is_bear else "觀望為主"
        long_advantage = max(0, min(100, 50 + mkt_adj * 3 + (15 if is_bull else -10 if is_bear else 0)))
        short_advantage = max(0, min(100, 50 - mkt_adj * 3 + (15 if is_bear else -10 if is_bull else 0)))

        if is_bull:
            insight = "今日多方條件較完整，可優先觀察做多候選股。量能與趨勢若同步配合，短線勝率較高。"
        elif is_bear:
            insight = "今日空方結構較明確，建議觀察做空機會或降低持倉。避免逆勢追多。"
        else:
            insight = "今日多空力道拉鋸，建議等待方向明確再操作，或以小部位試單為主。"

        st.markdown(f"""
        <div class="lp-strategy-panel">
            <div class="lp-strategy-header">MARKET STRATEGY OVERVIEW</div>
            <div class="lp-strategy-row">
                <span class="lp-strategy-label"><span class="lp-dot"></span>加權指數</span>
                <span class="lp-strategy-value">{mkt_close:,.0f}</span>
            </div>
            <div class="lp-strategy-row">
                <span class="lp-strategy-label"><span class="lp-dot" style="background:{direction_color};"></span>今日市場偏向</span>
                <span class="lp-strategy-value" style="color:{direction_color};">{direction_text}</span>
            </div>
            <div class="lp-strategy-row" style="border-bottom:none;">
                <span class="lp-strategy-label"><span class="lp-dot" style="background:#a78bfa;"></span>優先策略</span>
                <span class="lp-strategy-value">{strategy_text}</span>
            </div>
            <div class="lp-score-cards">
                <div class="lp-score-card long">
                    <div class="lp-score-card-label">做多優勢</div>
                    <div class="lp-score-card-value">{long_advantage}</div>
                    <div class="lp-score-mini-bar"><div class="lp-score-mini-fill" style="width:{long_advantage}%;"></div></div>
                </div>
                <div class="lp-score-card short">
                    <div class="lp-score-card-label">做空優勢</div>
                    <div class="lp-score-card-value">{short_advantage}</div>
                    <div class="lp-score-mini-bar"><div class="lp-score-mini-fill" style="width:{short_advantage}%;"></div></div>
                </div>
            </div>
            <div class="lp-insight">{insight}</div>
        </div>
        """, unsafe_allow_html=True)
    except Exception:
        st.info("大盤資料載入中...")

    # ── 核心功能（可點擊）──
    st.markdown('<div class="lp-divider"></div>', unsafe_allow_html=True)
    st.markdown('<div class="lp-section-title">🧩 核心功能</div>', unsafe_allow_html=True)

    features = [
        ("🎯", "自動挑股", "做多 / 做空雙模式，五維評分 ≥ 60 分入選", "分析中心", "lp_feat_auto"),
        ("🔍", "單股分析", "輸入代碼即時運算，技術面＋法人＋策略", "分析中心", "lp_feat_manual"),
        ("📸", "快照中心", "盤前快照，盤後對照，追蹤準確度", "快照中心", "lp_feat_snap"),
        ("💼", "持倉中心", "持倉追蹤、損益計算、當沖判讀", "持倉中心", "lp_feat_pos"),
    ]
    for icon, title, desc, page, key in features:
        st.markdown(f"""
        <div class="lp-card">
            <div class="lp-card-icon">{icon}</div>
            <div class="lp-card-title">{title}</div>
            <div class="lp-card-desc">{desc}</div>
            <div class="lp-card-arrow">→</div>
        </div>
        """, unsafe_allow_html=True)
        if st.button(f"進入{title}", use_container_width=True, key=key):
            st.session_state.current_page = page
            st.rerun()

    # ── 快速入口（帶模式切換）──
    st.markdown('<div class="lp-divider"></div>', unsafe_allow_html=True)
    st.markdown('<div class="lp-section-title">⚡ 一鍵操作</div>', unsafe_allow_html=True)

    q1, q2 = st.columns(2)
    with q1:
        if st.button("📈 做多挑股", use_container_width=True, type="primary"):
            st.session_state.current_page = "分析中心"
            st.session_state["_landing_auto_mode"] = "做多模式"
            st.rerun()
    with q2:
        if st.button("📉 做空挑股", use_container_width=True, type="primary"):
            st.session_state.current_page = "分析中心"
            st.session_state["_landing_auto_mode"] = "做空模式"
            st.rerun()

    # ── 今日重大新聞 ──
    st.markdown('<div class="lp-divider"></div>', unsafe_allow_html=True)
    _news_hdr_l, _news_hdr_r = st.columns([6, 1])
    _news_hdr_l.markdown('<div class="lp-section-title">📰 今日重大新聞</div>', unsafe_allow_html=True)
    if _news_hdr_r.button("🔄", key="lp_news_refresh", help="手動更新新聞（每 5 分鐘自動快取）"):
        st.cache_data.clear()
        st.rerun()

    _news_error = False
    _news_items = None
    try:
        _news_items = fetch_tw_stock_news()
    except Exception:
        _news_error = True

    # 多空關鍵字
    _BULL_WORDS = ["漲", "多", "買", "升", "突破", "創高", "利多", "擴產", "合作", "訂單", "上修", "增溫", "樂觀", "受惠", "AI", "rally", "gain", "bull", "surge"]
    _BEAR_WORDS = ["跌", "空", "賣", "降", "下修", "衰退", "虧損", "利空", "賣壓", "疲弱", "下滑", "衝擊", "裁員", "訴訟", "fall", "drop", "bear", "crash"]

    if _news_error:
        st.markdown("""
        <div class="lp-state-card error">
            <div class="lp-state-icon">⚠️</div>
            <div class="lp-state-title">新聞載入失敗</div>
            <div class="lp-state-desc">鉅亨網與 Yahoo 奇摩股市 RSS 暫時無回應，請按右上角 🔄 重試</div>
        </div>
        """, unsafe_allow_html=True)
    elif _news_items:
        for n in _news_items[:6]:
            title_text  = htmllib.escape(n.get("title", ""))
            time_text   = htmllib.escape(n.get("time", ""))
            source_text = htmllib.escape(n.get("publisher", ""))
            link_url    = n.get("link", "")
            raw_title   = n.get("title", "").lower()
            if any(w in raw_title for w in _BULL_WORDS):
                badge_cls, badge_text = "bull", "偏多"
            elif any(w in raw_title for w in _BEAR_WORDS):
                badge_cls, badge_text = "bear", "偏空"
            else:
                badge_cls, badge_text = "neutral", "中性"
            title_html = f'<a href="{link_url}" target="_blank" style="color:inherit;text-decoration:none;">{title_text}</a>' if link_url else title_text
            st.markdown(f"""
            <div class="lp-news-item">
                <div class="lp-news-meta">
                    <span class="lp-news-badge {badge_cls}">{badge_text}</span>
                    <span class="lp-news-time">{time_text}</span>
                </div>
                <div class="lp-news-title">{title_html}</div>
                <div class="lp-news-source">{source_text}</div>
            </div>
            """, unsafe_allow_html=True)
        st.caption("資料來源：鉅亨網 / Yahoo 奇摩股市｜每 5 分鐘自動更新")
    else:
        st.markdown("""
        <div class="lp-state-card empty">
            <div class="lp-state-icon">📭</div>
            <div class="lp-state-title">今日暫無重大市場新聞</div>
            <div class="lp-state-desc">目前未取得資料，可能是市場休市或 RSS 暫無更新，請稍後按 🔄 重試</div>
        </div>
        """, unsafe_allow_html=True)

    # ── 操作流程（精簡版）──
    st.markdown('<div class="lp-divider"></div>', unsafe_allow_html=True)
    st.markdown('<div class="lp-section-title">🔄 操作流程</div>', unsafe_allow_html=True)

    st.markdown("""
    <div class="lp-steps-compact">
        <div class="lp-step-compact">
            <span class="lp-step-compact-num">1</span>
            <span class="lp-step-compact-title">選擇模式</span>
            <div class="lp-step-compact-desc">做多或做空自動篩選</div>
        </div>
        <div class="lp-step-compact">
            <span class="lp-step-compact-num">2</span>
            <span class="lp-step-compact-title">五維評分</span>
            <div class="lp-step-compact-desc">100 分制綜合評級</div>
        </div>
        <div class="lp-step-compact">
            <span class="lp-step-compact-num">3</span>
            <span class="lp-step-compact-title">精選結果</span>
            <div class="lp-step-compact-desc">總分排序＋策略建議</div>
        </div>
        <div class="lp-step-compact">
            <span class="lp-step-compact-num">4</span>
            <span class="lp-step-compact-title">深入研究</span>
            <div class="lp-step-compact-desc">技術面＋法人＋K線</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Footer ──
    st.markdown(f"""
    <div class="lp-divider"></div>
    <div class="lp-footer">
        台股短線系統 {APP_VERSION}　｜　資料來源：Yahoo Finance / TWSE / TPEx<br>
        <span style="font-size:0.72rem;">本系統僅供分析參考，不構成投資建議。投資有風險，請自行判斷。</span>
    </div>
    """, unsafe_allow_html=True)


def render_global_banner():
    """每頁頂部呼叫一次，顯示大盤狀態欄 + 版本資訊。"""
    _mi    = st.session_state.get("market_info_cache") or {}
    _close = _mi.get("close")
    _chg   = _mi.get("change") or _mi.get("index_change")
    _label = _mi.get("label", "")
    _page  = st.session_state.get("current_page", "")

    # 加權指數文字
    if _close:
        _chg_txt = ""
        if _chg is not None:
            _sign = "+" if _chg >= 0 else ""
            _color = "#4ade80" if _chg >= 0 else "#f87171"
            _chg_txt = f'　<span style="color:{_color};font-weight:700;">{_sign}{_chg:.0f}</span>'
        _idx_part = f'加權　<span style="color:#d8ecff;font-weight:800;">{_close:,.2f}</span>{_chg_txt}'
    else:
        _idx_part = '加權　<span style="color:#3a5570;">載入中…</span>'

    # 大盤濾網 pill
    if _label == "偏多":
        _pill = f'<span style="background:rgba(34,197,94,0.12);border:1px solid rgba(34,197,94,0.25);color:#4ade80;border-radius:99px;padding:2px 10px;font-size:0.76rem;font-weight:700;">▲ 偏多</span>'
    elif _label == "偏空":
        _pill = f'<span style="background:rgba(239,68,68,0.12);border:1px solid rgba(239,68,68,0.25);color:#f87171;border-radius:99px;padding:2px 10px;font-size:0.76rem;font-weight:700;">▼ 偏空</span>'
    elif _label:
        _pill = f'<span style="background:rgba(100,160,255,0.10);border:1px solid rgba(100,160,255,0.22);color:#93c5fd;border-radius:99px;padding:2px 10px;font-size:0.76rem;font-weight:700;">◆ {_label}</span>'
    else:
        _pill = ""

    _sep = '<span style="color:rgba(100,140,220,0.25);margin:0 6px;">│</span>'
    _page_part = f'{_sep}<span style="color:#3a5570;font-size:0.80rem;">{_page}</span>' if _page else ""

    st.markdown(
        f'''<div style="
            display:flex; align-items:center; gap:10px; flex-wrap:wrap;
            background:rgba(8,18,40,0.55);
            border:1px solid rgba(59,110,248,0.13);
            border-radius:12px;
            padding:7px 14px;
            font-size:0.82rem; color:#5a7898;
            margin-bottom:0.75rem;
        ">
            <span style="font-weight:700;color:#3b6ef8;font-size:0.72rem;letter-spacing:.06em;text-transform:uppercase;">大盤</span>
            {_sep}
            <span style="color:#5a7898;">{_idx_part}</span>
            {_sep}
            <span style="color:#5a7898;">濾網</span>　{_pill}
            {_page_part}
            <span style="margin-left:auto;font-size:0.70rem;color:#2a3f58;">{APP_VERSION}</span>
        </div>''',
        unsafe_allow_html=True,
    )

with st.sidebar:
    st.markdown(f'<div class="nav-card"><div class="nav-title">台股短線系統</div><div style="font-size:1.12rem;font-weight:800;color:#c8dcf8;letter-spacing:-0.01em;">{APP_VERSION}</div><div style="font-size:0.80rem;color:#3a5570;margin-top:0.28rem;">AI 驅動當沖分析平台</div></div>', unsafe_allow_html=True)
    page_list = ["首頁", "分析中心", "市場儀表板", "快照中心", "持倉中心"]
    if st.session_state.current_page not in page_list:
        st.session_state.current_page = "分析中心"
    page_choice = st.radio("功能區", page_list, index=page_list.index(st.session_state.current_page), label_visibility="collapsed")
    if page_choice != st.session_state.current_page:
        st.session_state.current_page = page_choice
        st.rerun()

    st.toggle("手機精簡版", key="mobile_mode")

    st.markdown('<div class="nav-card"><div class="nav-title">使用者設定</div></div>', unsafe_allow_html=True)
    st.header("👤 使用者")
    user_options = st.session_state.user_list if st.session_state.user_list else DEFAULT_USERS.copy()
    current_idx = user_options.index(st.session_state.current_user) if st.session_state.current_user in user_options else 0
    chosen_user = st.selectbox("選擇使用者", user_options, index=current_idx)

    if chosen_user != st.session_state.current_user:
        st.session_state.current_user = chosen_user
        st.session_state.favorites = load_favorites()
        st.session_state.results_data = []
        st.session_state.selected_code = None
        st.session_state.analysis_mode = "idle"
        st.rerun()

    with st.expander("新增使用者", expanded=False):
        new_user = st.text_input("輸入新使用者名稱", key="new_user_name")
        if st.button("建立使用者", use_container_width=True):
            name = str(new_user).strip()
            if not name:
                st.warning("請先輸入使用者名稱。")
            elif name in user_options:
                st.warning("這個使用者已存在。")
            else:
                user_options.append(name)
                save_users(user_options)
                st.session_state.user_list = load_users()
                st.session_state.current_user = name
                st.session_state.favorites = load_favorites()
                st.session_state.results_data = []
                st.session_state.selected_code = None
                st.session_state.analysis_mode = "idle"
                st.success(f"已新增使用者：{name}")
                st.rerun()

    with st.expander("刪除使用者", expanded=False):
        deletable_users = [u for u in user_options if u != st.session_state.current_user]
        if deletable_users:
            delete_user = st.selectbox("選擇要刪除的使用者", deletable_users, index=0, key="delete_user_name")
            if st.button("刪除使用者", use_container_width=True):
                target = str(delete_user).strip()
                if target:
                    updated_users = [u for u in user_options if u != target]
                    save_users(updated_users)
                    st.session_state.user_list = load_users()

                    for prefix in ["stock_favorites", "stock_snapshots", "trades_v13"]:
                        p = user_file(prefix, target)
                        if p.exists():
                            p.unlink()

                    st.success(f"已刪除使用者：{target}")
                    st.rerun()
        else:
            st.caption("目前沒有可刪除的其他使用者。")

    st.caption(f"目前資料分流：{st.session_state.current_user}")
    with st.expander("加權指數來源／手動修正", expanded=False):
        st.number_input(
            "手動加權收盤指數（設 0＝自動）",
            min_value=0.0,
            max_value=200000.0,
            step=1.0,
            key="manual_taiex_override",
            help="自動時依「靜態收盤基準日」對齊：盤中顯示上一交易日收盤，收盤後顯示當日收盤；優先 TWSE OpenAPI，其次 Yahoo ^TWII 日線（非盤中即時）。",
        )
        st.caption("設為 **0** 即關閉手動覆寫。")
    st.markdown('<div class="nav-card"><div class="nav-title">目前頁面</div><div style="font-size:1rem;font-weight:700;color:#f8fafc;">' + st.session_state.current_page + '</div><div style="font-size:0.84rem;color:#cbd5e1;margin-top:0.3rem;">目前保留四個正式版主頁：分析中心、市場儀表板、快照中心、持倉中心。V149 直補 TPEx 法人主頁 / OpenAPI 來源，並維持盤前嚴格流動性門檻與自動挑股排序。</div></div>', unsafe_allow_html=True)
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
            st.session_state["market_info_cache"] = market_info
            tmp = [analyze_one(code, market_info["score_adj"], name_map) for code in favs]
            tmp = [x for x in tmp if x is not None]
            tmp = enrich_with_institutional(tmp)
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

def build_sector_snapshot(results: list[dict]) -> pd.DataFrame:
    if not results:
        return pd.DataFrame(columns=["族群", "平均漲跌幅", "平均量比", "代表股"])
    rows = []
    for item in results:
        stock = str(item.get("股票", ""))
        close = float(item.get("收盤", 0) or 0)
        support = float(item.get("支撐", 0) or 0)
        if support:
            pct = round((close - support) / support * 100, 2)
        else:
            pct = 0.0
        rows.append({
            "族群": item.get("族群", "未分類"),
            "平均漲跌幅": pct,
            "平均量比": float(item.get("量比5日", 0) or 0),
            "代表股": stock,
            "_rank": float(item.get("_rank", 0) or 0),
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["族群", "平均漲跌幅", "平均量比", "代表股"])
    idx = df.groupby("族群")["_rank"].idxmax()
    leaders = df.loc[idx, ["族群", "代表股"]]
    agg = df.groupby("族群", as_index=False).agg({"平均漲跌幅":"mean", "平均量比":"mean"})
    merged = agg.merge(leaders, on="族群", how="left")
    merged["平均漲跌幅"] = merged["平均漲跌幅"].round(2)
    merged["平均量比"] = merged["平均量比"].round(2)
    return merged.sort_values(["平均漲跌幅", "平均量比"], ascending=False).reset_index(drop=True)

def build_live_rankings(results: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not results:
        cols = ["股票", "族群", "漲跌幅%", "成交量"]
        empty1 = pd.DataFrame(columns=cols)
        empty2 = pd.DataFrame(columns=["股票", "交易訊號", "風報比", "量比5日"])
        return empty1, empty2
    rows = []
    for item in results:
        close = float(item.get("收盤", 0) or 0)
        support = float(item.get("支撐", 0) or 0)
        pct = round((close - support) / support * 100, 2) if support else 0.0
        rows.append({
            "股票": item.get("股票", ""),
            "族群": item.get("族群", "未分類"),
            "漲跌幅%": pct,
            "成交量": int(item.get("成交量", 0) or 0),
            "交易訊號": item.get("交易訊號", ""),
            "風報比": round(float(item.get("風報比", 0) or 0), 2),
            "量比5日": round(float(item.get("量比5日", 0) or 0), 2),
            "_rank": float(item.get("_rank", 0) or 0),
        })
    df = pd.DataFrame(rows)
    gainers = df.sort_values(["漲跌幅%", "成交量"], ascending=False).head(5)[["股票", "族群", "漲跌幅%", "成交量"]].reset_index(drop=True)
    radar = df.sort_values(["_rank", "風報比", "量比5日"], ascending=False).head(5)[["股票", "交易訊號", "風報比", "量比5日"]].reset_index(drop=True)
    return gainers, radar

def render_market_dashboard(results: list[dict]):
    st.markdown("### 市場儀表板")
    if not results:
        st.info("目前還沒有可用的分析結果，所以類股預覽、排行預覽、短線雷達會先顯示空白。你可以先回分析中心做手動搜尋或自動挑股，或先使用這一頁上方的官方市場快照。")
        return
    sector_df = build_sector_snapshot(results)
    gainers_df, radar_df = build_live_rankings(results)
    total_cnt = len(results)
    bullish_cnt = sum(1 for x in results if x.get("結論") in ["看多", "中性偏多"])
    entry_cnt = sum(1 for x in results if x.get("交易訊號") == "🔥進場")
    avg_rr = round(sum(float(x.get("風報比", 0) or 0) for x in results) / max(total_cnt, 1), 2)
    d1, d2, d3 = st.columns([1.15, 1.15, 1.4])
    with d1:
        st.markdown(f'<div class="dashboard-card"><h4>類股動能預覽</h4><div class="dashboard-sub">先用目前分析結果聚合，後續再接官方 API</div><div class="compact-note">最強族群：<b>{sector_df.iloc[0]["族群"] if not sector_df.empty else "N/A"}</b></div><div class="compact-note">平均量比最高：<b>{sector_df.sort_values("平均量比", ascending=False).iloc[0]["族群"] if not sector_df.empty else "N/A"}</b></div><div style="margin-top:0.55rem;">' + ''.join([f'<span class="rank-chip">{r["族群"]} {r["平均漲跌幅"]:+.2f}%</span>' for _, r in sector_df.head(3).iterrows()]) + '</div></div>', unsafe_allow_html=True)
    with d2:
        st.markdown(f'<div class="dashboard-card"><h4>短線雷達預覽</h4><div class="dashboard-sub">先做你圖中的飆股搜尋雛形</div><div class="compact-note">分析檔數：<b>{total_cnt}</b></div><div class="compact-note">偏多檔數：<b>{bullish_cnt}</b></div><div class="compact-note">🔥進場訊號：<b>{entry_cnt}</b></div><div class="compact-note">平均風報比：<b>{avg_rr}</b></div></div>', unsafe_allow_html=True)
    with d3:
        lead = gainers_df.iloc[0]["股票"] if not gainers_df.empty else "N/A"
        st.markdown(f'<div class="dashboard-card"><h4>排行中心預覽</h4><div class="dashboard-sub">先做首頁排行模組，後續再補成交值 / 量能 / 類股排行</div><div class="compact-note">強勢股第一名：<b>{lead}</b></div><div class="compact-note">這一版先用目前分析結果排序，不額外增加資料源風險。</div></div>', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("#### 類股漲跌預覽")
        show_df = sector_df.head(6).copy()
        if not show_df.empty:
            st.dataframe(show_df, use_container_width=True, hide_index=True)
    with c2:
        st.markdown("#### 即時排行預覽")
        if not gainers_df.empty:
            st.dataframe(gainers_df, use_container_width=True, hide_index=True)
    with st.expander("短線雷達預覽", expanded=False):
        st.dataframe(radar_df, use_container_width=True, hide_index=True)
        st.caption("這裡先做成系統內部的短線雷達雛形；之後可依你提供的版面，再擴充突破區間、均線多頭、量價齊揚等條件。")

def _intraday_long_metrics(item: dict) -> dict:
    entry = _safe_float(item.get("進場"))
    stop = _safe_float(item.get("停損"))
    target = _safe_float(item.get("中繼目標")) or _safe_float(item.get("目標")) or _safe_float(item.get("短期壓力"))
    resistance = _safe_float(item.get("短期壓力"))
    risk_pct = None
    target_pct = None
    resistance_pct = None
    if entry and stop is not None and entry > 0:
        risk_pct = (entry - stop) / entry * 100
    if entry and target is not None and entry > 0:
        target_pct = (target - entry) / entry * 100
    if entry and resistance is not None and entry > 0:
        resistance_pct = (resistance - entry) / entry * 100
    return {
        "entry": entry,
        "stop": stop,
        "target": target,
        "resistance": resistance,
        "risk_pct": risk_pct,
        "target_pct": target_pct,
        "resistance_pct": resistance_pct,
    }


def is_intraday_long_candidate(item: dict, strict: bool = False) -> bool:
    if not isinstance(item, dict):
        return False
    if item.get("排名分組") == "D_後段排除":
        return False
    if item.get("交易訊號") == "❌不進":
        return False
    if item.get("操作評級") in ["避開", "空方"]:
        return False
    if item.get("結論") not in ["看多", "中性偏多"]:
        return False
    if item.get("盤前建議") not in ["偏多（可進場）", "中性（等待確認）"]:
        return False
    if item.get("突破狀態") == "假突破風險":
        return False

    metrics = _intraday_long_metrics(item)
    risk_pct = metrics.get("risk_pct")
    target_pct = metrics.get("target_pct")
    entry = metrics.get("entry")
    stop = metrics.get("stop")
    target = metrics.get("target")

    if entry is None or stop is None or target is None:
        return False
    if risk_pct is None or target_pct is None:
        return False
    if risk_pct <= 0 or target_pct <= 0:
        return False
    if risk_pct > 5.0:
        return False
    if target_pct > 10.2:
        return False

    rr = float(item.get("風報比", 0) or 0)
    if strict:
        if rr < 1.2:
            return False
        if item.get("交易訊號") not in ["🔥進場", "👀觀察"]:
            return False
        if item.get("法人方向") in ["偏空", "小偏空"]:
            return False
        if float(item.get("量比5日", 0) or 0) < 1.0:
            return False
    else:
        if rr < 0.8:
            return False
        if item.get("法人方向") == "偏空":
            return False

    return True


def long_pick_compute(df: pd.DataFrame, item: dict) -> dict | None:
    if df is None or df.empty or len(df) < 20:
        return None
    close_s = pd.to_numeric(df["Close"], errors="coerce")
    high_s = pd.to_numeric(df["High"], errors="coerce")
    low_s = pd.to_numeric(df["Low"], errors="coerce")
    open_s = pd.to_numeric(df["Open"], errors="coerce")
    vol_s = pd.to_numeric(df["Volume"], errors="coerce").fillna(0)

    close = float(close_s.iloc[-1])
    high = float(high_s.iloc[-1])
    low = float(low_s.iloc[-1])
    open_ = float(open_s.iloc[-1])
    vol = float(vol_s.iloc[-1])

    ma5 = float(df["ma5"].iloc[-1]) if "ma5" in df.columns and pd.notna(df["ma5"].iloc[-1]) else None
    ma10 = float(df["ma10"].iloc[-1]) if "ma10" in df.columns and pd.notna(df["ma10"].iloc[-1]) else None
    ma20 = float(df["ma20"].iloc[-1]) if "ma20" in df.columns and pd.notna(df["ma20"].iloc[-1]) else None
    vol20 = float(df["vol20"].iloc[-1]) if "vol20" in df.columns and pd.notna(df["vol20"].iloc[-1]) else float(vol_s.tail(20).mean())
    vol5 = float(df["vol5"].iloc[-1]) if "vol5" in df.columns and pd.notna(df["vol5"].iloc[-1]) else vol

    if ma5 is None or ma10 is None or close <= 0:
        return None

    vol_lots_20 = vol_s.tail(20) / 1000.0
    avg_value_20 = float((close_s.tail(20) * vol_s.tail(20)).mean()) / 1e8
    avg_vol_lots_20 = float(vol_lots_20.mean())

    # ---- 1. 硬性過濾（只排除根本不能交易的）----
    if close > 1000:
        return None
    has_value = avg_value_20 >= 3.0
    has_vol = avg_vol_lots_20 >= 5000
    if not has_value and not has_vol:
        return None
    min_vol_5d = float(vol_s.tail(5).min()) / 1000.0
    if min_vol_5d <= 1000:
        return None

    close_5ago = float(close_s.iloc[-6]) if len(df) >= 6 else close
    chg_5d_pct = (close - close_5ago) / close_5ago * 100 if close_5ago > 0 else 0
    low20 = float(low_s.tail(20).min())
    upper_shadow = high - max(close, open_)
    bar_range = high - low
    vol_ratio_20 = vol / vol20 if vol20 > 0 else 1.0
    close_pos = (close - low) / bar_range if bar_range > 0 else 0.5

    # 強制排除：完全的空方結構 + 大跌
    if close < ma10 and ma5 < ma10 and chg_5d_pct < -5:
        return None

    # ---- 2. 評分（不再用必要條件擋，全靠分數篩）----
    # (1) 趨勢分 30
    score_trend = 0
    if close > ma5:
        score_trend += 8
    if close > ma10:
        score_trend += 8
    if ma5 > ma10:
        score_trend += 6
    if len(df) >= 3 and "ma10" in df.columns:
        ma10_prev = df["ma10"].iloc[-3]
        if pd.notna(ma10_prev) and ma10 >= float(ma10_prev):
            score_trend += 8

    # (2) 動能分 25
    score_momentum = 0
    if 3 <= chg_5d_pct <= 15:
        score_momentum += 15
    elif 0 <= chg_5d_pct < 3:
        score_momentum += 8
    elif chg_5d_pct > 15:
        score_momentum += 4
    if close_pos >= 0.7:
        score_momentum += 10

    # (3) 量價分 20
    score_volume = 0
    if 1.2 <= vol_ratio_20 <= 2.5:
        score_volume += 12
    elif 0.9 <= vol_ratio_20 < 1.2:
        score_volume += 6
    tail3 = df.tail(3)
    t3_close = pd.to_numeric(tail3["Close"], errors="coerce")
    t3_open = pd.to_numeric(tail3["Open"], errors="coerce")
    t3_vol = pd.to_numeric(tail3["Volume"], errors="coerce").fillna(0)
    up_vols, dn_vols = [], []
    for i in range(len(tail3)):
        c_i, o_i, v_i = float(t3_close.iloc[i]), float(t3_open.iloc[i]), float(t3_vol.iloc[i])
        if c_i >= o_i:
            up_vols.append(v_i)
        else:
            dn_vols.append(v_i)
    avg_up = sum(up_vols) / len(up_vols) if up_vols else 0
    avg_dn = sum(dn_vols) / len(dn_vols) if dn_vols else 0
    if avg_up > avg_dn and up_vols:
        score_volume += 8

    # (4) 籌碼分 15
    score_chip = 0
    has_inst = False
    foreign_val = float(item.get("外資買賣超", 0) or 0)
    trust_val = float(item.get("投信買賣超", 0) or 0)
    if foreign_val != 0 or trust_val != 0:
        has_inst = True
        if foreign_val > 0:
            score_chip += 6
        if trust_val > 0:
            score_chip += 6
        if foreign_val > 0 and trust_val > 0:
            score_chip += 3
    if not has_inst:
        avg_val_3 = float((close_s.tail(3) * vol_s.tail(3)).mean()) / 1e8
        if avg_val_3 > avg_value_20:
            score_chip += 15

    # (5) 風險扣分 -20
    score_risk = 0
    risk_parts: list[str] = []
    if bar_range > 0 and upper_shadow / bar_range > 0.45:
        score_risk -= 8
        risk_parts.append("長上影(-8)")
    is_black_today = close < open_
    if is_black_today and vol > vol20 * 1.5:
        score_risk -= 10
        risk_parts.append("爆量黑K(-10)")
    if ma5 > 0 and (close - ma5) / ma5 > 0.08:
        score_risk -= 6
        risk_parts.append("離MA5過遠(-6)")
    if close <= ma5 and close <= ma10:
        score_risk -= 6
        risk_parts.append("收盤在MA5與MA10下(-6)")
    if chg_5d_pct < -3:
        score_risk -= 4
        risk_parts.append("5日跌幅<-3%(-4)")

    if risk_parts:
        long_risk_detail = (
            "本輪已觸發：" + "、".join(risk_parts)
            + f"。風險欄位合計 {score_risk} 分（負值），已加進總分；總分最後限制在 0～100。"
        )
    else:
        long_risk_detail = (
            "本輪**未觸發**任何風險扣分條件，故風險欄為 0。"
            "（可能扣分項僅在觸發時才會出現：長上影-8、爆量黑K-10、離MA5過遠-6、"
            "收盤在雙均下-6、5日跌幅<-3%為-4。先前表內固定範例易造成誤解，已改為只顯示實際結果。）"
        )

    total = score_trend + score_momentum + score_volume + score_chip + score_risk
    total = max(0, min(100, total))

    reasons = []
    if score_trend >= 22:
        reasons.append("趨勢強")
    if chg_5d_pct >= 3:
        reasons.append(f"5日漲{chg_5d_pct:.1f}%")
    if vol_ratio_20 >= 1.2:
        reasons.append(f"量比{vol_ratio_20:.1f}倍")
    if foreign_val > 0:
        reasons.append("外資買")
    if trust_val > 0:
        reasons.append("投信買")
    if not reasons:
        reasons.append("綜合評分")

    warnings = []
    if score_risk < 0:
        if bar_range > 0 and upper_shadow / bar_range > 0.45:
            warnings.append("長上影")
        if is_black_today and vol > vol20 * 1.5:
            warnings.append("爆量黑K")
        if ma5 > 0 and (close - ma5) / ma5 > 0.08:
            warnings.append("離MA5過遠")

    return {
        "long_total": total,
        "long_trend": score_trend,
        "long_momentum": score_momentum,
        "long_volume": score_volume,
        "long_chip": score_chip,
        "long_risk": score_risk,
        "long_reason": "、".join(reasons[:4]),
        "long_warning": "、".join(warnings) if warnings else "",
        "long_chg5d": round(chg_5d_pct, 2),
        "long_vol_ratio": round(vol_ratio_20, 2),
        "long_close_pos": round(close_pos, 2),
        "long_risk_detail": long_risk_detail,
    }


def long_pick_strict_ok(metrics: dict) -> bool:
    if metrics is None:
        return False
    if metrics["long_total"] < 75:
        return False
    if metrics.get("long_chg5d", 0) <= 0:
        return False
    if metrics.get("long_vol_ratio", 0) < 1.0:
        return False
    if "爆量黑K" in metrics.get("long_warning", ""):
        return False
    return True


def short_pick_compute(df: pd.DataFrame, item: dict) -> dict | None:
    if df is None or df.empty or len(df) < 20:
        return None
    close_s = pd.to_numeric(df["Close"], errors="coerce")
    high_s = pd.to_numeric(df["High"], errors="coerce")
    low_s = pd.to_numeric(df["Low"], errors="coerce")
    open_s = pd.to_numeric(df["Open"], errors="coerce")
    vol_s = pd.to_numeric(df["Volume"], errors="coerce").fillna(0)

    close = float(close_s.iloc[-1])
    high = float(high_s.iloc[-1])
    low = float(low_s.iloc[-1])
    open_ = float(open_s.iloc[-1])
    vol = float(vol_s.iloc[-1])

    ma5 = float(df["ma5"].iloc[-1]) if "ma5" in df.columns and pd.notna(df["ma5"].iloc[-1]) else None
    ma10 = float(df["ma10"].iloc[-1]) if "ma10" in df.columns and pd.notna(df["ma10"].iloc[-1]) else None
    ma20 = float(df["ma20"].iloc[-1]) if "ma20" in df.columns and pd.notna(df["ma20"].iloc[-1]) else None
    vol20 = float(df["vol20"].iloc[-1]) if "vol20" in df.columns and pd.notna(df["vol20"].iloc[-1]) else float(vol_s.tail(20).mean())

    if ma5 is None or ma10 is None or close <= 0:
        return None

    vol_lots_20 = vol_s.tail(20) / 1000.0
    avg_value_20 = float((close_s.tail(20) * vol_s.tail(20)).mean()) / 1e8
    avg_vol_lots_20 = float(vol_lots_20.mean())

    # ---- 硬性過濾（同做多）----
    if close > 1000:
        return None
    has_value = avg_value_20 >= 3.0
    has_vol = avg_vol_lots_20 >= 5000
    if not has_value and not has_vol:
        return None
    min_vol_5d = float(vol_s.tail(5).min()) / 1000.0
    if min_vol_5d <= 1000:
        return None

    # ---- 做空必要條件 ----
    if close >= ma5 and close >= ma10 and ma5 >= ma10:
        return None
    high20 = float(high_s.tail(20).max())
    if high20 > 0 and close > high20 * 0.97:
        return None

    close_5ago = float(close_s.iloc[-6]) if len(df) >= 6 else close
    chg_5d_pct = (close - close_5ago) / close_5ago * 100 if close_5ago > 0 else 0

    # ---- 強制排除 ----
    if close > ma10 and ma5 > ma10:
        return None
    if chg_5d_pct > 5:
        return None

    pv_label = str(item.get("價量結論", "") or "")
    weak_bear_shrink = pv_label == "價跌量縮"
    strong_bear_pv = pv_label in ("價跌量增", "開高走低爆量", "跳空下跌")

    # ---- 評分（總分 100）----
    # (1) 趨勢分 30：空方趨勢
    score_trend = 0
    if close < ma5:
        score_trend += 8
    if close < ma10:
        score_trend += 8
    if ma5 < ma10:
        score_trend += 6
    if len(df) >= 3 and "ma10" in df.columns:
        ma10_prev = df["ma10"].iloc[-3]
        if pd.notna(ma10_prev) and ma10 <= float(ma10_prev):
            score_trend += 8

    # (2) 動能分 25：空方動能
    score_momentum = 0
    if -15 <= chg_5d_pct <= -3:
        score_momentum += 15
    elif -3 < chg_5d_pct <= 0:
        score_momentum += 8
    elif chg_5d_pct < -15:
        score_momentum += 4
    bar_range = high - low
    close_pos = (close - low) / bar_range if bar_range > 0 else 0.5
    if close_pos <= 0.3:
        score_momentum += 10

    # (3) 量價分 20：價跌量增類為空方當沖主訊號；價跌量縮不當強空加分
    score_volume = 0
    vol_ratio_20 = vol / vol20 if vol20 > 0 else 1.0
    is_black = close < open_
    if weak_bear_shrink:
        score_volume -= 6
    else:
        if is_black and 1.2 <= vol_ratio_20 <= 3.0:
            score_volume += 14 if strong_bear_pv else 12
        elif is_black and 0.9 <= vol_ratio_20 < 1.2:
            score_volume += 4 if strong_bear_pv else 6
    tail3 = df.tail(3)
    t3_close = pd.to_numeric(tail3["Close"], errors="coerce")
    t3_open = pd.to_numeric(tail3["Open"], errors="coerce")
    t3_vol = pd.to_numeric(tail3["Volume"], errors="coerce").fillna(0)
    dn_vols, up_vols = [], []
    for i in range(len(tail3)):
        c_i, o_i, v_i = float(t3_close.iloc[i]), float(t3_open.iloc[i]), float(t3_vol.iloc[i])
        if c_i < o_i:
            dn_vols.append(v_i)
        else:
            up_vols.append(v_i)
    avg_dn = sum(dn_vols) / len(dn_vols) if dn_vols else 0
    avg_up = sum(up_vols) / len(up_vols) if up_vols else 0
    if weak_bear_shrink:
        if avg_dn > avg_up and dn_vols:
            score_volume += 1
    elif avg_dn > avg_up and dn_vols:
        score_volume += 8

    # (4) 籌碼分 15
    score_chip = 0
    has_inst = False
    foreign_val = float(item.get("外資買賣超", 0) or 0)
    trust_val = float(item.get("投信買賣超", 0) or 0)
    if foreign_val != 0 or trust_val != 0:
        has_inst = True
        if foreign_val < 0:
            score_chip += 6
        if trust_val < 0:
            score_chip += 6
        if foreign_val < 0 and trust_val < 0:
            score_chip += 3
    if not has_inst:
        avg_val_3 = float((close_s.tail(3) * vol_s.tail(3)).mean()) / 1e8
        if avg_val_3 < avg_value_20:
            score_chip += 15

    # (5) 風險扣分 -20
    score_risk = 0
    risk_parts_s: list[str] = []
    lower_shadow = min(close, open_) - low
    if bar_range > 0 and lower_shadow / bar_range > 0.45:
        score_risk -= 8
        risk_parts_s.append("長下影(-8)")
    is_green = close > open_
    if is_green and vol > vol20 * 1.5:
        score_risk -= 10
        risk_parts_s.append("爆量紅K(-10)")
    if ma5 > 0 and (ma5 - close) / ma5 > 0.08:
        score_risk -= 6
        risk_parts_s.append("離MA5過遠(-6)")

    if risk_parts_s:
        short_risk_detail = (
            "本輪已觸發：" + "、".join(risk_parts_s)
            + f"。風險欄位合計 {score_risk} 分（負值），已加進總分；總分最後限制在 0～100。"
        )
    else:
        short_risk_detail = (
            "本輪**未觸發**任何風險扣分條件，故風險欄為 0。"
            "（做空側可能扣分：長下影-8、爆量紅K-10、離MA5過遠-6，僅在觸發時列入。）"
        )

    total = score_trend + score_momentum + score_volume + score_chip + score_risk
    total = max(0, min(100, total))

    reasons = []
    if weak_bear_shrink:
        reasons.append("偏空但動能不足（價跌量縮）")
    if score_trend >= 22:
        reasons.append("空方趨勢強")
    if chg_5d_pct <= -3:
        reasons.append(f"5日跌{chg_5d_pct:.1f}%")
    if strong_bear_pv and pv_label and not weak_bear_shrink:
        reasons.append(pv_label)
    elif is_black and vol_ratio_20 >= 1.2 and not weak_bear_shrink:
        reasons.append(f"黑K量比{vol_ratio_20:.1f}倍")
    if foreign_val < 0:
        reasons.append("外資賣")
    if trust_val < 0:
        reasons.append("投信賣")
    if not reasons:
        reasons.append("綜合評分")

    warnings = []
    if weak_bear_shrink:
        warnings.append("價跌量縮：跌勢可能趨緩，不宜作空方當沖主依據")
    if score_risk < 0:
        if bar_range > 0 and lower_shadow / bar_range > 0.45:
            warnings.append("長下影")
        if is_green and vol > vol20 * 1.5:
            warnings.append("爆量紅K")
        if ma5 > 0 and (ma5 - close) / ma5 > 0.08:
            warnings.append("離MA5過遠")

    return {
        "short_total": total,
        "short_trend": score_trend,
        "short_momentum": score_momentum,
        "short_volume": score_volume,
        "short_chip": score_chip,
        "short_risk": score_risk,
        "short_reason": "、".join(reasons[:4]),
        "short_warning": "、".join(warnings) if warnings else "",
        "short_chg5d": round(chg_5d_pct, 2),
        "short_vol_ratio": round(vol_ratio_20, 2),
        "short_close_pos": round(close_pos, 2),
        "short_risk_detail": short_risk_detail,
    }


def auto_pick_general_score(item: dict) -> float:
    score = 0.0
    inst_score = float(item.get("法人分數", 0) or 0)
    liq_score = liquidity_auto_bonus(item, "general")
    kd_k = float(item.get("KD_K", 50) or 50)
    bias5 = float(item.get("乖離率5日", 0) or 0)

    score += float(item.get("_rank", 0)) * 0.55
    score += min(float(item.get("量比5日", 0) or 0), 5) * 18
    score += min(max(float(item.get("量能變化%", 0) or 0), -50), 200) * 0.18
    score += inst_score * 1.2
    score += liq_score

    if 45 <= kd_k <= 80:
        score += 12
    elif kd_k < 45:
        score += 4
    else:
        score -= min(kd_k - 80, 20) * 0.8

    if 0 <= bias5 <= 6:
        score += 8
    elif -3 <= bias5 < 0:
        score += 3
    elif bias5 > 6:
        score -= min(bias5 - 6, 10) * 1.2
    else:
        score -= min(abs(bias5), 10) * 1.3

    if item.get("交易訊號") == "🔥進場":
        score += 18
    elif item.get("交易訊號") == "👀觀察":
        score += 10
    elif item.get("交易訊號") == "❌不進":
        score -= 10

    if item.get("結論") == "看多":
        score += 12
    elif item.get("結論") == "中性偏多":
        score += 8
    elif item.get("結論") in ["看空", "中性偏空"]:
        score -= 12

    if item.get("量能變化") == "量增":
        score += 10
    elif item.get("量能變化") == "量縮":
        score -= 4

    if item.get("法人方向") == "偏多":
        score += 8
    elif item.get("法人方向") == "小偏多":
        score += 4
    elif item.get("法人方向") == "偏空":
        score -= 8
    elif item.get("法人方向") == "小偏空":
        score -= 4

    if item.get("法人共振") == "偏多共振":
        score += 6
    elif item.get("法人共振") == "偏空共振":
        score -= 6
    return round(score, 2)

def auto_pick_strict_score(item: dict) -> float:
    score = 0.0
    inst_score = float(item.get("法人分數", 0) or 0)
    liq_score = liquidity_auto_bonus(item, "strict")
    bias5 = float(item.get("乖離率5日", 0) or 0)

    score += float(item.get("_rank", 0)) * 0.45
    score += min(float(item.get("風報比", 0) or 0), 4) * 28
    score += min(float(item.get("量比5日", 0) or 0), 3) * 8
    score += max(0, 80 - abs(float(item.get("KD_K", 50) or 50) - 50)) * 0.18
    score += inst_score * 1.35
    score += liq_score

    if -2 <= bias5 <= 5:
        score += 6
    else:
        score -= min(abs(bias5 - 1.5), 15) * 0.9

    if item.get("交易訊號") == "🔥進場":
        score += 16
    elif item.get("交易訊號") == "👀觀察":
        score += 8
    elif item.get("交易訊號") == "❌不進":
        score -= 10

    if item.get("結論") == "看多":
        score += 16
    elif item.get("結論") == "中性偏多":
        score += 10
    elif item.get("結論") in ["看空", "中性偏空"]:
        score -= 14

    if item.get("趨勢燈號") == "綠燈":
        score += 10
    elif item.get("趨勢燈號") == "紅燈":
        score -= 8

    if item.get("進場燈號") == "綠燈":
        score += 10
    elif item.get("進場燈號") == "紅燈":
        score -= 8

    if item.get("量能變化") == "量增":
        score += 4

    if item.get("法人方向") == "偏多":
        score += 10
    elif item.get("法人方向") == "小偏多":
        score += 5
    elif item.get("法人方向") == "偏空":
        score -= 10
    elif item.get("法人方向") == "小偏空":
        score -= 5

    if item.get("法人共振") == "偏多共振":
        score += 10
    elif item.get("法人共振") == "偏空共振":
        score -= 10
    return round(score, 2)


def calc_short_rr(item: dict) -> float:
    entry = _safe_float(item.get("空方建議進場"))
    stop = _safe_float(item.get("空方停損"))
    target1 = _safe_float(item.get("空方中繼目標"))
    target2 = _safe_float(item.get("空方跌破目標"))
    target = None
    if entry is not None and target2 is not None and target2 < entry:
        target = target2
    elif entry is not None and target1 is not None and target1 < entry:
        target = target1
    elif target2 is not None:
        target = target2
    elif target1 is not None:
        target = target1
    if entry is None or stop is None or target is None:
        return 0.0
    risk = max(stop - entry, 0.01)
    reward = max(entry - target, 0)
    return round(reward / risk, 2)


def bearish_signal_hits(item: dict) -> int:
    hits = 0
    conclusion = str(item.get("結論", ""))
    signal = str(item.get("交易訊號", ""))
    pv = str(item.get("價量結論", ""))
    kd_k = _safe_float(item.get("KD_K"))
    kd_d = _safe_float(item.get("KD_D"))
    dif = _safe_float(item.get("MACD_DIF"))
    dea = _safe_float(item.get("MACD_DEA"))
    close = _safe_float(item.get("收盤"))
    support = _safe_float(item.get("支撐"))

    if conclusion in ["看空", "中性偏空"]:
        hits += 1
    if signal in ["❌不進", "⏳等待"]:
        hits += 1
    if pv in ["價跌量增", "開高走低爆量", "跳空下跌"]:
        hits += 1
    if kd_k is not None and kd_d is not None and kd_k < kd_d:
        hits += 1
    if dif is not None and dea is not None and dif < dea:
        hits += 1
    if close is not None and support is not None and close < support:
        hits += 1
    return hits


def auto_pick_short_score(item: dict) -> float:
    score = 0.0
    conclusion = str(item.get("結論", ""))
    signal = str(item.get("交易訊號", ""))
    pv = str(item.get("價量結論", ""))
    kd_k = _safe_float(item.get("KD_K")) or 50.0
    kd_d = _safe_float(item.get("KD_D")) or 50.0
    dif = _safe_float(item.get("MACD_DIF"))
    dea = _safe_float(item.get("MACD_DEA"))
    vol_ratio = float(item.get("量比5日", 0) or 0)
    vol_chg = float(item.get("量能變化%", 0) or 0)
    close = _safe_float(item.get("收盤"))
    support = _safe_float(item.get("支撐"))
    bias5 = float(item.get("乖離率5日", 0) or 0)
    short_rr = calc_short_rr(item)
    inst_score = float(item.get("法人分數", 0) or 0)
    liq_score = liquidity_auto_bonus(item, "short")
    inst_direction = str(item.get("法人方向", "") or "")

    score += min(short_rr, 6) * 24
    score += min(vol_ratio, 5) * 10
    score += min(max(vol_chg, -100), 200) * 0.10
    score += liq_score

    if kd_k < kd_d:
        score += 12
    else:
        score -= 6

    if dif is not None and dea is not None:
        if dif < dea:
            score += 10
        else:
            score -= 4

    if pv in ["價跌量增", "開高走低爆量", "跳空下跌"]:
        score += 18
    elif pv == "價跌量縮":
        score -= 8
    elif pv in ["價漲量增", "跳空上漲爆量"]:
        score -= 16

    if bias5 <= -1:
        score += min(abs(bias5), 10) * 1.2
    elif bias5 >= 4:
        score -= min(bias5, 12) * 1.1

    if conclusion == "看空":
        score += 18
    elif conclusion == "中性偏空":
        score += 10
    elif conclusion == "中性":
        score += 4
    else:
        score -= 12

    if signal == "❌不進":
        score += 12
    elif signal == "⏳等待":
        score += 8
    elif signal == "👀觀察":
        score += 2
    else:
        score -= 10

    if item.get("量能變化") == "量增":
        score += 6

    if close is not None and support is not None and close < support:
        score += 14

    score += bearish_signal_hits(item) * 6
    score -= inst_score * 1.1
    if inst_direction == "偏空":
        score += 10
    elif inst_direction == "小偏空":
        score += 5
    elif inst_direction == "偏多":
        score -= 10
    elif inst_direction == "小偏多":
        score -= 5
    if item.get("法人共振") == "偏空共振":
        score += 8
    elif item.get("法人共振") == "偏多共振":
        score -= 8
    return round(score, 2)


def build_short_auto_reason(item: dict) -> str:
    reasons = []
    pv = str(item.get("價量結論", ""))
    conclusion = str(item.get("結論", ""))
    signal = str(item.get("交易訊號", ""))
    short_rr = calc_short_rr(item)

    if pv in ["價跌量增", "開高走低爆量", "跳空下跌"]:
        reasons.append(pv)
    elif pv == "價跌量縮":
        reasons.append("價跌量縮（偏空、動能不足，觀察為主）")
    if conclusion in ["看空", "中性偏空", "中性"]:
        reasons.append(conclusion)
    if signal in ["❌不進", "⏳等待"]:
        reasons.append(f"訊號{signal}")
    if item.get("量能變化") == "量增":
        reasons.append("量增")
    if item.get("法人方向") in ["偏空", "小偏空"]:
        reasons.append(f"法人{item.get('法人方向')}")
    elif item.get("法人方向") in ["偏多", "小偏多"]:
        reasons.append(f"法人{item.get('法人方向')}")
    if item.get("法人共振") in ["偏空共振", "偏多共振"]:
        reasons.append(str(item.get("法人共振")))
    if short_rr >= 1.0:
        reasons.append(f"空方風報比{short_rr}")
    if not reasons:
        reasons.append("弱勢結構觀察")
    return "做空模式：" + "、".join(reasons[:4])


def make_snapshot_batch_id(snapshot_time: str, mode: str = "", count: int = 0) -> str:
    return f"{snapshot_time}|||{mode}|||{count}"


def detect_strategy_mode(row: dict) -> str:
    """根據快照/交易/分析資料推斷這筆資料屬於多方或空方。"""
    if row is None:
        return "多方"
    if isinstance(row, pd.Series):
        data = row.to_dict()
    elif isinstance(row, dict):
        data = row
    else:
        try:
            data = dict(row)
        except Exception:
            return "多方"

    direct_keys = ["快照邏輯", "策略模式", "方向", "模式"]
    for key in direct_keys:
        val = str(data.get(key, "") or "").strip()
        if not val:
            continue
        if "空" in val:
            return "空方"
        if any(x in val for x in ["多", "long", "LONG"]):
            return "多方"

    for key in ["結論", "盤前建議", "計畫結論", "盤前快照結論", "交易訊號"]:
        val = str(data.get(key, "") or "").strip()
        if not val:
            continue
        if any(x in val for x in ["看空", "偏空", "空方"]):
            return "空方"
        if any(x in val for x in ["看多", "偏多", "多方"]):
            return "多方"

    for key in ["空方建議進場", "空方停損", "空方短期壓力", "空方短期支撐", "空方中繼目標", "空方跌破目標"]:
        val = data.get(key, "")
        if str(val).strip() not in ["", "None", "nan", "NaN"]:
            return "空方"

    return "多方"


def materialize_snapshot_row(row: dict, forced_mode: str | None = None):
    """
    將快照/分析結果統一實體化成可直接顯示與比對的欄位。
    若指定 forced_mode 或偵測為空方，則把進場/停損/支撐/壓力/目標/風報比
    轉成空方欄位，避免快照中心多空欄位混用。
    """
    if row is None:
        return {}
    if isinstance(row, pd.Series):
        rec = row.to_dict()
    elif isinstance(row, dict):
        rec = dict(row)
    else:
        try:
            rec = dict(row)
        except Exception:
            return {}

    mode_text = str(forced_mode or '').strip()
    if mode_text:
        mode = '空方' if '空' in mode_text else '多方'
    else:
        mode = detect_strategy_mode(rec)
    rec['快照邏輯'] = mode

    if mode == '空方':
        mapping = {
            '進場': '空方建議進場',
            '停損': '空方停損',
            '支撐': '空方短期支撐',
            '短期壓力': '空方短期壓力',
            '中繼目標': '空方中繼目標',
            '突破目標': '空方跌破目標',
            '風報比': '空方風報比',
        }
        for target_key, short_key in mapping.items():
            val = rec.get(short_key, '')
            sval = str(val).strip() if val is not None else ''
            if sval not in ['', 'None', 'nan', 'NaN']:
                rec[target_key] = val
    return rec


def grouped_prefast_snapshots():
    snaps = load_snapshots()
    if not snaps:
        return []
    groups = {}
    for s in snaps:
        if s.get("類型") != "盤前":
            continue
        t = s.get("時間", "")
        if not t:
            continue
        groups.setdefault(t, []).append(s)
    result = []
    for t, rows in groups.items():
        mode = rows[0].get("快照邏輯", detect_strategy_mode(rows[0])) if rows else "多方"
        count = len(rows)
        result.append({
            "批次ID": make_snapshot_batch_id(t, mode, count),
            "時間": t,
            "檔數": count,
            "股票清單": [r.get("股票", "") for r in rows],
            "快照邏輯": mode,
            "rows": rows
        })
    result.sort(key=lambda x: x["時間"], reverse=True)
    return result


def get_prefast_group_map():
    return {g["批次ID"]: g for g in grouped_prefast_snapshots()}


def format_prefast_group_label(group: dict) -> str:
    return f"{group['時間']}｜{group['檔數']}檔"


def delete_selected_from_pre_snapshot(snapshot_time: str, selected_stocks: list[str]):
    snaps = load_snapshots()
    remain = []
    selected = set(selected_stocks)
    for s in snaps:
        if s.get("類型") == "盤前" and s.get("時間") == snapshot_time and s.get("股票") in selected:
            continue
        remain.append(s)
    save_snapshots(remain)

def get_latest_ohlc_for_compare(symbol_code: str):
    try:
        official = _official_latest_ohlc_map(symbol_code)
        if official.get("hit") and official.get("close") is not None:
            return {
                "close": official.get("close", ""),
                "high": official.get("high") if official.get("high") is not None else official.get("close", ""),
                "low": official.get("low") if official.get("low") is not None else official.get("close", ""),
                "date": official.get("date", ""),
                "source": official.get("source", "official"),
                "official_hit": True,
            }
        return {"close": "", "high": "", "low": "", "date": "", "source": "", "official_hit": False}
    except Exception:
        return {"close": "", "high": "", "low": "", "date": "", "source": "", "official_hit": False}

def get_latest_daily_bar_date(symbol_code: str):
    """
    取最新官方日資料日期；若官方不存在，再退回日線最後一筆日期。
    """
    try:
        official = _official_latest_ohlc_map(symbol_code)
        if official.get("hit") and official.get("date"):
            dt = pd.to_datetime(official.get("date"), errors="coerce")
            if pd.notna(dt):
                return dt.date()
        df = download_symbol(symbol_code)
        if df is None or df.empty:
            return None
        idx = df.index[-1]
        dt = pd.to_datetime(idx, errors="coerce")
        if pd.isna(dt):
            return None
        try:
            if getattr(dt, "tzinfo", None) is not None:
                dt = dt.tz_convert("Asia/Taipei").tz_localize(None)
        except Exception:
            pass
        return dt.date()
    except Exception:
        return None


def get_post_market_status(symbol_code: str = "2330.TW", snapshot_time_str: str = ""):
    """
    V144：
    先看日線最後日期，再處理非交易日 / 舊分時資料。
    除了 ready True/False，也把快照狀態改成可直接閱讀的分類：
    可正式對照 / 僅建立待盤後結果 / 非交易日快照 / 抓取異常。
    """
    snapshot_date = None
    snapshot_dt = None
    if snapshot_time_str:
        try:
            snapshot_dt = pd.to_datetime(snapshot_time_str, errors="coerce")
            if pd.notna(snapshot_dt):
                snapshot_date = snapshot_dt.date()
        except Exception:
            snapshot_dt = None
            snapshot_date = None

    def _build_status(stage: str, ready: bool, message: str, category: str, tone: str = "info"):
        return {
            "stage": stage,
            "ready": ready,
            "message": message,
            "category": category,
            "tone": tone,
        }

    def _confirm_post_ready(base_message: str):
        first = get_latest_ohlc_for_compare(symbol_code)
        second = get_latest_ohlc_for_compare(symbol_code)
        required = ["close", "high", "low"]
        complete = all(str(first.get(k, "")) != "" and str(second.get(k, "")) != "" for k in required)
        consistent = all(str(first.get(k, "")) == str(second.get(k, "")) for k in required)
        if complete and consistent:
            return _build_status(
                "可正式對照",
                True,
                base_message + " 已完成盤後資料二次確認，可直接產生正式盤後對照。",
                "可正式對照",
                "success",
            )
        return _build_status(
            "收盤後待更新",
            False,
            base_message + " 但盤後資料尚未通過完整/一致檢查，現在只建議先建立待盤後結果。",
            "僅建立待盤後結果",
            "info",
        )

    def _non_trading_message(base: str):
        return _build_status(
            "非交易日快照",
            False,
            base + " 這批快照建立在非交易日，不建議立即當成正式盤後結果；可先保留待驗名單，等下一交易日收盤後再重新確認。",
            "非交易日快照",
            "info",
        )

    try:
        daily_date = get_latest_daily_bar_date(symbol_code)
    except Exception:
        daily_date = None

    if snapshot_date is not None and snapshot_date.weekday() >= 5:
        base = f"依快照日期 {snapshot_date} 判定：這批建立於週末/假日。"
        if daily_date is not None:
            base += f" 目前最新日線資料日期為 {daily_date}。"
        return _non_trading_message(base)

    if snapshot_date is not None and daily_date is not None:
        if daily_date > snapshot_date:
            return _confirm_post_ready(f"依日線資料日期 {daily_date} 判定：已晚於快照日期 {snapshot_date}。")

        if daily_date == snapshot_date:
            return _confirm_post_ready(f"依日線資料日期 {daily_date} 判定：當天正式收盤資料已生成。")

        if daily_date < snapshot_date and snapshot_dt is not None:
            hhmm_snap = int(snapshot_dt.hour) * 100 + int(snapshot_dt.minute)
            if hhmm_snap < 900:
                return _build_status(
                    "交易日前盤前快照",
                    False,
                    f"依日線資料日期 {daily_date} 判定：快照時間 {snapshot_dt.strftime('%m-%d %H:%M')} 早於開盤。現在只能先建立待盤後結果，等收盤後再重新確認。",
                    "僅建立待盤後結果",
                    "info",
                )
            if hhmm_snap >= 1330:
                return _build_status(
                    "收盤後待更新",
                    False,
                    f"依日線資料日期 {daily_date} 判定：快照時間 {snapshot_dt.strftime('%m-%d %H:%M')} 已在收盤後，但當日正式盤後資料尚未生成。現在只能先建立待盤後結果。",
                    "僅建立待盤後結果",
                    "info",
                )
            return _build_status(
                "交易日盤中快照",
                False,
                f"依日線資料日期 {daily_date} 判定：快照時間 {snapshot_dt.strftime('%m-%d %H:%M')} 仍在交易時段。正式價格變化與結構變化需待盤後才能確認。",
                "僅建立待盤後結果",
                "info",
            )

    try:
        df_intra = download_intraday(symbol_code)
        if df_intra is None or df_intra.empty or "Datetime" not in df_intra.columns:
            return _build_status(
                "抓取異常",
                False,
                "抓不到可用的日線/分時資料，暫時無法判定這批快照是否可正式對照。",
                "抓取異常",
                "warning",
            )

        last_dt = pd.to_datetime(df_intra["Datetime"].iloc[-1], errors="coerce")
        if pd.isna(last_dt):
            return _build_status(
                "抓取異常",
                False,
                "分時資料最後時間無法辨識，暫時無法判定這批快照是否可正式對照。",
                "抓取異常",
                "warning",
            )

        market_clock = last_dt.strftime("%m-%d %H:%M")
        last_date = last_dt.date()
        hhmm = int(last_dt.hour) * 100 + int(last_dt.minute)

        if snapshot_date is not None and last_date < snapshot_date:
            if snapshot_date.weekday() >= 5:
                return _non_trading_message(f"依分時最後時間 {market_clock} 判定：快照日期 {snapshot_date} 為非交易日。")
            if snapshot_dt is not None:
                hhmm_snap = int(snapshot_dt.hour) * 100 + int(snapshot_dt.minute)
                if hhmm_snap < 900:
                    return _build_status(
                        "交易日前盤前快照",
                        False,
                        f"依分時最後時間 {market_clock} 判定：快照時間 {snapshot_dt.strftime('%m-%d %H:%M')} 早於開盤。現在只能先建立待盤後結果，等收盤後再重新確認。",
                        "僅建立待盤後結果",
                        "info",
                    )
                if hhmm_snap >= 1330:
                    return _build_status(
                        "收盤後待更新",
                        False,
                        f"依分時最後時間 {market_clock} 判定：分時資料仍停在上一交易日，當日正式盤後資料尚未生成。現在只能先建立待盤後結果。",
                        "僅建立待盤後結果",
                        "info",
                    )
                return _build_status(
                    "交易日盤中快照",
                    False,
                    f"依分時最後時間 {market_clock} 判定：快照建立在交易時段中，正式價格變化與結構變化需待盤後才能確認。",
                    "僅建立待盤後結果",
                    "info",
                )

        if hhmm < 1330:
            return _build_status(
                "交易日盤中快照",
                False,
                f"依分時最後時間 {market_clock} 判定：市場尚未收盤。現在只能先建立待盤後結果，正式對照需等收盤後再確認。",
                "僅建立待盤後結果",
                "info",
            )

        return _confirm_post_ready(f"依分時最後時間 {market_clock} 判定：已收盤。")
    except Exception as e:
        return _build_status(
            "抓取異常",
            False,
            f"盤後狀態判定失敗：{e}",
            "抓取異常",
            "warning",
        )

def compare_pre_snapshot_with_current(rows, market_score_adj, name_map, snapshot_time_str=""):
    compare_rows = []
    market_status = get_post_market_status(snapshot_time_str=snapshot_time_str)
    post_ready = market_status.get("ready", False)
    post_stage = market_status.get("stage", "更新中")
    for pre in rows:
        pre = materialize_snapshot_row(pre)
        snapshot_mode = detect_strategy_mode(pre)
        code = str(pre.get("股票代碼", "") or pre.get("股票", "")).split("（")[0].strip()
        if not code:
            continue

        now_item = analyze_one(code, market_score_adj, name_map)
        if now_item is not None and snapshot_mode == "空方":
            now_item = materialize_snapshot_row(dict(now_item, 策略模式="做空模式"))
        elif now_item is not None:
            now_item = materialize_snapshot_row(now_item)
        if now_item is None:
            compare_rows.append({
                "股票": pre.get("股票", code),
                "股票代碼": code,
                "盤前快照時間": pre.get("時間", ""),
                "盤前收盤": pre.get("收盤", ""),
                "盤前支撐": pre.get("支撐", ""),
                "盤前壓力": pre.get("短期壓力", ""),
                "盤前建議進場": pre.get("進場", ""),
                "盤前停損": pre.get("停損", ""),
                "盤前風報比": pre.get("風報比", ""),
                "盤前結論": pre.get("結論", ""),
                "盤前訊號": pre.get("交易訊號", ""),
                "快照邏輯": snapshot_mode,
                "盤後收盤": "",
                "盤後最高": "",
                "盤後最低": "",
                "盤後風報比": "",
                "盤後結論": "抓取失敗",
                "盤後訊號": "抓取失敗",
                "支撐驗證": "資料不足",
                "壓力驗證": "資料不足",
                "停損觸發": "資料不足",
                "是否可成交": "資料不足",
                "模擬進場價": pre.get("進場", ""),
                "模擬收盤價": "",
                "收盤模擬損益": "",
                "收盤模擬報酬率%": "",
                "收盤模擬結果": "資料不足",
                "模擬最高價": "",
                "最高模擬損益": "",
                "最高模擬報酬率%": "",
                "最高模擬結果": "資料不足",
                "價格變化": "資料不足",
                "結構變化": "資料不足",
                "價量結論": "資料不足",
                "判斷理由": "資料不足",
                "變化判斷": "資料不足"
            })
            continue

        pre_con = str(pre.get("結論", ""))
        now_con = str(now_item.get("結論", ""))
        if not post_ready:
            current_reason = []
            pv_now = now_item.get("價量結論", "")
            if pv_now:
                current_reason.append(f"價量：{pv_now}")
            current_reason.append(f"狀態分類：{market_status.get('category', post_stage)}")
            current_reason.append(market_status.get("message", "正式價格變化與結構變化需待盤後確認"))
            compare_rows.append({
                "股票": pre.get("股票", code),
                "股票代碼": code,
                "盤前快照時間": pre.get("時間", ""),
                "盤前收盤": pre.get("收盤", ""),
                "盤前支撐": pre.get("支撐", ""),
                "盤前壓力": pre.get("短期壓力", ""),
                "盤前建議進場": pre.get("進場", ""),
                "盤前停損": pre.get("停損", ""),
                "盤前風報比": pre.get("風報比", ""),
                "盤前結論": pre_con,
                "盤前訊號": pre.get("交易訊號", ""),
                "盤後收盤": "",
                "盤後最高": "",
                "盤後最低": "",
                "盤後風報比": "",
                "盤後結論": f"{market_status.get('category', post_stage)}",
                "盤後訊號": "⏳待確認",
                "支撐驗證": "更新中",
                "壓力驗證": "更新中",
                "停損觸發": "更新中",
                "是否可成交": "更新中",
                "模擬進場價": pre.get("進場", ""),
                "模擬收盤價": "",
                "收盤模擬損益": "",
                "收盤模擬報酬率%": "",
                "收盤模擬結果": "更新中",
                "模擬最高價": "",
                "最高模擬損益": "",
                "最高模擬報酬率%": "",
                "最高模擬結果": "更新中",
                "價格變化": "待盤後",
                "結構變化": "待盤後",
                "價量結論": now_item.get("價量結論", ""),
                "判斷理由": "｜".join(current_reason),
                "變化判斷": "待盤後"
            })
            continue
        pre_sig = str(pre.get("交易訊號", ""))
        now_sig = str(now_item.get("交易訊號", ""))

        pre_support = pre.get("支撐", "")
        pre_resistance = pre.get("短期壓力", "")
        pre_entry = pre.get("進場", "")
        pre_stop = pre.get("停損", "")

        ohlc_info = get_latest_ohlc_for_compare(code)
        post_close = ohlc_info.get("close", "")
        post_high = ohlc_info.get("high", "")
        post_low = ohlc_info.get("low", "")
        official_hit = bool(ohlc_info.get("official_hit", False))
        official_date = str(ohlc_info.get("date", "") or "")
        official_source = str(ohlc_info.get("source", "") or "")
        if not official_hit or post_close in ["", None]:
            compare_rows.append({
                "股票": pre.get("股票", code),
                "股票代碼": code,
                "盤前快照時間": pre.get("時間", ""),
                "快照邏輯": snapshot_mode,
                "盤前收盤": pre.get("收盤", ""),
                "盤前支撐": pre_support,
                "盤前壓力": pre_resistance,
                "盤前建議進場": pre_entry,
                "盤前停損": pre_stop,
                "盤前風報比": pre.get("風報比", ""),
                "盤前結論": pre_con,
                "盤前訊號": pre.get("交易訊號", ""),
                "盤後收盤": "",
                "盤後最高": "",
                "盤後最低": "",
                "盤後風報比": "",
                "盤後結論": "官方資料未更新",
                "盤後訊號": "⏳待確認",
                "支撐驗證": "待官方更新",
                "壓力驗證": "待官方更新",
                "停損觸發": "待官方更新",
                "是否可成交": "待官方更新",
                "模擬進場價": pre.get("進場", ""),
                "模擬收盤價": "",
                "收盤模擬損益": "",
                "收盤模擬報酬率%": "",
                "收盤模擬結果": "待官方更新",
                "模擬最高價": "",
                "最高模擬損益": "",
                "最高模擬報酬率%": "",
                "最高模擬結果": "待官方更新",
                "價格變化": "待官方更新",
                "結構變化": "待官方更新",
                "價量結論": now_item.get("價量結論", ""),
                "判斷理由": f"尚未取得今日官方盤後資料，這次不再用舊日線冒充今日收盤。來源：{official_source or 'NONE'} 日期：{official_date or '--'}",
                "變化判斷": "待官方更新"
            })
            continue

        support_check = "資料不足"
        resistance_check = "資料不足"
        stop_trigger = "資料不足"
        fillable = "資料不足"
        sim_close_pnl = ""
        sim_close_ret = ""
        sim_high_pnl = ""
        sim_high_ret = ""
        sim_close_result = "資料不足"
        sim_high_result = "資料不足"

        rr_diff = 0
        try:
            pre_rr = float(pre.get("風報比", 0))
            now_rr = float(now_item.get("風報比", 0))
            rr_diff = round(now_rr - pre_rr, 2)
        except Exception:
            pass

        try:
            pre_support_f = float(pre_support)
            post_low_f = float(post_low)
            if snapshot_mode == "空方":
                support_check = "跌破支撐" if post_low_f <= pre_support_f else "尚未跌破"
            else:
                support_check = "守住支撐" if post_low_f >= pre_support_f else "跌破支撐"
        except Exception:
            pass

        try:
            pre_resistance_f = float(pre_resistance)
            post_high_f = float(post_high)
            post_close_f = float(post_close)
            if snapshot_mode == "空方":
                if post_high_f < pre_resistance_f:
                    resistance_check = "未碰壓力"
                elif post_close_f >= pre_resistance_f:
                    resistance_check = "有效站回壓力"
                else:
                    resistance_check = "碰壓回落"
            else:
                if post_high_f < pre_resistance_f:
                    resistance_check = "未碰壓力"
                elif post_close_f >= pre_resistance_f:
                    resistance_check = "有效突破"
                else:
                    resistance_check = "遇壓回落"
        except Exception:
            pass

        try:
            pre_stop_f = float(pre_stop)
            if snapshot_mode == "空方":
                post_high_f = float(post_high)
                stop_trigger = "已觸發" if post_high_f >= pre_stop_f else "未觸發"
            else:
                post_low_f = float(post_low)
                stop_trigger = "已觸發" if post_low_f <= pre_stop_f else "未觸發"
        except Exception:
            pass

        try:
            pre_entry_f = float(pre_entry)
            sim_data = simulate_short_entry_from_intraday(code, pre_entry_f, pre_stop) if snapshot_mode == "空方" else simulate_entry_from_intraday(code, pre_entry_f, pre_stop)
            fillable = sim_data.get("是否可成交", "資料不足")
            sim_close_pnl = sim_data.get("收盤模擬損益", "")
            sim_close_ret = sim_data.get("收盤模擬報酬率%", "")
            sim_close_result = sim_data.get("收盤模擬結果", "資料不足")
            sim_high_pnl = sim_data.get("最高模擬損益", "")
            sim_high_ret = sim_data.get("最高模擬報酬率%", "")
            sim_high_result = sim_data.get("最高模擬結果", "資料不足")
            # V62 修正：
            # 真實盤後資料（post_close / post_high / post_low）不可被模擬結果覆蓋或清空。
            # 模擬結果只寫入模擬欄位，盤後欄位永遠保留真實盤後資料。
            if fillable == "可成交":
                stop_trigger = sim_data.get("停損觸發", stop_trigger)
            elif fillable == "未成交":
                pass
                stop_trigger = "--"
        except Exception:
            pass

        stronger = 0
        if pre_con != now_con:
            if snapshot_mode == "空方":
                stronger += 1 if now_con in ["看空", "中性偏空"] and pre_con not in ["看空", "中性偏空"] else 0
                stronger -= 1 if pre_con in ["看空", "中性偏空"] and now_con not in ["看空", "中性偏空"] else 0
            else:
                stronger += 1 if now_con in ["看多", "中性偏多"] and pre_con not in ["看多", "中性偏多"] else 0
                stronger -= 1 if pre_con in ["看多", "中性偏多"] and now_con not in ["看多", "中性偏多"] else 0
        if pre_sig != now_sig:
            sig_rank = {"🔥進場": 4, "👀觀察": 3, "⏳等待": 2, "❌不進": 0}
            stronger += 1 if sig_rank.get(now_sig, 1) > sig_rank.get(pre_sig, 1) else 0
            stronger -= 1 if sig_rank.get(now_sig, 1) < sig_rank.get(pre_sig, 1) else 0
        if rr_diff > 0.15:
            stronger += 1
        elif rr_diff < -0.15:
            stronger -= 1
        if snapshot_mode == "空方":
            if resistance_check == "碰壓回落":
                stronger += 1
            elif resistance_check == "有效站回壓力":
                stronger -= 1
            if support_check == "跌破支撐":
                stronger += 1
            if stop_trigger == "已觸發":
                stronger -= 1
        else:
            if resistance_check == "有效突破":
                stronger += 1
            elif resistance_check == "遇壓回落":
                stronger -= 1
            if support_check == "跌破支撐":
                stronger -= 1
            if stop_trigger == "已觸發":
                stronger -= 1

        if stronger >= 1:
            judge = "變強"
        elif stronger <= -1:
            judge = "變弱"
        else:
            judge = "持平"
        price_change = compare_price_change(pre.get("收盤", ""), post_close)
        compare_reason = []
        pv_now = now_item.get("價量結論", "")
        if pv_now:
            compare_reason.append(f"價量：{pv_now}")
        if snapshot_mode == "空方":
            if resistance_check == "碰壓回落":
                compare_reason.append("反彈碰壓後回落，空方節奏延續，加分")
            elif resistance_check == "有效站回壓力":
                compare_reason.append("價格站回空方壓力，空方結構受損，扣分")
            if support_check == "跌破支撐":
                compare_reason.append("價格跌破空方支撐，下跌段延續，加分")
            if stop_trigger == "已觸發":
                compare_reason.append("空方停損觸發，扣分")
        else:
            if resistance_check == "有效突破":
                compare_reason.append("壓力有效突破，加分")
            elif resistance_check == "遇壓回落":
                compare_reason.append("量能放大但收在壓力下，扣分")
            if support_check == "跌破支撐":
                compare_reason.append("收盤/低點跌破支撐，扣分")
            if stop_trigger == "已觸發":
                compare_reason.append("停損觸發，扣分")
        if rr_diff > 0.15:
            compare_reason.append("盤後風報比優於盤前，加分")
        elif rr_diff < -0.15:
            compare_reason.append("盤後風報比低於盤前，扣分")
        if pre_sig != now_sig:
            compare_reason.append(f"交易訊號由 {pre_sig} 變為 {now_sig}")
        if not compare_reason:
            compare_reason.append("整體結構變化不大，暫列持平")

        compare_rows.append({
            "股票": pre.get("股票", code),
            "股票代碼": code,
            "盤前快照時間": pre.get("時間", ""),
            "快照邏輯": snapshot_mode,
            "盤前收盤": pre.get("收盤", ""),
            "盤前支撐": pre_support,
            "盤前壓力": pre_resistance,
            "盤前建議進場": pre_entry,
            "盤前停損": pre_stop,
            "盤前風報比": pre.get("風報比", ""),
            "盤前結論": pre_con,
            "盤前訊號": pre_sig,
            "盤後收盤": post_close,
            "盤後最高": post_high,
            "盤後最低": post_low,
            "盤後資料日期": official_date,
            "盤後資料來源": official_source,
            "盤後風報比": now_item.get("風報比", ""),
            "盤後結論": now_con,
            "盤後訊號": now_sig,
            "支撐驗證": support_check,
            "壓力驗證": resistance_check,
            "停損觸發": stop_trigger,
            "是否可成交": fillable,
            "模擬進場價": pre_entry,
            "模擬收盤價": post_close,
            "收盤模擬損益": sim_close_pnl,
            "收盤模擬報酬率%": sim_close_ret,
            "收盤模擬結果": sim_close_result,
            "模擬最高價": post_high,
            "最高模擬損益": sim_high_pnl,
            "最高模擬報酬率%": sim_high_ret,
            "最高模擬結果": sim_high_result,
            "價格變化": price_change,
            "結構變化": judge,
            "變化判斷": judge,
            "價量結論": now_item.get("價量結論", ""),
            "判斷理由": "｜".join(compare_reason)
        })
    return pd.DataFrame(compare_rows)


def _is_missing(v):
    try:
        if v is None:
            return True
        s = str(v).strip()
        return s == "" or s.lower() in {"nan", "none", "null", "--"}
    except Exception:
        return True


def fmt_price(v):
    try:
        if _is_missing(v):
            return ""
        return f"{float(v):.2f}"
    except Exception:
        return "" if _is_missing(v) else str(v)


def fmt_pct(v):
    try:
        if _is_missing(v):
            return ""
        return f"{float(v):.2f}%"
    except Exception:
        return "" if _is_missing(v) else str(v)


def fmt_text(v):
    if _is_missing(v):
        return ""
    return str(v)


def fmt_lots(v):
    try:
        if _is_missing(v):
            return "--"
        lots = float(v) / 1000.0
        if abs(lots - round(lots)) < 1e-9:
            return f"{int(round(lots)):,} 張"
        return f"{lots:,.1f} 張"
    except Exception:
        return "--"


def fmt_trade_value_yi(v):
    try:
        if _is_missing(v):
            return "--"
        yi = float(v) / 100000000.0
        return f"{yi:,.2f} 億元"
    except Exception:
        return "--"


def fmt_transactions(v):
    try:
        if _is_missing(v):
            return "--"
        return f"{float(v):,.0f} 筆"
    except Exception:
        return "--"


def fmt_price_with_unit(v):
    base = fmt_price(v)
    return f"{base} 元" if base else "--"


def _safe_float(v):
    try:
        if _is_missing(v):
            return None
        val = float(v)
        if pd.isna(val):
            return None
        return val
    except Exception:
        return None


def _safe_int(v):
    f = _safe_float(v)
    if f is None:
        return None
    try:
        return int(round(f))
    except Exception:
        return None


def fmt_int(v):
    i = _safe_int(v)
    return f"{i:,}" if i is not None else "--"


def _pick_live_price(row: dict):
    return _safe_float(row.get("TWSE收盤")) or _safe_float(row.get("收盤")) or _safe_float(row.get("盤後收盤"))


def _position_symbol(row: dict) -> str:
    return str(row.get("_code") or row.get("股票代碼") or "").strip().upper()


@st.cache_data(ttl=1200, show_spinner=False)
def fetch_position_delayed_quote(symbol: str):
    """持倉中心專用：用 20 分鐘快取抓盤中延遲參考價，穩定優先。"""
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return {"price": None, "high": None, "low": None, "volume": None, "time": "", "source": "無代碼"}

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        "interval": "1m",
        "range": "1d",
        "includePrePost": "false",
        "events": "div,splits",
    }
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
    }

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        if resp.ok:
            data = resp.json()
            result = ((data or {}).get("chart") or {}).get("result") or []
            if result:
                r0 = result[0] or {}
                meta = r0.get("meta") or {}
                timestamps = r0.get("timestamp") or []
                indicators = ((r0.get("indicators") or {}).get("quote") or [{}])[0] or {}
                closes = indicators.get("close") or []
                highs = indicators.get("high") or []
                lows = indicators.get("low") or []
                volumes = indicators.get("volume") or []

                last_price = None
                for v in reversed(closes):
                    p = _safe_float(v)
                    if p is not None:
                        last_price = p
                        break
                if last_price is None:
                    last_price = _safe_float(meta.get("regularMarketPrice")) or _safe_float(meta.get("previousClose"))

                day_high = max([_safe_float(v) for v in highs if _safe_float(v) is not None], default=_safe_float(meta.get("regularMarketDayHigh")))
                day_low = min([_safe_float(v) for v in lows if _safe_float(v) is not None], default=_safe_float(meta.get("regularMarketDayLow")))

                last_volume = None
                for v in reversed(volumes):
                    vv = _safe_float(v)
                    if vv is not None:
                        last_volume = vv
                        break
                if last_volume is None:
                    last_volume = _safe_float(meta.get("regularMarketVolume"))

                quote_time = ""
                if timestamps:
                    try:
                        quote_time = datetime.fromtimestamp(int(timestamps[-1])).strftime("%Y-%m-%d %H:%M")
                    except Exception:
                        quote_time = ""
                if not quote_time and meta.get("regularMarketTime"):
                    try:
                        quote_time = datetime.fromtimestamp(int(meta.get("regularMarketTime"))).strftime("%Y-%m-%d %H:%M")
                    except Exception:
                        quote_time = ""

                return {
                    "price": round(last_price, 2) if last_price is not None else None,
                    "high": round(day_high, 2) if day_high is not None else None,
                    "low": round(day_low, 2) if day_low is not None else None,
                    "volume": int(last_volume) if last_volume is not None else None,
                    "time": quote_time,
                    "source": "盤中延遲參考價（20分鐘快取）",
                }
    except Exception:
        pass

    return {
        "price": None,
        "high": None,
        "low": None,
        "volume": None,
        "time": "",
        "source": "盤中延遲參考價抓取失敗",
    }


def resolve_position_reference_quote(row: dict):
    symbol = _position_symbol(row)
    quote = fetch_position_delayed_quote(symbol)
    price = _safe_float(quote.get("price"))
    if price is not None:
        return quote
    return {
        "price": _pick_live_price(row),
        "high": _safe_float(row.get("TWSE最高")) or _safe_float(row.get("高")) or _safe_float(row.get("收盤")),
        "low": _safe_float(row.get("TWSE最低")) or _safe_float(row.get("低")) or _safe_float(row.get("收盤")),
        "volume": _safe_float(row.get("TWSE成交量")) or _safe_float(row.get("成交量")),
        "time": fmt_text(row.get("TWSE日期", "")) if str(row.get("TWSE日期", "")).strip() else "",
        "source": "回退到官方日資料/盤前資料",
    }


def is_taiwan_regular_session(now: datetime | None = None) -> bool:
    """台股一般交易時段 09:00–13:30（Asia/Taipei），週一至週五。"""
    try:
        tz = ZoneInfo("Asia/Taipei")
        t = now.astimezone(tz) if now else datetime.now(tz)
    except Exception:
        t = datetime.now()
    if t.weekday() >= 5:
        return False
    mins = t.hour * 60 + t.minute
    return (9 * 60) <= mins <= (13 * 60 + 30)


def resolve_position_judgment_mode(quote: dict) -> tuple[str, str]:
    """(模式標籤, 簡短說明) — 僅供持倉中心顯示。"""
    in_sess = is_taiwan_regular_session()
    src = str((quote or {}).get("source", "") or "")
    price_ok = _safe_float((quote or {}).get("price")) is not None
    failed = "失敗" in src

    if in_sess and price_ok and not failed:
        return "盤中", "主判讀依盤中價、當日高低與波動區間推算支撐／壓力／停損／目標；盤前欄位請見下方折疊。"
    if in_sess and failed:
        return "回退", "交易時段內但盤中行情未取得，位階改以官方日資料／盤前分析為主。"
    if in_sess and not price_ok:
        return "回退", "交易時段內但無有效盤中價，位階改以官方日資料／盤前分析為主。"
    return "回退", "非交易時段或僅有日線參考，位階以盤前分析／日資料為主。"


def compute_intraday_position_levels(entry: float, side: str, quote: dict) -> dict | None:
    """依盤中最新價與當日高低區間推算當沖用支撐／壓力／停損／目標（持倉中心專用）。"""
    hi = _safe_float((quote or {}).get("high"))
    lo = _safe_float((quote or {}).get("low"))
    last = _safe_float((quote or {}).get("price"))
    try:
        entry = float(entry)
    except (TypeError, ValueError):
        return None
    if entry <= 0 or hi is None or lo is None or last is None:
        return None
    if hi < lo:
        hi, lo = lo, hi
    rng = hi - lo
    if rng <= 0:
        rng = max(entry * 0.008, 0.08)

    tick = max(entry * 0.0015, 0.05)

    if side == "做空":
        resistance = round(hi, 2)
        support = round(lo, 2)
        stop = entry + max(rng * 0.38, entry * 0.01, 0.1)
        stop = min(stop, resistance + max(rng * 0.2, tick * 2))
        target1 = entry - max(rng * 0.42, entry * 0.008, tick)
        target2 = max(lo - tick, entry - max(rng * 0.72, entry * 0.018, tick * 2))
        if target2 >= target1:
            target2 = round(target1 - max(tick, entry * 0.004), 2)
    else:
        resistance = round(hi, 2)
        support = round(lo, 2)
        stop = entry - max(rng * 0.38, entry * 0.01, 0.1)
        stop = max(stop, support - max(rng * 0.15, tick * 2))
        target1 = entry + max(rng * 0.42, entry * 0.008, tick)
        target2 = min(hi + tick, entry + max(rng * 0.72, entry * 0.018, tick * 2))
        if target2 <= target1:
            target2 = round(target1 + max(tick, entry * 0.004), 2)

    return {
        "support": round(lo, 2),
        "resistance": round(hi, 2),
        "stop": round(stop, 2),
        "target1": round(target1, 2),
        "target2": round(target2, 2),
    }


def kdj_signal(row: dict) -> str:
    k = _safe_float(row.get("KD_K"))
    d = _safe_float(row.get("KD_D"))
    j = _safe_float(row.get("KDJ_J"))
    if k is None or d is None:
        return "資料不足"
    if k > d and (j is None or j < 90):
        return "偏多"
    if k < d and (j is None or j > 10):
        return "偏空"
    return "中性"


def macd_signal(row: dict) -> str:
    dif = _safe_float(row.get("MACD_DIF"))
    dea = _safe_float(row.get("MACD_DEA"))
    bar = _safe_float(row.get("MACD_BAR"))
    if dif is None or dea is None:
        return "資料不足"
    if dif > dea and (bar is None or bar >= 0):
        return "偏多"
    if dif < dea and (bar is None or bar <= 0):
        return "偏空"
    return "中性"


def build_position_plan(row: dict, entry_price: float, side: str = "做多", shares: int = 1000, mode: str = "當沖", reference_quote: dict | None = None, level_override: dict | None = None):
    reference_quote = reference_quote or {}
    current_price = _safe_float(reference_quote.get("price")) or _pick_live_price(row)
    level_source = "盤前／日線分析"
    if isinstance(level_override, dict) and all(k in level_override for k in ("support", "resistance", "stop", "target1", "target2")):
        support = _safe_float(level_override.get("support"))
        resistance = _safe_float(level_override.get("resistance"))
        stop = _safe_float(level_override.get("stop"))
        target1 = _safe_float(level_override.get("target1"))
        target2 = _safe_float(level_override.get("target2"))
        level_source = "盤中波動區間推算"
    elif side == "做空":
        short_map = build_short_strategy(row)
        support = _safe_float(short_map.get("空方短期支撐"))
        resistance = _safe_float(short_map.get("空方短期壓力"))
        stop = _safe_float(short_map.get("空方停損"))
        target1 = _safe_float(short_map.get("空方中繼目標")) or support
        target2 = _safe_float(short_map.get("空方跌破目標"))
    else:
        support = _safe_float(row.get("支撐"))
        resistance = _safe_float(row.get("短期壓力"))
        stop = _safe_float(row.get("停損"))
        target1 = _safe_float(row.get("中繼目標")) or _safe_float(row.get("目標")) or resistance
        target2 = _safe_float(row.get("突破目標")) or resistance

    if side == "做空":
        pnl_per_share = (entry_price - current_price) if current_price is not None else None
    else:
        pnl_per_share = (current_price - entry_price) if current_price is not None else None

    pnl_amount = pnl_per_share * shares if pnl_per_share is not None else None
    pnl_pct = (pnl_per_share / entry_price * 100) if (pnl_per_share is not None and entry_price) else None

    if side == "做空":
        if resistance is not None and entry_price >= resistance:
            cost_zone = "成本在空方壓力上方，位置相對有利"
        elif support is not None and entry_price <= support:
            cost_zone = "成本已接近空方支撐，追空位置偏差"
        else:
            cost_zone = "成本在空方壓力與支撐之間"
    else:
        if support is not None and entry_price <= support:
            cost_zone = "成本在支撐下方，位置相對有利"
        elif resistance is not None and entry_price >= resistance:
            cost_zone = "成本在壓力上方，位置較被動"
        else:
            cost_zone = "成本在支撐與壓力之間"

    if side == "做空":
        if current_price is None:
            action = "資料不足"
            reason = "目前無法取得最新價格，先保守觀察。"
        elif pnl_pct is not None and pnl_pct <= -12:
            action = "先停損回補，不再硬抱"
            reason = f"這筆空單目前逆勢虧損 {abs(pnl_pct):.2f}% ，已不是等待反彈結束的節奏，先把風險收掉比較實際。"
        elif pnl_pct is not None and pnl_pct <= -8:
            action = "空單先縮部位，別再硬扛"
            reason = f"這筆空單目前逆勢虧損 {abs(pnl_pct):.2f}% ，成本位置已經偏掉，先縮部位或先回補會比續抱更合理。"
        elif stop is not None and current_price >= stop:
            action = "碰空方停損，先回補"
            reason = "現價已碰到空方停損區，代表這段空方節奏已被破壞，先回補處理比較乾脆。"
        elif target2 is not None and current_price <= target2:
            action = "跌破目標已到，先回補大半"
            reason = "現價已到跌破目標區，先回補大半、把獲利收進來，比繼續貪更穩。"
        elif target1 is not None and current_price <= target1:
            action = "到第一回補區，先回補一部分"
            reason = "現價已到第一回補區，先回補一部分，剩下部位再看有沒有續跌空間。"
        elif resistance is not None and current_price >= resistance:
            action = "反彈到壓力，別急著追空"
            reason = "現價已反彈到空方壓力附近，先等轉弱訊號出來，再決定要不要補空。"
        else:
            action = "續抱沒問題，但停損要跟緊"
            reason = "目前還在空方節奏內，可以續抱，但反彈若站回壓力上方就不要再拖。"
    else:
        if current_price is None:
            action = "資料不足"
            reason = "目前無法取得最新價格，先保守觀察。"
        elif pnl_pct is not None and pnl_pct <= -12:
            action = "先停損出場，不要再攤平"
            reason = f"這筆多單目前虧損 {abs(pnl_pct):.2f}% ，已經不是正常震盪範圍，先出場控風險會比較實際。"
        elif pnl_pct is not None and pnl_pct <= -8:
            action = "先縮或先出，不建議再硬抱"
            reason = f"這筆多單目前虧損 {abs(pnl_pct):.2f}% ，成本位置已經偏掉，先縮部位或先出會比死抱更合理。"
        elif stop is not None and current_price <= stop:
            action = "跌到停損區，先出場"
            reason = "現價已跌到停損區，這時候先出場控風險，比期待它自己彈回來更實際。"
        elif support is not None and current_price < support:
            action = "跌破支撐，先減碼"
            reason = "現價已跌破短期支撐，這種走法通常先減碼會比較安全，剩下部位再看有沒有站回。"
        elif target2 is not None and current_price >= target2:
            action = "到第二目標，先收大半"
            reason = "現價已到第二目標區，先收大半、剩餘部位再用移動停利去跟。"
        elif target1 is not None and current_price >= target1:
            action = "到第一目標，先收一部分"
            reason = "現價已到第一目標區，先收一部分，把帳上獲利鎖住，再看能不能續攻。"
        elif resistance is not None and current_price >= resistance:
            action = "碰壓力先減碼"
            reason = "現價已靠近短壓，若突破不了，先減碼會比等拉回再處理更順手。"
        else:
            action = "續抱可以，但支撐要守住"
            reason = "目前還在支撐與壓力區間內，可以續抱，但一旦支撐失守就不要再拖。"

    risk_pct = None
    to_target1_pct = None
    if entry_price:
        if side == "做空":
            if stop is not None:
                risk_pct = (stop - entry_price) / entry_price * 100
            if target1 is not None:
                to_target1_pct = (entry_price - target1) / entry_price * 100
        else:
            if stop is not None:
                risk_pct = (entry_price - stop) / entry_price * 100
            if target1 is not None:
                to_target1_pct = (target1 - entry_price) / entry_price * 100

    return {
        "side": side,
        "mode": mode,
        "entry_price": entry_price,
        "shares": shares,
        "current_price": current_price,
        "support": support,
        "resistance": resistance,
        "stop": stop,
        "target1": target1,
        "target2": target2,
        "pnl_amount": pnl_amount,
        "pnl_pct": pnl_pct,
        "risk_pct": risk_pct,
        "to_target1_pct": to_target1_pct,
        "cost_zone": cost_zone,
        "action": action,
        "reason": reason,
        "price_source": fmt_text(reference_quote.get("source", "")) or "--",
        "price_time": fmt_text(reference_quote.get("time", "")) or "",
        "day_high": _safe_float(reference_quote.get("high")),
        "day_low": _safe_float(reference_quote.get("low")),
        "day_volume": _safe_float(reference_quote.get("volume")),
        "level_source": level_source,
    }


def calc_position_rr(entry: float | None, stop: float | None, target1: float | None, side: str = "做多"):
    if entry is None or stop is None or target1 is None:
        return None
    try:
        entry = float(entry)
        stop = float(stop)
        target1 = float(target1)
    except Exception:
        return None
    risk = (entry - stop) if side != "做空" else (stop - entry)
    reward = (target1 - entry) if side != "做空" else (entry - target1)
    if risk <= 0:
        return None
    return round(reward / risk, 2)


def build_original_position_targets(row: dict, side: str = "做多"):
    if side == "做空":
        short_map = build_short_strategy(row)
        plan_entry = _safe_float(short_map.get("空方建議進場"))
        stop = _safe_float(short_map.get("空方停損"))
        resistance = _safe_float(short_map.get("空方短期壓力"))
        mid_target = _safe_float(short_map.get("空方中繼目標"))
        breakout_target = _safe_float(short_map.get("空方跌破目標"))
        rr = calc_position_rr(plan_entry, stop, mid_target, side)
        return {
            "plan_entry": plan_entry,
            "plan_stop": stop,
            "plan_resistance": resistance,
            "plan_mid_target": mid_target,
            "plan_breakout_target": breakout_target,
            "plan_rr": rr,
            "plan_conclusion": fmt_text(row.get("結論", "")) or "--",
            "plan_signal": fmt_text(row.get("交易訊號", "")) or "--",
        }

    plan_entry = _safe_float(row.get("進場"))
    stop = _safe_float(row.get("停損"))
    resistance = _safe_float(row.get("短期壓力"))
    mid_target = _safe_float(row.get("中繼目標"))
    breakout_target = _safe_float(row.get("突破目標"))
    rr = _safe_float(row.get("風報比"))
    if rr is None:
        rr = calc_position_rr(plan_entry, stop, _safe_float(row.get("目標")) or resistance, side)
    return {
        "plan_entry": plan_entry,
        "plan_stop": stop,
        "plan_resistance": resistance,
        "plan_mid_target": mid_target,
        "plan_breakout_target": breakout_target,
        "plan_rr": rr,
        "plan_conclusion": fmt_text(row.get("結論", "")) or "--",
        "plan_signal": fmt_text(row.get("交易訊號", "")) or "--",
    }


def _remaining_pct(current: float | None, ref: float | None, side: str = "做多", target_kind: str = "up"):
    if current is None or ref is None or current == 0:
        return None
    if side == "做空":
        if target_kind == "risk":
            return round((ref - current) / current * 100, 2)
        return round((current - ref) / current * 100, 2)
    if target_kind == "risk":
        return round((current - ref) / current * 100, 2)
    return round((ref - current) / current * 100, 2)


def evaluate_entry_alignment(actual_entry: float | None, plan_entry: float | None, side: str = "做多"):
    if actual_entry is None or plan_entry is None or plan_entry == 0:
        return {"entry_gap_pct": None, "alignment": "缺原始進場資料", "alignment_note": "目前無法比對你的實際成本與原始進場計畫。"}
    gap_pct = round((actual_entry - plan_entry) / plan_entry * 100, 2)
    if abs(gap_pct) <= 2:
        alignment = "符合原始計畫"
        note = f"你的成本和原始進場差 {gap_pct:+.2f}% ，仍在可接受範圍內。"
    elif side == "做空" and gap_pct > 2:
        alignment = "高於原始空點"
        note = f"你的空單成本比原始空點高 {gap_pct:.2f}% ，位置相對有利。"
    elif side == "做空" and gap_pct < -2:
        alignment = "低於原始空點"
        note = f"你的空單成本比原始空點低 {abs(gap_pct):.2f}% ，屬提早偏離，風險緩衝較少。"
    elif side != "做空" and gap_pct > 2:
        alignment = "高於原始進場"
        note = f"你的多單成本比原始進場高 {gap_pct:.2f}% ，偏追價。"
    else:
        alignment = "低於原始進場"
        note = f"你的多單成本比原始進場低 {abs(gap_pct):.2f}% ，成本相對有利。"
    return {"entry_gap_pct": gap_pct, "alignment": alignment, "alignment_note": note}



def build_dynamic_hold_decision(row: dict, plan: dict, tracking: dict, intraday_primary: bool = False):
    side = plan.get("side", "做多")
    current = _safe_float(plan.get("current_price"))
    entry = _safe_float(plan.get("entry_price"))
    pnl_pct = _safe_float(plan.get("pnl_pct"))
    stop = _safe_float(plan.get("stop"))
    support = _safe_float(plan.get("support"))
    resistance = _safe_float(plan.get("resistance"))
    target1 = _safe_float(plan.get("target1"))
    target2 = _safe_float(plan.get("target2"))
    conclusion = fmt_text(tracking.get("plan_conclusion", ""))
    signal = fmt_text(tracking.get("plan_signal", ""))
    mode = fmt_text(plan.get("mode", "當沖")) or "當沖"

    technical_structure = "中性"
    position_state = "觀察中"
    risk_level = "中"
    advice = "觀察"
    action_hint = "先觀察"
    summary = "先看目前價格相對停損、壓力與目標的位置，再決定是否續抱。"
    reasons = []

    def pct_text(val):
        return f"{val:.2f}%" if val is not None else "--"

    if current is None:
        return {
            "advice": "資料不足",
            "risk_level": "高",
            "technical_structure": "資料不足",
            "position_state": "無法判讀",
            "action_hint": "先觀察",
            "summary": "目前抓不到最新價格，先不要用這頁做盤中決策。",
            "reasons": ["最新價格未更新", "請稍後再按一次計算持倉判讀"],
        }

    to_stop_pct = tracking.get("to_stop_pct")
    to_resistance_pct = tracking.get("to_resistance_pct")
    to_mid_pct = tracking.get("to_mid_pct")
    to_breakout_pct = tracking.get("to_breakout_pct")

    # 當沖優先：先看實際成本損益，再看技術位置
    if pnl_pct is not None:
        if pnl_pct >= 3:
            position_state = "獲利擴大"
        elif pnl_pct >= 1:
            position_state = "穩定獲利"
        elif pnl_pct >= 0:
            position_state = "小幅獲利"
        elif pnl_pct > -1.5:
            position_state = "逆風警戒"
        elif pnl_pct > -3:
            position_state = "逆風擴大"
        else:
            position_state = "部位失效"

    if side == "做空":
        # 先處理當沖空單成本失控
        if pnl_pct is not None and pnl_pct <= -3:
            technical_structure = "失效"
            risk_level = "極高"
            advice = "出場"
            action_hint = "優先回補"
            summary = f"這筆空單目前虧損 {abs(pnl_pct):.2f}% ，以當沖邏輯已屬異常部位，先回補控風險，不再用技術面硬抱。"
            reasons = [
                f"當沖空單虧損 {abs(pnl_pct):.2f}% 已超過異常門檻",
                f"成本 {fmt_price_with_unit(entry)} 與現價 {fmt_price_with_unit(current)} 差距過大",
            ]
        elif pnl_pct is not None and pnl_pct <= -2:
            technical_structure = "失效"
            risk_level = "極高"
            advice = "出場"
            action_hint = "部位失效先回補"
            summary = f"這筆空單目前虧損 {abs(pnl_pct):.2f}% ，已超過一般當沖可容忍範圍，先回補比續抱合理。"
            reasons = [
                f"當沖空單虧損 {abs(pnl_pct):.2f}% 已超過部位失效門檻",
                "這不是等待再轉弱的節奏，先把風險收掉",
            ]
        elif pnl_pct is not None and pnl_pct <= -1.5:
            technical_structure = "轉弱"
            risk_level = "高"
            advice = "出場"
            action_hint = "優先回補"
            summary = f"這筆空單目前虧損 {abs(pnl_pct):.2f}% ，以當沖來說已偏大，不建議再期待技術面自己修復。"
            reasons = [
                f"當沖空單虧損 {abs(pnl_pct):.2f}% 已超過警戒門檻",
                "先回補、等更好的空點，比硬扛更務實",
            ]
        elif stop is not None and current >= stop:
            technical_structure = "偏弱"
            risk_level = "高"
            advice = "出場"
            action_hint = "碰停損先回補"
            summary = "現價已到空方停損區，原本空方節奏被破壞，這時候續抱風險高於報酬。"
            reasons = [f"現價 {fmt_price_with_unit(current)} 已到空方停損 {fmt_price_with_unit(stop)}", "原始空方計畫失效，先把風險收掉"]
        elif target2 is not None and current <= target2:
            technical_structure = "偏強"
            risk_level = "低"
            advice = "減碼"
            action_hint = "高獲利區先回補大半"
            summary = "現價已到跌破目標區，代表空方計畫已跑到高獲利段，先回補大半最穩。"
            reasons = [f"已到第二目標 {fmt_price_with_unit(target2)}", "先鎖住大部分獲利，再決定剩餘部位"]
        elif target1 is not None and current <= target1:
            technical_structure = "偏強"
            risk_level = "中低"
            advice = "減碼續抱"
            action_hint = "第一回補區先收一部分"
            summary = "現價已到第一回補區，先回補一部分，把已到手的空方優勢先鎖住。"
            reasons = [f"已到第一目標 {fmt_price_with_unit(target1)}", "剩餘部位再看有沒有續跌空間"]
        elif resistance is not None and current >= resistance * 0.985:
            technical_structure = "轉弱"
            risk_level = "中高"
            advice = "觀察"
            action_hint = "靠壓力不追空"
            summary = "現價已反彈到空方壓力附近，這裡比較像等轉弱訊號，不適合直接樂觀續抱。"
            reasons = [f"現價接近空方壓力 {fmt_price_with_unit(resistance)}", "若再站上去，空方結構就會被破壞"]
        elif not intraday_primary and (conclusion in ["中性偏多", "看多"] or signal == "🔥進場"):
            technical_structure = "轉弱"
            risk_level = "中高"
            advice = "觀察"
            action_hint = "嚴設停損續抱"
            summary = "整體技術面對空方不算友善，雖然空方計畫還沒失效，但不適合太樂觀。"
            reasons = [f"原始結論：{conclusion or '--'}", f"原始訊號：{signal or '--'}"]
        elif pnl_pct is not None and pnl_pct >= 3:
            technical_structure = "偏強"
            risk_level = "低"
            advice = "續抱"
            action_hint = "續抱並移動停利"
            summary = "這筆空單已進入當沖優勢段，可續抱，但停利要跟上，不要讓獲利回吐太多。"
            reasons = [f"目前獲利 {pnl_pct:.2f}%", f"到停損剩餘：{pct_text(to_stop_pct)}"]
        else:
            technical_structure = "中性偏強"
            risk_level = "中" if (to_stop_pct is not None and to_stop_pct < 3) else "中低"
            advice = "續抱"
            action_hint = "續抱但停損要跟緊"
            summary = "目前還在空方節奏內，可以續抱，但只要站回壓力上方就不要再拖。"
            reasons = [f"到停損剩餘：{pct_text(to_stop_pct)}", f"到第一目標空間：{pct_text(to_mid_pct)}"]
    else:
        # 先處理當沖多單成本失控
        if pnl_pct is not None and pnl_pct <= -3:
            technical_structure = "失效"
            risk_level = "極高"
            advice = "出場"
            action_hint = "優先出場"
            summary = f"這筆多單目前虧損 {abs(pnl_pct):.2f}% ，以當沖邏輯已屬異常部位，先出場控風險，不再用技術面硬抱。"
            reasons = [
                f"當沖多單虧損 {abs(pnl_pct):.2f}% 已超過異常門檻",
                f"成本 {fmt_price_with_unit(entry)} 與現價 {fmt_price_with_unit(current)} 差距過大",
            ]
        elif pnl_pct is not None and pnl_pct <= -2:
            technical_structure = "失效"
            risk_level = "極高"
            advice = "出場"
            action_hint = "部位失效優先出場"
            summary = f"這筆多單目前虧損 {abs(pnl_pct):.2f}% ，已超過一般當沖可容忍範圍，先出場比續抱合理。"
            reasons = [
                f"當沖多單虧損 {abs(pnl_pct):.2f}% 已超過部位失效門檻",
                "這不是一般回檔，先把風險收掉更務實",
            ]
        elif pnl_pct is not None and pnl_pct <= -1.5:
            technical_structure = "轉弱"
            risk_level = "高"
            advice = "出場"
            action_hint = "優先出場"
            summary = f"這筆多單目前虧損 {abs(pnl_pct):.2f}% ，以當沖來說已偏大，不建議再期待技術面自己修復。"
            reasons = [
                f"當沖多單虧損 {abs(pnl_pct):.2f}% 已超過警戒門檻",
                "先出場、等更好的點，比硬抱更合理",
            ]
        elif stop is not None and current <= stop:
            technical_structure = "偏弱"
            risk_level = "高"
            advice = "出場"
            action_hint = "碰停損先出"
            summary = "現價已到停損區，原本多方計畫失效，這時候再抱下去風險通常大於報酬。"
            reasons = [f"現價 {fmt_price_with_unit(current)} 已到停損 {fmt_price_with_unit(stop)}", "先保住本金，不要再期待它自己彈回來"]
        elif support is not None and current < support:
            technical_structure = "轉弱"
            risk_level = "高" if pnl_pct is not None and pnl_pct <= 0 else "中高"
            advice = "減碼" if pnl_pct is not None and pnl_pct > 0 else "出場"
            action_hint = "跌破支撐先保守"
            summary = "現價已跌破原本支撐，這表示多方結構開始轉弱，續抱時要更保守。"
            reasons = [f"現價跌破支撐 {fmt_price_with_unit(support)}", "若站不回去，這筆多單優勢會越來越小"]
        elif target2 is not None and current >= target2:
            technical_structure = "偏強"
            risk_level = "低"
            advice = "減碼"
            action_hint = "高獲利區先收大半"
            summary = "現價已到第二目標區，代表這筆多單已跑到高獲利段，先收大半最穩。"
            reasons = [f"已到第二目標 {fmt_price_with_unit(target2)}", "先把大部分獲利鎖住，剩餘部位再用停利續抱"]
        elif target1 is not None and current >= target1:
            technical_structure = "偏強"
            risk_level = "中低"
            advice = "減碼續抱"
            action_hint = "第一目標先收一部分"
            summary = "現價已到第一目標區，先收一部分，把帳上獲利鎖住，再看能不能續攻。"
            reasons = [f"已到第一目標 {fmt_price_with_unit(target1)}", "剩餘部位可視走勢決定是否續抱"]
        elif resistance is not None and current >= resistance * 0.985:
            technical_structure = "中性"
            risk_level = "中"
            advice = "減碼續抱"
            action_hint = "碰壓力先減碼"
            summary = "現價已靠近短期壓力，這裡比較像先觀察突破結果，不適合無腦續抱。"
            reasons = [f"現價接近短壓 {fmt_price_with_unit(resistance)}", "若突破不了，容易先回檔"]
        elif not intraday_primary and (conclusion in ["中性偏空", "看空"] or signal == "❌不進"):
            technical_structure = "轉弱"
            risk_level = "中高"
            advice = "觀察"
            action_hint = "不加碼、嚴設停損"
            summary = "雖然你這筆單不一定立刻有問題，但整體技術結構對多方不算友善，續抱時要更重視風控。"
            reasons = [f"原始結論：{conclusion or '--'}", f"原始訊號：{signal or '--'}"]
        elif pnl_pct is not None and pnl_pct >= 3:
            technical_structure = "偏強"
            risk_level = "低"
            advice = "續抱"
            action_hint = "續抱並移動停利"
            summary = "這筆多單已進入當沖優勢段，可續抱，但停利要跟上，不要讓獲利回吐太多。"
            reasons = [f"目前獲利 {pnl_pct:.2f}%", f"到停損剩餘：{pct_text(to_stop_pct)}"]
        elif pnl_pct is not None and pnl_pct >= 1:
            technical_structure = "中性偏強"
            risk_level = "中低"
            advice = "續抱"
            action_hint = "續抱但不戀戰"
            summary = "目前仍是當沖可接受的獲利段，可以續抱，但靠近壓力就要提高警覺。"
            reasons = [f"目前獲利 {pnl_pct:.2f}%", f"到短壓剩餘：{pct_text(to_resistance_pct)}"]
        else:
            technical_structure = "中性偏強"
            risk_level = "中" if (to_stop_pct is not None and to_stop_pct < 3) else "中低"
            advice = "續抱"
            action_hint = "續抱但支撐要守住"
            summary = "目前還在支撐與目標之間，這個價位仍可續抱，但一旦支撐失守就不要再拖。"
            reasons = [f"到停損剩餘：{pct_text(to_stop_pct)}", f"到第一目標空間：{pct_text(to_mid_pct)}"]

    # 附加說明：當沖模式下，若成本與現價偏離太大，額外提示部位已不適合再用盤中技術續抱
    if mode == "當沖" and pnl_pct is not None and pnl_pct <= -3 and side != "做空":
        reasons = reasons[:3] + ["這筆部位已不適合再用當沖續抱邏輯處理"]
    if mode == "當沖" and pnl_pct is not None and pnl_pct <= -3 and side == "做空":
        reasons = reasons[:3] + ["這筆部位已不適合再用當沖空單續抱邏輯處理"]

    return {
        "advice": advice,
        "risk_level": risk_level,
        "technical_structure": technical_structure,
        "position_state": position_state,
        "action_hint": action_hint,
        "summary": summary,
        "reasons": reasons[:4],
    }



def build_position_tracking(row: dict, plan: dict):
    side = plan.get("side", "做多")
    current = _safe_float(plan.get("current_price"))
    support = _safe_float(plan.get("support"))
    resistance = _safe_float(plan.get("resistance"))
    stop = _safe_float(plan.get("stop"))
    target1 = _safe_float(plan.get("target1"))
    target2 = _safe_float(plan.get("target2"))
    actual_entry = _safe_float(plan.get("entry_price"))

    original = build_original_position_targets(row, side)
    align = evaluate_entry_alignment(actual_entry, original.get("plan_entry"), side)

    to_stop_pct = _remaining_pct(current, stop, side, "risk")
    to_resistance_pct = _remaining_pct(current, resistance, side, "up")
    to_mid_pct = _remaining_pct(current, target1, side, "up")
    to_breakout_pct = _remaining_pct(current, target2, side, "up")

    conclusion = fmt_text(row.get("結論", ""))
    signal = fmt_text(row.get("交易訊號", ""))

    status = "正常推進"
    status_note = "目前價格仍大致沿著原始計畫區間推進。"
    if current is None:
        status = "資料不足"
        status_note = "目前抓不到最新價格，先保守觀察。"
    elif side == "做空":
        if stop is not None and current >= stop:
            status = "接近停損"
            status_note = "現價已逼近或觸及空方停損區，先把風險放在第一位。"
        elif resistance is not None and current >= resistance * 0.985:
            status = "接近壓力"
            status_note = "現價已回到空方壓力附近，若再站上去，原始空方計畫就會被破壞。"
        elif target2 is not None and current <= target2:
            status = "已達突破"
            status_note = "現價已到跌破目標區，屬於原始空方計畫的深獲利段。"
        elif target1 is not None and current <= target1:
            status = "已達中繼"
            status_note = "現價已到第一回補區，可視為原始計畫的中繼完成。"
        elif conclusion in ["中性偏多", "看多"] or signal == "🔥進場":
            status = "結構轉弱"
            status_note = "原始空方部位遇到偏多結構，宜保守。"
    else:
        if stop is not None and current <= stop:
            status = "接近停損"
            status_note = "現價已逼近或觸及停損區，這時要先保住本金。"
        elif support is not None and current < support:
            status = "結構轉弱"
            status_note = "現價已跌破原本支撐，代表多方結構轉弱。"
        elif target2 is not None and current >= target2:
            status = "已達突破"
            status_note = "現價已到突破目標區，屬於原始計畫的高獲利段。"
        elif target1 is not None and current >= target1:
            status = "已達中繼"
            status_note = "現價已到中繼目標區，可考慮開始分批鎖利。"
        elif resistance is not None and current >= resistance * 0.985:
            status = "接近壓力"
            status_note = "現價已靠近短期壓力，接下來重點是能不能有效突破。"
        elif conclusion in ["中性偏空", "看空"] or signal == "❌不進":
            status = "結構轉弱"
            status_note = "系統結構轉弱，續抱時要更重視風控。"

    return {
        **original,
        **align,
        "status": status,
        "status_note": status_note,
        "to_stop_pct": to_stop_pct,
        "to_resistance_pct": to_resistance_pct,
        "to_mid_pct": to_mid_pct,
        "to_breakout_pct": to_breakout_pct,
    }


def render_position_status_chips(chips: list[str]):
    chip_html = "".join([f'<span class="status-chip">{html_escape(x)}</span>' for x in chips if str(x).strip()])
    if chip_html:
        st.markdown(f'<div style="margin-top:0.2rem;">{chip_html}</div>', unsafe_allow_html=True)


def render_single_stock_detail_panel(select_source: pd.DataFrame, df_result: pd.DataFrame, name_map: dict, market_info: dict):
    if select_source is None or len(select_source) == 0:
        return
    ensure_selected_code(select_source)
    # 選單鍵必須含 _code，避免不同股票「中文簡稱相同」時 dict 覆寫導致選單與詳細區綁錯檔
    option_map = {_detail_select_label(row): row["_code"] for _, row in select_source.iterrows()}
    reverse_map = {v: k for k, v in option_map.items()}

    with st.container(border=True):
        st.markdown("### 單股詳細分析")
        labels = list(option_map.keys())
        current_label = reverse_map.get(st.session_state.selected_code, labels[0])
        current_index = labels.index(current_label) if current_label in labels else 0

        # ── 導航：以 selected_code 為唯一 source of truth ──────────────────
        # Streamlit 規則：widget key 渲染後不能在同一 run 內直接寫入
        # session_state[widget_key]。改用 index= 讓 selectbox 自動對齊，
        # 按鈕與快速查看只改 selected_code，不碰 widget key。
        current_label = reverse_map.get(st.session_state.selected_code, labels[0])
        current_index = labels.index(current_label) if current_label in labels else 0
        code_values = list(option_map.values())
        pos_idx = (code_values.index(st.session_state.selected_code) + 1
                   if st.session_state.selected_code in code_values else 1)

        def _on_select_change(sel_key):
            val = st.session_state.get(sel_key)
            if val and val in option_map:
                st.session_state.selected_code = option_map[val]

        if st.session_state.mobile_mode:
            nav1, nav2 = st.columns(2)
            nav1.button("上一檔", use_container_width=True, key="detail_prev_mobile",
                        on_click=move_selected, args=(select_source, -1))
            nav2.button("下一檔", use_container_width=True, key="detail_next_mobile",
                        on_click=move_selected, args=(select_source, 1))
            st.selectbox("選擇查看單股", labels, index=current_index,
                         key="detail_select_mobile",
                         on_change=_on_select_change, args=("detail_select_mobile",))
            st.caption(f"目前位置：{pos_idx} / {len(option_map)}")
        else:
            nav1, nav2, nav3, nav4 = st.columns([1, 1, 4, 2])
            with nav1:
                st.button("上一檔", use_container_width=True, key="detail_prev_pc",
                          on_click=move_selected, args=(select_source, -1))
            with nav2:
                st.button("下一檔", use_container_width=True, key="detail_next_pc",
                          on_click=move_selected, args=(select_source, 1))
            with nav3:
                st.selectbox("選擇查看單股", labels, index=current_index,
                             key="detail_select_pc",
                             on_change=_on_select_change, args=("detail_select_pc",))
            with nav4:
                st.caption(f"目前位置：{pos_idx} / {len(option_map)}")

            st.markdown("#### 快速查看")
            quick_cols = st.columns(min(5, len(select_source)))
            top_quick = select_source.head(5)
            for col, (_, quick_row) in zip(quick_cols, top_quick.iterrows()):
                if col.button(quick_row["股票"], use_container_width=True, key=f"quick_{quick_row['_code']}"):
                    # 只改 selected_code，不碰已渲染的 widget key
                    st.session_state.selected_code = quick_row["_code"]

        _matched = df_result[df_result["_code"] == st.session_state.selected_code]
        if _matched.empty:
            st.warning("選取的股票不在目前結果中，請重新選擇。")
            return
        row = _matched.iloc[0].copy()

        f1, f2 = st.columns([1,5])
        with f1:
            if st.session_state.selected_code not in st.session_state.favorites:
                if st.button("加入最愛", key="detail_add_fav"):
                    st.session_state.favorites.append(st.session_state.selected_code)
                    save_favorites(st.session_state.favorites)
            else:
                if st.button("移除最愛", key="detail_remove_fav"):
                    st.session_state.favorites = [x for x in st.session_state.favorites if x != st.session_state.selected_code]
                    save_favorites(st.session_state.favorites)
        with f2:
            if st.session_state.selected_code in st.session_state.favorites:
                st.success(f"{display_name(st.session_state.selected_code, name_map)} 已收藏到我的最愛。")

        st.subheader(f"{reverse_map[st.session_state.selected_code]} 詳細分析")
        st.caption("已分析結果內切換單股時，只更新本區塊；上方總表與候選股面板不會重新重算。")

        detail_view_mode = st.radio("檢視模式", ["決策摘要", "技術細節", "官方資料"], horizontal=True, key="detail_view_mode")

        if detail_view_mode == "決策摘要":
            # 三大法人資料已在搜尋/自動挑股時預先補入，此 toggle 改為顯示偵錯面板
            show_inst_debug = st.toggle("顯示法人偵錯面板", value=False, key=f"load_inst_{st.session_state.selected_code}")
            row = pd.Series(dict(row))
            # 若預先補入的資料仍為「未載入」（例如舊版快照），補一次
            if str(row.get("法人方向", "")) in ("", "未載入", "None"):
                try:
                    row = pd.Series(resolve_institutional_context(
                        st.session_state.selected_code, row, fetch_institutional_bundle_all()
                    ))
                except Exception:
                    row["法人方向"] = "資料不足"
                    row["法人共振"] = "中性"
                    row["法人摘要"] = "三大法人資料不足"
            st.markdown("#### 核心摘要")
            def _fmt_row(key):
                v = row.get(key)
                try:
                    return f"{float(v):.2f}" if v is not None and v != "" else "--"
                except (ValueError, TypeError):
                    return str(v) if v else "--"
            core_top = [
                ("收盤", _fmt_row("收盤")),
                ("結論", str(row.get("結論", "--"))),
                ("交易訊號", str(row.get("交易訊號", "--"))),
            ]
            k1, k2, k3 = st.columns(3)
            for col, (label, value) in zip((k1, k2, k3), core_top):
                col.metric(label, value)

            st.markdown("#### 三大法人")
            def _safe_lot_metric(value):
                return fmt_lots(value)

            def _safe_pct_metric(value):
                num = safe_float_or_none(value)
                return f"{num:.2f}%" if num is not None else "--"

            inst_pairs = [
                ("法人方向", row.get("法人方向", "--")),
                ("法人共振", row.get("法人共振", "--")),
                ("三大法人買賣超(張)", _safe_lot_metric(row.get("三大法人買賣超", None))),
                ("法人佔成交量比%", _safe_pct_metric(row.get("法人佔成交量比%", None))),
                ("外資買賣超(張)", _safe_lot_metric(row.get("外資買賣超", None))),
                ("投信買賣超(張)", _safe_lot_metric(row.get("投信買賣超", None))),
            ]
            inst_cols_per_row = 2 if st.session_state.mobile_mode else 3
            for i in range(0, len(inst_pairs), inst_cols_per_row):
                cols = st.columns(inst_cols_per_row)
                for col, (label, value) in zip(cols, inst_pairs[i:i+inst_cols_per_row]):
                    col.metric(label, value)
            raw_inst_debug = {
                "三大法人買賣超_raw": row.get("三大法人買賣超", None),
                "外資買賣超_raw": row.get("外資買賣超", None),
                "投信買賣超_raw": row.get("投信買賣超", None),
                "法人佔成交量比_raw": row.get("法人佔成交量比%", None),
                "三大法人買賣超_parsed": safe_int_or_none(row.get("三大法人買賣超", None)),
                "外資買賣超_parsed": safe_int_or_none(row.get("外資買賣超", None)),
                "投信買賣超_parsed": safe_int_or_none(row.get("投信買賣超", None)),
                "法人佔成交量比_parsed": safe_float_or_none(row.get("法人佔成交量比%", None)),
            }
            st.caption(row.get("法人摘要", "三大法人資料不足，暫不納入籌碼判讀。"))
            with st.expander("法人抓取偵錯面板", expanded=(show_inst_debug and str(row.get("法人方向", "")) == "資料不足")):
                debug_rows = fetch_institutional_bundle_all().get("debug", []) if show_inst_debug else []
                hit_code = "是" if str(row.get("法人命中代碼", "否")) == "是" else "否"
                d1, d2, d3 = st.columns(3)
                d1.metric("本股代碼", str(st.session_state.selected_code or "").split(".")[0])
                d2.metric("命中法人資料", hit_code)
                d3.metric("最終採用來源", row.get("法人資料源", "NONE"))
                st.caption("遇到資料不足時，直接看下面這張表：哪個來源有回資料、解析命中幾筆、最後採用哪個來源。")
                if debug_rows:
                    st.dataframe(pd.DataFrame(debug_rows), use_container_width=True, hide_index=True)
                else:
                    st.info("目前沒有可用的法人抓取偵錯資料。")
                with st.expander("法人欄位值偵錯", expanded=False):
                    st.json({k: ("--" if (isinstance(v, float) and pd.isna(v)) else v) for k, v in raw_inst_debug.items()})

            st.markdown("#### 綜合評分")
            st.radio(
                "評分面向",
                options=["long", "short"],
                format_func=lambda x: "📈 做多評分" if x == "long" else "📉 做空評分",
                horizontal=True,
                key="selected_side",
                help="與下方策略區連動：選做多只看做多評分與做多策略，選做空只看做空評分與做空策略。",
            )
            df_chart = get_symbol_indicator_chart(st.session_state.selected_code)
            row_dict = row.to_dict() if hasattr(row, "to_dict") else dict(row)
            long_metrics = long_pick_compute(df_chart, row_dict) if df_chart is not None and not df_chart.empty else None
            short_metrics = short_pick_compute(df_chart, row_dict) if df_chart is not None and not df_chart.empty else None

            def _render_score_panel(prefix, label, metrics, threshold=60):
                key_total = f"{prefix}_total"
                key_trend = f"{prefix}_trend"
                key_mom = f"{prefix}_momentum"
                key_vol = f"{prefix}_volume"
                key_chip = f"{prefix}_chip"
                key_risk = f"{prefix}_risk"
                key_reason = f"{prefix}_reason"
                key_warning = f"{prefix}_warning"
                key_chg5d = f"{prefix}_chg5d"
                key_vol_ratio = f"{prefix}_vol_ratio"

                if st.session_state.mobile_mode:
                    m1, m2 = st.columns(2)
                    m1.metric(f"{label}總分", str(metrics[key_total]))
                    m2.metric("入選原因", metrics[key_reason])
                    m3, m4, m5 = st.columns(3)
                    m3.metric("趨勢", f'{metrics[key_trend]}/30')
                    m4.metric("動能", f'{metrics[key_mom]}/25')
                    m5.metric("量價", f'{metrics[key_vol]}/20')
                    m6, m7 = st.columns(2)
                    m6.metric("籌碼", f'{metrics[key_chip]}/15')
                    m7.metric("風險", f'{metrics[key_risk]}')
                else:
                    m1, m2, m3, m4, m5, m6 = st.columns(6)
                    m1.metric(f"{label}總分", str(metrics[key_total]))
                    m2.metric("趨勢", f'{metrics[key_trend]}/30')
                    m3.metric("動能", f'{metrics[key_mom]}/25')
                    m4.metric("量價", f'{metrics[key_vol]}/20')
                    m5.metric("籌碼", f'{metrics[key_chip]}/15')
                    m6.metric("風險", f'{metrics[key_risk]}')
                risk_detail_key = f"{prefix}_risk_detail"
                risk_label = metrics.get(risk_detail_key) or (
                    "（評分資料不足，無法列出風險明細。）"
                )
                with st.expander(f"展開{label}評分明細", expanded=False):
                    detail_items = [
                        {"項目": "趨勢分", "分數": metrics[key_trend], "滿分": 30},
                        {"項目": "動能分", "分數": metrics[key_mom], "滿分": 25, "說明": f'5日漲幅{metrics[key_chg5d]:.1f}%'},
                        {"項目": "量價分", "分數": metrics[key_vol], "滿分": 20, "說明": f'量比20日={metrics[key_vol_ratio]:.1f}倍'},
                        {"項目": "籌碼分", "分數": metrics[key_chip], "滿分": 15},
                        {"項目": "風險扣分", "分數": metrics[key_risk], "滿分": 0, "說明": risk_label},
                    ]
                    st.dataframe(pd.DataFrame(detail_items), use_container_width=True, hide_index=True)
                    st.caption("「風險扣分」滿分欄為 0 表示此列為扣分項而非加分項；負值會與前四項加總後再截斷至 0～100 為總分。")
                if metrics.get(key_warning):
                    st.caption(f'⚠️ 風險警示：{metrics[key_warning]}')
                if metrics[key_total] >= threshold:
                    st.caption(f'✅ {label}總分 {metrics[key_total]} ≥ {threshold}，符合{label}門檻')
                else:
                    st.caption(f'⚠️ {label}總分 {metrics[key_total]} < {threshold}，未達{label}門檻')

            _side = st.session_state.selected_side
            if _side == "long":
                if long_metrics is not None:
                    _render_score_panel("long", "做多", long_metrics)
                else:
                    st.info("不符合做多必要條件（收盤需 > MA5 > MA10、MA5 ≥ MA10）")
            else:
                if short_metrics is not None:
                    _render_score_panel("short", "做空", short_metrics)
                else:
                    st.info("不符合做空必要條件（需跌破均線、弱勢結構）")

            st.markdown("#### 策略區")
            _hdr_l, _hdr_r = st.columns([3, 1])
            with _hdr_l:
                st.caption("位置與進出場欄位已整合到做多／做空策略；與上方「評分面向」同步，只顯示目前選擇的一側。")
            with _hdr_r:
                st.caption("目前顯示：**做多策略**" if _side == "long" else "目前顯示：**做空策略**")

            def _fmt_price_with_delta(price_val, close_val):
                """回傳 (價格字串, delta字串)，delta 顯示距現價百分比（+漲/-跌）。"""
                try:
                    p = float(price_val)
                    if p <= 0:
                        return "--", None
                    price_str = f"{p:.2f}"
                    if close_val and close_val > 0:
                        pct = (p - close_val) / close_val * 100
                        delta_str = f"{pct:+.1f}%"
                    else:
                        delta_str = None
                    return price_str, delta_str
                except (TypeError, ValueError):
                    return "--", None

            _close_val = _safe_float(row.get("收盤"))

            if _side == "long":
                with st.expander("做多策略", expanded=True):
                    long_pairs = [
                        ("多方短期支撐", "支撐"),
                        ("多方短期壓力", "短期壓力"),
                        ("多方建議進場", "進場"),
                        ("多方停損",     "停損"),
                        ("多方中繼目標", "中繼目標"),
                        ("多方突破目標", "突破目標"),
                    ]
                    long_cols_per_row = 2 if st.session_state.mobile_mode else 3
                    for i in range(0, len(long_pairs), long_cols_per_row):
                        cols = st.columns(long_cols_per_row)
                        for col, (label, row_key) in zip(cols, long_pairs[i:i+long_cols_per_row]):
                            val_str, delta_str = _fmt_price_with_delta(row.get(row_key), _close_val)
                            col.metric(label, val_str, delta=delta_str, delta_color="normal")
                    st.caption("中繼目標＝進場到短壓55%處（日內可達）；突破目標＝短壓上方0.4 ATR（突破後追）。均已限制在漲停板98%內。")
            else:
                with st.expander("做空策略", expanded=True):
                    short_pairs = [
                        ("空方短期壓力", "空方短期壓力"),
                        ("空方短期支撐", "空方短期支撐"),
                        ("空方建議進場", "空方建議進場"),
                        ("空方停損",     "空方停損"),
                        ("空方中繼目標", "空方中繼目標"),
                        ("空方跌破目標", "空方跌破目標"),
                    ]
                    short_cols_per_row = 2 if st.session_state.mobile_mode else 3
                    for i in range(0, len(short_pairs), short_cols_per_row):
                        cols = st.columns(short_cols_per_row)
                        for col, (label, row_key) in zip(cols, short_pairs[i:i+short_cols_per_row]):
                            val_str, delta_str = _fmt_price_with_delta(row.get(row_key), _close_val)
                            col.metric(label, val_str, delta=delta_str, delta_color="inverse")
                    st.caption("空方策略：已將壓力、進場、停損、中繼目標與跌破目標限制在次日可交易邊界內，避免偏波段過寬數值。")

            render_reason_block(row, "本檔判斷理由")

            st.markdown("#### 摘要重點")
            st.info(row["摘要1"])
            st.info(row["摘要2"])
            st.info(row["摘要3"])
            if row["交易訊號"] == "🔥進場":
                st.success(f"操作建議：可列入優先觀察，重點看 {row['短期壓力']:.2f} 是否有效站上。")
            elif row["交易訊號"] == "⏳等待":
                st.warning(f"操作建議：先等突破或回測確認，不建議在 {row['收盤']:.2f} 直接追價。")
            elif row["交易訊號"] == "❌不進":
                st.error("操作建議：目前偏保守，先不進場，等待結構或燈號改善。")
            else:
                st.info("操作建議：可持續觀察量價與燈號變化，再決定是否列入候選。")

        elif detail_view_mode == "技術細節":
            tech_left, tech_right = (st.columns(2) if not st.session_state.mobile_mode else [st.container(), st.container()])
            with tech_left:
                st.markdown("#### 技術節奏")
                tech_pairs = [
                    ("星級", row["星級"]),
                    ("操作評級", row["操作評級"]),
                    ("KD-K", f'{row["KD_K"]:.1f}'),
                    ("KD-D", f'{row["KD_D"]:.1f}'),
                    ("KDJ-J", f'{_safe_float(row.get("KDJ_J")):.1f}' if _safe_float(row.get("KDJ_J")) is not None else "--"),
                    ("5日乖離率", f'{row["乖離率5日"]:.2f}%'),
                    ("MACD DIF", f'{_safe_float(row.get("MACD_DIF")):.2f}' if _safe_float(row.get("MACD_DIF")) is not None else "--"),
                    ("MACD DEA", f'{_safe_float(row.get("MACD_DEA")):.2f}' if _safe_float(row.get("MACD_DEA")) is not None else "--"),
                    ("MACD柱體", f'{_safe_float(row.get("MACD_BAR")):.2f}' if _safe_float(row.get("MACD_BAR")) is not None else "--"),
                ]
                tech_cols_per_row = 2 if st.session_state.mobile_mode else 3
                for i in range(0, len(tech_pairs), tech_cols_per_row):
                    cols = st.columns(tech_cols_per_row)
                    for col, (label, value) in zip(cols, tech_pairs[i:i+tech_cols_per_row]):
                        col.metric(label, value)
                st.caption(f"KDJ判讀：{kdj_signal(row)}｜MACD判讀：{macd_signal(row)}")
                st.markdown("#### 訊號燈號")
                render_signal_lights(row)

            with tech_right:
                st.markdown("#### 量能觀察")
                volume_pairs = [
                    ("量比(5日)", f'{row["量比5日"]:.2f}'),
                    ("今日成交量(張)", f'{round(float(row["成交量"])/1000):,}'),
                    ("昨日成交量(張)", f'{round(float(row["昨量"])/1000):,}'),
                    ("量能變化", f'{row["量能變化"]} {row["量能變化%"]:+.2f}%'),
                    ("價量結論", row.get("價量結論", "")),
                    ("價量燈號", row.get("價量燈號", "")),
                ]
                vol_cols_per_row = 2
                for i in range(0, len(volume_pairs), vol_cols_per_row):
                    cols = st.columns(vol_cols_per_row)
                    for col, (label, value) in zip(cols, volume_pairs[i:i+vol_cols_per_row]):
                        col.metric(label, value)

            show_chart = st.toggle("載入圖表", value=False, key=f"show_chart_{st.session_state.selected_code}")
            if show_chart:
                df_chart = get_symbol_indicator_chart(st.session_state.selected_code)
                chart_mode = st.radio("圖表模式", ["日K圖", "當日走勢圖"], horizontal=True, key=f"chart_mode_{st.session_state.selected_code}")
                if chart_mode == "日K圖":
                    st.plotly_chart(make_candle_figure(df_chart, row), use_container_width=True)
                else:
                    intra_df = download_intraday(st.session_state.selected_code)
                    st.plotly_chart(make_intraday_figure(intra_df, row), use_container_width=True)
            else:
                st.caption("圖表預設不載入，切換股票會更快；需要時再手動開啟。")

            show_news = st.toggle("載入短線重點資訊 / 最近標題", value=False, key=f"show_news_{st.session_state.selected_code}")
            if show_news:
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
            else:
                st.caption("新聞與事件摘要預設不載入，避免切換單股時額外等待。")

        else:
            with st.container(border=True):
                st.markdown("#### 官方最新日資料")
                if str(row.get("TWSE命中", "")) == "是":
                    st.caption("收盤/收盤差異＝元；成交量＝張；成交值＝億元；成交筆數＝筆。缺值顯示為 --。")
                    ta, tb, tc = st.columns(3)
                    ta.metric("官方日期", fmt_text(row.get("TWSE日期", "")) or "--")
                    tb.metric("官方收盤", fmt_price_with_unit(row.get("TWSE收盤", "")))
                    tc.metric("收盤差異", fmt_price_with_unit(row.get("TWSE收盤差異", "")))
                    td, te, tf = st.columns(3)
                    td.metric("官方成交量", fmt_lots(row.get("TWSE成交量", "")))
                    te.metric("官方成交值", fmt_trade_value_yi(row.get("TWSE成交值", "")))
                    tf.metric("官方成交筆數", fmt_transactions(row.get("TWSE成交筆數", "")))
                    st.caption(f'來源：{fmt_text(row.get("TWSE來源", "")) or "--"}')
                else:
                    st.info("這檔股票目前沒有命中官方日資料補資料。")

                extra_pairs = [
                    ("排名分組", row.get("排名分組", "--")),
                    ("排名原因", row.get("排名原因", "--")),
                    ("選股理由", row.get("選股理由", "--")),
                    ("追價建議", row.get("追價建議", "--")),
                ]
                cols = st.columns(2 if st.session_state.mobile_mode else 4)
                for col, (label, value) in zip(cols, extra_pairs):
                    col.metric(label, value)


if st.session_state.current_page == "首頁":
    render_landing_page()

if st.session_state.current_page != "首頁":
    render_global_banner()

if st.session_state.current_page == "分析中心":
    st.markdown('<div class="main-shell"><h3>📈 分析中心</h3><p>正式版分析工作區：集中處理個股搜尋、自動挑股、補資料命中與單股判讀。V149 直補 TPEx 法人主頁 / OpenAPI 來源，並保留盤前嚴格流動性門檻。</p></div>', unsafe_allow_html=True)
    st.caption(f"目前使用者：{st.session_state.current_user}")
    jump_c1, jump_c2 = st.columns([1.2, 4])
    if jump_c1.button("前往市場儀表板", use_container_width=True):
        st.session_state.current_page = "市場儀表板"
        st.rerun()
    jump_c2.info("整體市場總覽、官方補資料狀態、類股與排行預覽都集中在市場儀表板頁。")
    market_info = market_filter()
    st.session_state["market_info_cache"] = market_info  # 供 topbar 取用

    c_title, c_mkt1, c_mkt2 = st.columns([3, 1.5, 1.5])
    with c_title:
        st.markdown("### 市場概況")
    c_mkt1.metric("大盤濾網", market_info["label"])
    c_mkt2.metric("加權指數", f'{market_info["close"]:.2f}' if market_info["close"] else "N/A")
    if market_info.get("taiex_static_caption"):
        st.markdown(market_info["taiex_static_caption"])
    if market_info.get("taiex_hint"):
        st.caption(market_info["taiex_hint"])
    render_market_taiex_diagnostics(market_info)
    st.info(market_info["text"])

    with st.container(border=True):
        st.markdown("### 操作列")
        op1, op2 = st.columns([3, 1])
        with op1:
            stocks = st.text_input("輸入股票（可直接打四位數）", key="input_value", placeholder="例如：1815, 2330, 2330.TW")
        with op2:
            top_n = st.selectbox("顯示檔數", [5,10,15,20], index=0)

        opm1, opm2 = st.columns(2)
        with opm1:
            _landing_mode = st.session_state.pop("_landing_auto_mode", None)
            _mode_options = ["做多模式", "做空模式"]
            _mode_default = _mode_options.index(_landing_mode) if _landing_mode in _mode_options else 0
            auto_pick_mode = st.selectbox("自動挑股模式", _mode_options, index=_mode_default)
        with opm2:
            candidate_pool_mode = st.selectbox(
                "候選池來源",
                ["核心池（穩定優先）", "擴充池（約250檔，本機較慢）"],
                index=0,
                help="擴充池會對大量股票逐一下載日線；本機網路慢時可能需數分鐘。建議日常先用核心池。",
            )
        st.caption("做多模式：總分≥60，趨勢+動能+量價+籌碼綜合評分；做空模式：總分≥60，同架構反向評分，篩選實戰可操作標的")
        st.caption("V189：自動挑股後備區＋候選檔數同步；單股評分風險列改實際觸發說明、價量理由寫清楚。")
        st.caption("盤前嚴格流動性門檻：20日均量 ≥ 15,000 張、20日均值 ≥ 5 億、20日均振幅 ≥ 3%、近5日低量天數不超過 2 天、量能穩定比 ≥ 0.60。")

        op3, op4 = st.columns(2)
        manual_search = op3.button("搜尋", use_container_width=True)
        auto_pick = op4.button("自動挑股", use_container_width=True)
        _ap_n = st.session_state.get("last_auto_pick_candidate_count")
        layer_meta = st.session_state.get("last_candidate_pool_layer_meta", {}) or {}
        if _ap_n is not None and layer_meta:
            st.caption(f"上次自動挑股候選：**{_ap_n}** 檔｜{layer_meta.get('mode_label', '未記錄')}")
        elif st.session_state.get("last_candidate_pool"):
            source_text = layer_meta.get("mode_label", "未記錄")
            st.caption(f"上次建立候選池：{len(st.session_state.get('last_candidate_pool', []))} 檔｜來源：{source_text}")

    if manual_search:
        tmp = []
        for stock in [s.strip() for s in stocks.split(",") if s.strip()]:
            item = analyze_one_cached(stock, market_info["score_adj"], name_map)
            if item is not None:
                item["策略模式"] = "手動分析"
                tmp.append(item)
        tmp = enrich_with_institutional(tmp)
        tmp = sorted(tmp, key=lambda x: x["_rank"], reverse=True)
        st.session_state.results_data = tmp
        st.session_state.selected_code = tmp[0]["_code"] if tmp else None
        st.session_state.analysis_mode = "manual"
        st.session_state.last_liquidity_summary = {}
        _auto_pick_clear_backup_session()
        st.session_state.last_auto_pick_formal_count = None
        st.session_state.last_auto_pick_backup_count = None

    if auto_pick:
        try:
            _auto_pick_clear_backup_session()
            pick_progress = st.empty()
            def _auto_pick_progress(info):
                ph = info.get("phase", "")
                pr = info.get("pending_retry", 0)
                pick_progress.markdown(
                    f"**自動挑股進度** ｜ 總候選 **{info.get('total_candidates', 0)}** ｜ "
                    f"已處理 **{info.get('completed_fetch', 0)}**（快取命中 {info.get('cache_hits', 0)}、冷卻跳過 {info.get('cooldown_skips', 0)}）｜ "
                    f"暫列待補抓 **{pr}** ｜ 已確定失敗 **{info.get('failures', 0)}** ｜ 批次 **{info.get('batch_index', 0)}** / **{info.get('batch_total', 0)}**"
                    + (f" ｜ *{ph}*" if ph else "")
                )

            indicator_df_store = {}
            with st.spinner("正在建立候選池並執行自動挑股，請稍候..."):
                pool_layers = build_candidate_pool_layers(name_map, max_price=1100.0, expanded_target=250)
                if candidate_pool_mode == "核心池（穩定優先）":
                    active_candidate_pool = pool_layers.get("core_pool", [])
                    mode_label = f"核心池（穩定優先）｜{pool_layers.get('core_count', 0)} 檔"
                else:
                    active_candidate_pool = pool_layers.get("full_pool", [])
                    mode_label = (
                        f"擴充池（慢速）｜核心 {pool_layers.get('core_count', 0)} 檔＋擴充 {pool_layers.get('expansion_count', 0)} 檔＝{pool_layers.get('full_count', 0)} 檔"
                    )

                st.session_state.last_candidate_pool = active_candidate_pool
                st.session_state.last_auto_pick_candidate_count = len(active_candidate_pool)
                st.session_state.last_candidate_pool_layer_meta = {
                    "mode_label": mode_label,
                    "selected_mode": candidate_pool_mode,
                    "core_count": pool_layers.get("core_count", 0),
                    "expansion_count": pool_layers.get("expansion_count", 0),
                    "full_count": pool_layers.get("full_count", 0),
                }
                raw_candidates, engine_debug, indicator_df_store = analyze_candidate_pool_session(
                    tuple(active_candidate_pool),
                    int(market_info["score_adj"]),
                    name_map,
                    progress_hook=_auto_pick_progress,
                )

            pick_progress.markdown(
                f"**自動挑股完成** ｜ 候選 **{engine_debug.get('candidate_count', 0)}** ｜ "
                f"初評列管 **{engine_debug.get('result_count', 0)}** ｜ "
                f"快取命中 **{engine_debug.get('cache_hits', 0)}** ｜ "
                f"本輪新成功 **{engine_debug.get('new_success', 0)}** ｜ "
                f"失敗 **{engine_debug.get('new_failures', 0)}** ｜ 補抓救回 **{engine_debug.get('retry_recovered', 0)}** ｜ "
                f"技術資料重用 **{engine_debug.get('indicator_df_cached', 0)}** 檔"
            )
            st.caption(
                "說明：「本輪新成功／失敗」只計這次實際重新下載分析的檔數；若本輪全部來自快取命中，兩者可能皆為 0，並不代表沒有資料。"
            )

            liquidity_passed = [x for x in raw_candidates if liquidity_pass(x)]
            liquidity_filtered = [x for x in raw_candidates if not liquidity_pass(x)]
            st.session_state.last_liquidity_summary = {
                "raw_count": len(raw_candidates),
                "passed_count": len(liquidity_passed),
                "filtered_count": len(liquidity_filtered),
                "top_reasons": [f"{x.get('股票','')}｜{liquidity_penalty_reason(x)}" for x in liquidity_filtered[:10]],
                "missing_liq_count": sum(1 for x in raw_candidates if "流動性達標" not in x),
                "debug_rows": [
                    {
                        "股票": x.get("股票", x.get("_code", "")),
                        "流動性達標": x.get("流動性達標", "<缺漏>"),
                        "流動性結論": x.get("流動性結論", "<缺漏>"),
                        "流動性摘要": x.get("流動性摘要", "<缺漏>"),
                        "流動性排除原因": x.get("流動性排除原因", "<缺漏>"),
                    }
                    for x in raw_candidates[:20]
                ],
            }
            source_candidates = liquidity_passed

            # ── 補三大法人資料（在評分之前）──────────────────────────────────
            # short_pick_compute / long_pick_compute 的「籌碼分」直接讀
            # item["法人方向"] / item["法人共振"]，必須在評分前補好
            source_candidates = enrich_with_institutional(source_candidates)
            # ─────────────────────────────────────────────────────────────────

            engine_debug = dict(engine_debug)
            engine_debug["liquidity_passed_count"] = len(liquidity_passed)
            engine_debug["liquidity_raw_count"] = len(raw_candidates)
            st.session_state.last_candidate_engine_summary = engine_debug
            qc_blocked, qc_msg = _auto_pick_quality_blocked(engine_debug)
            st.session_state.last_auto_pick_quality_blocked = qc_blocked
            st.session_state.last_auto_pick_quality_message = qc_msg if qc_blocked else ""

            with st.expander("自動挑股本輪資料面板（點標題可收合）", expanded=False):
                with st.container(border=True):
                    render_auto_pick_engine_panel(engine_debug)

            if qc_blocked:
                st.warning(qc_msg)
                with st.expander("本輪失敗與冷卻明細", expanded=False):
                    cooldown_rows = engine_debug.get("cooldown_rows", [])
                    failure_rows = engine_debug.get("failure_rows", [])
                    if cooldown_rows:
                        st.caption("冷卻跳過")
                        st.dataframe(pd.DataFrame(cooldown_rows), use_container_width=True, hide_index=True)
                    if failure_rows:
                        st.caption("本輪失敗（含補抓後仍失敗）")
                        st.dataframe(pd.DataFrame(failure_rows), use_container_width=True, hide_index=True)
                st.session_state.results_data = []
                st.session_state.selected_code = None
                st.session_state.analysis_mode = "auto"
                st.session_state.selected_side = "short" if auto_pick_mode == "做空模式" else "long"
                _auto_pick_clear_backup_session()
                st.session_state.last_auto_pick_formal_count = 0
                st.session_state.last_auto_pick_backup_count = 0

            elif not source_candidates:
                st.warning("本次候選池全部被盤前流動性門檻排除，請放寬條件或改用手動搜尋。")
                liq_summary = st.session_state.get("last_liquidity_summary", {}) or {}
                with st.expander("查看盤前流動性排除偵錯", expanded=False):
                    dm1, dm2, dm3 = st.columns(3)
                    dm1.metric("候選池分析數", liq_summary.get("raw_count", 0))
                    dm2.metric("通過流動性門檻", liq_summary.get("passed_count", 0))
                    dm3.metric("缺漏流動性欄位", liq_summary.get("missing_liq_count", 0))
                    reasons = liq_summary.get("top_reasons", [])
                    if reasons:
                        st.caption("前幾個排除原因：")
                        for r in reasons:
                            st.write(f"- {r}")
                    debug_rows = liq_summary.get("debug_rows", [])
                    if debug_rows:
                        st.dataframe(pd.DataFrame(debug_rows), use_container_width=True, hide_index=True)

                engine_summary = st.session_state.get("last_candidate_engine_summary", {}) or {}
                with st.expander("查看候選池引擎偵錯摘要", expanded=False):
                    render_auto_pick_engine_panel(engine_summary)
                    cooldown_rows = engine_summary.get("cooldown_rows", [])
                    failure_rows = engine_summary.get("failure_rows", [])
                    if cooldown_rows:
                        st.caption("冷卻跳過：")
                        st.dataframe(pd.DataFrame(cooldown_rows), use_container_width=True, hide_index=True)
                    if failure_rows:
                        st.caption("本輪失敗：")
                        st.dataframe(pd.DataFrame(failure_rows), use_container_width=True, hide_index=True)
                st.session_state.results_data = []
                st.session_state.selected_code = None
                st.session_state.analysis_mode = "auto"
                st.session_state.selected_side = "short" if auto_pick_mode == "做空模式" else "long"
                _auto_pick_clear_backup_session()
                st.session_state.last_auto_pick_formal_count = 0
                st.session_state.last_auto_pick_backup_count = 0

            else:
                if auto_pick_mode == "做空模式":
                    candidates = []
                    for item in source_candidates:
                        code = item.get("_code", item.get("股票代碼", ""))
                        df_chart = _indicator_df_for_auto_pick(item, indicator_df_store)
                        if df_chart is None or df_chart.empty:
                            try:
                                df_chart = get_symbol_indicator_chart(code)
                            except Exception:
                                df_chart = None
                        metrics = short_pick_compute(df_chart, item) if df_chart is not None and not df_chart.empty else None
                        if metrics is None:
                            continue
                        if metrics["short_total"] < 60:
                            continue
                        item = item.copy()
                        item["策略模式"] = "做空模式"
                        item["_auto_mode_score"] = metrics["short_total"]
                        item["做空總分"] = metrics["short_total"]
                        item["趨勢分"] = metrics["short_trend"]
                        item["動能分"] = metrics["short_momentum"]
                        item["量價分"] = metrics["short_volume"]
                        item["籌碼分"] = metrics["short_chip"]
                        item["風險扣分"] = metrics["short_risk"]
                        item["入選原因"] = metrics["short_reason"]
                        item["風險警示"] = metrics["short_warning"]
                        item["5日漲幅%"] = metrics["short_chg5d"]
                        item["量比20日"] = metrics["short_vol_ratio"]
                        candidates.append(item)
                    candidates = sorted(candidates, key=lambda x: x.get("_auto_mode_score", 0), reverse=True)
                else:
                    candidates = []
                    for item in source_candidates:
                        code = item.get("_code", item.get("股票代碼", ""))
                        df_chart = _indicator_df_for_auto_pick(item, indicator_df_store)
                        if df_chart is None or df_chart.empty:
                            try:
                                df_chart = get_symbol_indicator_chart(code)
                            except Exception:
                                df_chart = None
                        metrics = long_pick_compute(df_chart, item) if df_chart is not None and not df_chart.empty else None
                        if metrics is None:
                            continue
                        if metrics["long_total"] < 60:
                            continue
                        item = item.copy()
                        item["策略模式"] = "做多模式"
                        item["_auto_mode_score"] = metrics["long_total"]
                        item["做多總分"] = metrics["long_total"]
                        item["趨勢分"] = metrics["long_trend"]
                        item["動能分"] = metrics["long_momentum"]
                        item["量價分"] = metrics["long_volume"]
                        item["籌碼分"] = metrics["long_chip"]
                        item["風險扣分"] = metrics["long_risk"]
                        item["入選原因"] = metrics["long_reason"]
                        item["風險警示"] = metrics["long_warning"]
                        item["5日漲幅%"] = metrics["long_chg5d"]
                        item["量比20日"] = metrics["long_vol_ratio"]
                        candidates.append(item)
                    candidates = sorted(candidates, key=lambda x: x.get("_auto_mode_score", 0), reverse=True)

                if not candidates and source_candidates:
                    fb = [
                        dict(x)
                        for x in sorted(
                            source_candidates,
                            key=lambda x: float(x.get("_rank") or 0),
                            reverse=True,
                        )[:top_n]
                    ]
                    if auto_pick_mode == "做空模式":
                        for r in fb:
                            r["策略模式"] = "後備觀察（非做空推薦）"
                        st.session_state.auto_pick_backup_rows = fb
                        st.session_state.auto_pick_backup_mode = "short"
                    else:
                        for r in fb:
                            r["策略模式"] = "後備觀察（非做多推薦）"
                        st.session_state.auto_pick_backup_rows = fb
                        st.session_state.auto_pick_backup_mode = "long"
                    st.session_state.auto_pick_backup_detail_code = None
                    st.session_state.results_data = []
                    st.session_state.selected_code = None
                    st.session_state.last_auto_pick_formal_count = 0
                    st.session_state.last_auto_pick_backup_count = len(fb)
                else:
                    final_candidates = candidates[:top_n]
                    st.session_state.results_data = final_candidates
                    st.session_state.selected_code = st.session_state.results_data[0]["_code"] if st.session_state.results_data else None
                    st.session_state.last_auto_pick_formal_count = len(st.session_state.results_data)
                    st.session_state.last_auto_pick_backup_count = 0
                st.session_state.analysis_mode = "auto"
                st.session_state.selected_side = "short" if auto_pick_mode == "做空模式" else "long"
        except Exception as e:
            st.error(f"自動挑股發生錯誤：{e}")

    results = st.session_state.results_data
    backup_rows = list(st.session_state.get("auto_pick_backup_rows") or [])
    backup_detail_code = st.session_state.get("auto_pick_backup_detail_code")
    has_auto_backup = st.session_state.analysis_mode == "auto" and bool(backup_rows)

    twse_meta = load_cached_official_meta() if (st.session_state.results_data or backup_rows) else {}
    twse_hit_count = sum(1 for r in (st.session_state.results_data or []) if str(dict(r).get("TWSE命中", "")) == "是")

    if results:
        if st.session_state.analysis_mode == "manual":
            st.caption("目前模式：手動搜尋｜單股搜尋走獨立路徑，不會重跑 250 檔候選池；若該檔已在候選池分析過，會直接重用逐股快取。")
        elif st.session_state.analysis_mode == "auto":
            layer_meta = st.session_state.get("last_candidate_pool_layer_meta", {}) or {}
            source_text = layer_meta.get("mode_label", "未記錄")
            st.caption(f"目前模式：自動挑股（{auto_pick_mode}）｜候選池來源：{source_text}")
            if st.session_state.get("last_auto_pick_formal_count") is not None:
                stat1, stat2 = st.columns(2)
                stat1.metric("正式推薦股數", int(st.session_state.last_auto_pick_formal_count))
                stat2.metric("後備觀察股數", int(st.session_state.last_auto_pick_backup_count or 0))
        elif st.session_state.analysis_mode == "favorites":
            st.caption("目前模式：最愛分析")

        twm1, twm2, twm3 = st.columns(3)
        twm1.metric("官方命中數", twse_hit_count)
        twm2.metric("官方最新日期", str((twse_meta or {}).get("latest_date", "")))
        twm3.metric("官方來源狀態", str((twse_meta or {}).get("ssl_mode", "")))
        if (twse_meta or {}).get("hybrid_error"):
            st.caption(f'官方補資料已自動降級，不影響主分析：{(twse_meta or {}).get("hybrid_error")}')
        st.caption("分析中心已改成速度優先：這裡只顯示已快取的官方資料狀態，不再自動逐檔補資料；需要時請到市場儀表板或官方資料區查看。")

        df_result = dataframe_from_analysis_records(results)

        favs = st.session_state.favorites
        if favs:
            with st.expander("我的最愛追蹤面板", expanded=False):
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

        result_mode_series = df_result["策略模式"].astype(str) if "策略模式" in df_result.columns else pd.Series([], dtype=str)
        board_mode = "空方" if result_mode_series.str.contains("做空").any() else "多方"
        is_short_board = board_mode == "空方"
        df_result = ensure_dataframe_columns(df_result, ["空方風報比", "_bearish_hits", "空方建議進場", "空方停損", "空方短期壓力", "空方短期支撐", "空方中繼目標", "空方跌破目標"])

        with st.expander("策略與篩選", expanded=not st.session_state.mobile_mode):
            st.markdown(f"### {board_mode}策略與篩選")
            st.caption("這一區已改成多空雙模式面板：做多看強勢延續，做空看弱勢結構、跌破與反彈不過壓力。常用條件放上面，進階條件收合避免版面太擠。")
            preset = st.session_state.preset_strategy
            filtered_df = df_result.copy()
            if is_short_board and "_auto_mode_score" not in filtered_df.columns:
                n = len(filtered_df)
                idx = filtered_df.index
                if n > 0:
                    if "做空總分" in filtered_df.columns:
                        s_short = pd.to_numeric(filtered_df["做空總分"], errors="coerce")
                    else:
                        s_short = pd.Series([float("nan")] * n, index=idx)
                    if "_rank" in filtered_df.columns:
                        s_rank = pd.to_numeric(filtered_df["_rank"], errors="coerce")
                    else:
                        s_rank = pd.Series(0.0, index=idx)
                    filtered_df["_auto_mode_score"] = s_short.fillna(s_rank).fillna(0)

            if is_short_board:
                ps1, ps2, ps3 = st.columns(3)
                if ps1.button("偏空股", use_container_width=True):
                    st.session_state.preset_strategy = "bearish"
                if ps2.button("放量轉弱", use_container_width=True):
                    st.session_state.preset_strategy = "weak_price_volume"
                if ps3.button("高空方風報比", use_container_width=True):
                    st.session_state.preset_strategy = "high_short_rr"
                ps4, ps5, ps6, ps7 = st.columns(4)
                if ps4.button("跌破觀察", use_container_width=True):
                    st.session_state.preset_strategy = "breakdown_watch"
                if ps5.button("反彈空", use_container_width=True):
                    st.session_state.preset_strategy = "rebound_short"
                if ps6.button("排除綠燈", use_container_width=True):
                    st.session_state.preset_strategy = "hide_green_only"
                if ps7.button("取消策略", use_container_width=True):
                    st.session_state.preset_strategy = "none"

                _short_conclusion_opts = ["全部", "看空", "中性偏空", "中性"]
                _short_signal_opts = ["全部", "🔥進場", "👀觀察", "⏳等待", "❌不進"]
                _short_weak_opts = ["全部", "價跌量增", "開高走低爆量", "跳空下跌", "量縮", "量比>=1"]
                _short_rr_opts = ["全部", ">=1.0", ">=1.2", ">=1.5", ">=2.0"]
                _short_hits_opts = ["全部", ">=2", ">=3"]

                _pre_conclusion = "全部"; _pre_signal = "全部"; _pre_weak = "全部"
                _pre_rr = "全部"; _pre_hits = "全部"; _pre_hide_green = False
                if preset == "bearish":
                    _pre_conclusion = "看空"; _pre_rr = ">=1.2"; _pre_hide_green = True
                elif preset == "weak_price_volume":
                    _pre_weak = "價跌量增"; _pre_rr = ">=1.0"
                elif preset == "high_short_rr":
                    _pre_rr = ">=1.5"; _pre_hits = ">=2"
                elif preset == "breakdown_watch":
                    _pre_weak = "跳空下跌"; _pre_hits = ">=2"
                elif preset == "rebound_short":
                    _pre_conclusion = "中性偏空"; _pre_rr = ">=1.2"
                elif preset == "hide_green_only":
                    _pre_hide_green = True

                with st.expander("進階條件", expanded=False):
                    fl1, fl2 = st.columns(2)
                    conclusion_filter = fl1.selectbox("空方傾向", _short_conclusion_opts, index=_short_conclusion_opts.index(_pre_conclusion))
                    signal_filter = fl2.selectbox("交易訊號", _short_signal_opts, index=_short_signal_opts.index(_pre_signal))
                    fl3, fl4, fl5 = st.columns(3)
                    weak_filter = fl3.selectbox("弱勢型態", _short_weak_opts, index=_short_weak_opts.index(_pre_weak))
                    min_short_rr = fl4.selectbox("最低空方風報比", _short_rr_opts, index=_short_rr_opts.index(_pre_rr))
                    hits_filter = fl5.selectbox("最低弱勢訊號數", _short_hits_opts, index=_short_hits_opts.index(_pre_hits))
                    hide_green = st.checkbox("隱藏綠燈 / 偏多股", value=_pre_hide_green)

                if conclusion_filter != "全部":
                    filtered_df = filtered_df[filtered_df["結論"] == conclusion_filter]
                if signal_filter != "全部":
                    filtered_df = filtered_df[filtered_df["交易訊號"] == signal_filter]
                if weak_filter in ["價跌量增", "開高走低爆量", "跳空下跌", "量縮"]:
                    if weak_filter == "量縮":
                        filtered_df = filtered_df[filtered_df["量能變化"] == "量縮"]
                    else:
                        filtered_df = filtered_df[filtered_df["價量結論"] == weak_filter]
                elif weak_filter == "量比>=1":
                    filtered_df = filtered_df[pd.to_numeric(filtered_df["量比5日"], errors="coerce") >= 1]
                if min_short_rr != "全部":
                    rr_value = float(min_short_rr.replace(">=", ""))
                    filtered_df = filtered_df[pd.to_numeric(filtered_df["空方風報比"], errors="coerce") >= rr_value]
                if hits_filter != "全部":
                    hit_value = int(hits_filter.replace(">=", ""))
                    filtered_df = filtered_df[pd.to_numeric(filtered_df["_bearish_hits"], errors="coerce").fillna(0) >= hit_value]
                if hide_green:
                    filtered_df = filtered_df[(filtered_df["價量燈號"] != "綠燈") & (~filtered_df["結論"].isin(["看多", "中性偏多"]))]

                preset_name_map = {
                    "none": "自訂",
                    "bearish": "偏空股",
                    "weak_price_volume": "放量轉弱",
                    "high_short_rr": "高空方風報比",
                    "breakdown_watch": "跌破觀察",
                    "rebound_short": "反彈空",
                    "hide_green_only": "排除綠燈",
                }
            else:
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

                _long_trend_opts = ["全部", "綠燈", "黃燈", "紅燈"]
                _long_signal_opts = ["全部", "🔥進場", "👀觀察", "⏳等待", "❌不進"]
                _long_vol_opts = ["全部", "量增", "量縮", "量比>=1"]
                _long_rr_opts = ["全部", ">=1.0", ">=1.2", ">=1.5", ">=2.0"]

                _pre_trend = "全部"; _pre_signal = "全部"; _pre_vol = "全部"
                _pre_rr = "全部"; _pre_hide_red = False
                if preset == "bullish":
                    _pre_trend = "綠燈"; _pre_rr = ">=1.2"; _pre_hide_red = True
                elif preset == "volume_up":
                    _pre_vol = "量增"; _pre_rr = ">=1.0"
                elif preset == "high_rr":
                    _pre_rr = ">=1.5"; _pre_hide_red = True
                elif preset == "breakout_watch":
                    _pre_signal = "⏳等待"; _pre_rr = ">=1.0"
                elif preset == "low_risk":
                    _pre_trend = "綠燈"; _pre_rr = ">=1.2"; _pre_vol = "量比>=1"; _pre_hide_red = True
                elif preset == "hide_red_only":
                    _pre_hide_red = True

                with st.expander("進階條件", expanded=False):
                    fl1, fl2 = st.columns(2)
                    trend_filter = fl1.selectbox("趨勢燈號", _long_trend_opts, index=_long_trend_opts.index(_pre_trend))
                    signal_filter = fl2.selectbox("交易訊號", _long_signal_opts, index=_long_signal_opts.index(_pre_signal))
                    fl3, fl4, fl5 = st.columns(3)
                    volume_filter = fl3.selectbox("量能條件", _long_vol_opts, index=_long_vol_opts.index(_pre_vol))
                    min_rr = fl4.selectbox("最低風報比", _long_rr_opts, index=_long_rr_opts.index(_pre_rr))
                    hide_red = fl5.checkbox("隱藏紅燈 / 不進", value=_pre_hide_red)

                if trend_filter != "全部":
                    filtered_df = filtered_df[filtered_df["趨勢燈號"] == trend_filter]
                if signal_filter != "全部":
                    filtered_df = filtered_df[filtered_df["交易訊號"] == signal_filter]
                if volume_filter == "量增":
                    filtered_df = filtered_df[filtered_df["量能變化"] == "量增"]
                elif volume_filter == "量縮":
                    filtered_df = filtered_df[filtered_df["量能變化"] == "量縮"]
                elif volume_filter == "量比>=1":
                    filtered_df = filtered_df[pd.to_numeric(filtered_df["量比5日"], errors="coerce") >= 1]
                if min_rr != "全部":
                    rr_value = float(min_rr.replace(">=", ""))
                    filtered_df = filtered_df[pd.to_numeric(filtered_df["風報比"], errors="coerce") >= rr_value]
                if hide_red:
                    filtered_df = filtered_df[(filtered_df["進場燈號"] != "紅燈") & (filtered_df["交易訊號"] != "❌不進")]

                preset_name_map = {
                    "none": "自訂",
                    "bullish": "偏多股",
                    "volume_up": "量增股",
                    "high_rr": "高風報比",
                    "breakout_watch": "突破觀察",
                    "low_risk": "低風險觀察",
                    "hide_red_only": "排除紅燈",
                }

            st.caption(f"目前預設策略：{preset_name_map.get(preset, '自訂')}｜面板模式：{board_mode}")

        with st.expander("盤前候選股面板", expanded=not st.session_state.mobile_mode):
            panel_df = filtered_df.copy()
            st.markdown(f"### 盤前候選股面板｜{board_mode}")
            layer_meta = st.session_state.get("last_candidate_pool_layer_meta", {}) or {}
            if layer_meta:
                st.caption(f"候選池來源：{layer_meta.get('mode_label', '未記錄')}")
            if is_short_board:
                bearish_count = int(panel_df["結論"].isin(["看空", "中性偏空"]).sum()) if not panel_df.empty else 0
                weak_count = int(panel_df["價量結論"].isin(["價跌量增", "開高走低爆量", "跳空下跌"]).sum()) if not panel_df.empty else 0
                high_rr_count = int((pd.to_numeric(panel_df["空方風報比"], errors="coerce") >= 1.5).sum()) if not panel_df.empty else 0
                actionable_count = int((pd.to_numeric(panel_df["_bearish_hits"], errors="coerce").fillna(0) >= 2).sum()) if not panel_df.empty else 0
                avg_rr = round(pd.to_numeric(panel_df["空方風報比"], errors="coerce").mean(), 2) if not panel_df.empty else 0.0
                pc1, pc2, pc3, pc4 = st.columns(4)
                pc1.metric("篩選後檔數", len(panel_df))
                pc2.metric("偏空候選", bearish_count)
                pc3.metric("弱勢價量", weak_count)
                pc4.metric("反彈空 / 跌破觀察", actionable_count)
                st.caption(f"篩選名單平均空方風報比：{avg_rr}｜高空方風報比檔數：{high_rr_count}")
            else:
                green_count = int((panel_df["趨勢燈號"] == "綠燈").sum()) if not panel_df.empty else 0
                volume_up_count = int((panel_df["量能變化"] == "量增").sum()) if not panel_df.empty else 0
                actionable_count = int(panel_df["交易訊號"].isin(["🔥進場", "👀觀察"]).sum()) if not panel_df.empty else 0
                avg_rr = round(pd.to_numeric(panel_df["風報比"], errors="coerce").mean(), 2) if not panel_df.empty else 0.0
                pc1, pc2, pc3, pc4 = st.columns(4)
                pc1.metric("篩選後檔數", len(panel_df))
                pc2.metric("綠燈數", green_count)
                pc3.metric("量增股數", volume_up_count)
                pc4.metric("可優先觀察", actionable_count)
                st.caption(f"篩選名單平均風報比：{avg_rr}")

            if not panel_df.empty:
                if is_short_board:
                    top_cards = panel_df.sort_values(["空方風報比", "_auto_mode_score", "量比5日"], ascending=[False, False, False]).head(3)
                else:
                    top_cards = panel_df.sort_values(["風報比", "量比5日"], ascending=[False, False]).head(3)
                st.markdown("#### TOP 候選")
                top_cols = st.columns(1 if st.session_state.mobile_mode else 3)
                if st.session_state.mobile_mode:
                    top_cols = [st.container(), st.container(), st.container()]
                for col, (_, r) in zip(top_cols, top_cards.iterrows()):
                    with col:
                        top_line_2 = f'{r["族群"]}｜{r["結論"]}｜{r["交易訊號"]}'
                        if is_short_board:
                            top_line_3 = f'空方風報比：{fmt_price(r.get("空方風報比"))}｜弱勢訊號：{int(r.get("_bearish_hits", 0) or 0)}'
                            top_line_4 = f'空方壓力：{fmt_price(r.get("空方短期壓力"))}｜空方支撐：{fmt_price(r.get("空方短期支撐"))}'
                        else:
                            top_line_3 = f'風報比：{r["風報比"]}｜量比：{r["量比5日"]}'
                            top_line_4 = f'支撐：{r["支撐"]}｜短壓：{r["短期壓力"]}'
                        st.markdown(
                            f"""
                            <div style="border:1px solid #334155;border-radius:12px;padding:12px;background:#0b1220;margin-bottom:8px;min-height:152px;">
                                <div style="font-size:18px;font-weight:700;color:white;margin-bottom:8px;">{r["股票"]}</div>
                                <div style="color:#cbd5e1;font-size:13px;margin-bottom:6px;">{top_line_2}</div>
                                <div style="color:#cbd5e1;font-size:13px;">{top_line_3}</div>
                                <div style="color:#cbd5e1;font-size:13px;">{top_line_4}</div>
                                <div style="color:#93c5fd;font-size:13px;margin-top:6px;">{r["排名原因"]}</div>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )

        with st.expander("總表分析", expanded=not st.session_state.mobile_mode):
            st.markdown(f"### 總表分析｜{board_mode}")
            sort_c1, sort_c2 = st.columns([2, 3])
            if is_short_board:
                sort_options = {
                    "預設排名": ("_auto_mode_score", False),
                    "空方風報比（高→低）": ("空方風報比", False),
                    "弱勢訊號數（高→低）": ("_bearish_hits", False),
                    "量比（高→低）": ("量比5日", False),
                    "收盤價（高→低）": ("收盤", False),
                    "收盤價（低→高）": ("收盤", True),
                }
            else:
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
            layer_meta = st.session_state.get("last_candidate_pool_layer_meta", {}) or {}
            pool_caption = f"｜候選池：{layer_meta.get('selected_mode', '手動/未記錄')}" if layer_meta else ""
            sort_c2.caption(f"目前排序：{sort_label}｜模式：{board_mode}{pool_caption}")

            has_new_score = ("做多總分" in sorted_df.columns and sorted_df["做多總分"].notna().any()) or \
                           ("做空總分" in sorted_df.columns and sorted_df["做空總分"].notna().any())
            score_col_name = "做空總分" if is_short_board else "做多總分"
            if has_new_score:
                if st.session_state.mobile_mode:
                    core_cols = ["股票", "收盤", score_col_name, "趨勢分", "動能分", "量價分", "入選原因", "風險警示"]
                else:
                    core_cols = ["股票", "收盤", score_col_name, "趨勢分", "動能分", "量價分", "籌碼分", "風險扣分", "5日漲幅%", "量比20日", "入選原因", "風險警示"]
                detail_cols = ["進場", "停損", "短期壓力", "風報比", "結論", "交易訊號", "RSI", "KD_K", "量能變化%", "量比5日"]
            elif is_short_board:
                core_cols = ["股票", "收盤", "空方建議進場", "空方停損", "空方短期壓力", "空方中繼目標", "空方風報比", "結論", "交易訊號"]
                detail_cols = ["空方短期支撐", "空方跌破目標", "_bearish_hits", "價量結論", "價量燈號", "量能變化", "量能變化%", "量比5日", "排名分組", "排名原因", "TWSE日期", "TWSE名稱", "TWSE成交量", "TWSE成交值"]
            else:
                core_cols = get_analysis_core_columns(st.session_state.mobile_mode)
                detail_cols = ["支撐", "排名分組", "排名原因", "RSI", "KD_K", "KD_D", "KDJ_J", "MACD_DIF", "MACD_DEA", "MACD_BAR", "乖離率5日", "量能變化", "量能變化%", "量比5日", "TWSE日期", "TWSE名稱", "TWSE成交量", "TWSE成交值", "TWSE漲跌價差", "TWSE成交筆數", "TWSE收盤差異"]
            sorted_df = ensure_dataframe_columns(sorted_df, list(dict.fromkeys(core_cols + detail_cols)))
            st.dataframe(sorted_df[core_cols], use_container_width=True, hide_index=True)

            with st.expander("展開詳細欄位 / 排名系統", expanded=False):
                detail_df = sorted_df[list(dict.fromkeys(core_cols + detail_cols))]
                st.dataframe(detail_df, use_container_width=True, hide_index=True)
                rank_df = sorted_df.copy()
                rank_df.insert(0, "排名", range(1, len(rank_df) + 1))
                if is_short_board:
                    rank_cols = ["排名", "股票", "排名分組", "排名原因", "結論", "交易訊號", "空方風報比", "_bearish_hits", "價量結論", "量比5日"]
                else:
                    rank_cols = ["排名", "股票", "排名分組", "排名原因", "星級", "操作評級", "結論", "交易訊號", "風報比", "RSI", "KD_K", "KD_D", "KDJ_J", "MACD_BAR"]
                st.dataframe(rank_df[rank_cols], use_container_width=True, hide_index=True)

        select_source = sorted_df if ("sorted_df" in locals() and not sorted_df.empty) else (filtered_df if not filtered_df.empty else df_result)
        render_single_stock_detail_panel(select_source, df_result, name_map, market_info)

    elif has_auto_backup:
        layer_meta = st.session_state.get("last_candidate_pool_layer_meta", {}) or {}
        source_text = layer_meta.get("mode_label", "未記錄")
        st.caption(f"目前模式：自動挑股（{auto_pick_mode}）｜候選池來源：{source_text}")
        if st.session_state.get("last_auto_pick_formal_count") is not None:
            s1, s2 = st.columns(2)
            s1.metric("正式推薦股數", 0)
            s2.metric("後備觀察股數", int(st.session_state.last_auto_pick_backup_count or len(backup_rows)))
        bm = st.session_state.get("auto_pick_backup_mode")
        if bm == "short":
            st.error("本輪沒有符合做空條件的正式標的（做空總分≥60）。")
            st.info("本輪做空模式沒有正式推薦股。若要查看後備觀察股請於下方展開「後備觀察股（非推薦）」並手動選股載入單股詳細分析；後備名單僅為內建排名，**不代表做空建議**。")
        else:
            st.error("本輪沒有符合做多條件的正式標的（做多總分≥60）。")
            st.info("本輪做多模式沒有正式推薦股。後備名單僅供觀察，請手動選股後再載入詳細分析。")
        b_hit = sum(1 for r in backup_rows if str(dict(r).get("TWSE命中", "")) == "是")
        twb1, twb2, twb3 = st.columns(3)
        twb1.metric("後備列官方命中數", b_hit)
        twb2.metric("官方最新日期", str((twse_meta or {}).get("latest_date", "")))
        twb3.metric("官方來源狀態", str((twse_meta or {}).get("ssl_mode", "")))
        with st.expander("後備觀察股（非推薦）", expanded=False):
            bdf = pd.DataFrame(backup_rows)
            pref_cols = ["股票", "_code", "結論", "交易訊號", "收盤", "_rank", "排名分組", "排名原因"]
            show_cols = [c for c in pref_cols if c in bdf.columns]
            st.dataframe(bdf[show_cols] if show_cols else bdf, use_container_width=True, hide_index=True)
            pick_options = [("（請選擇）", None)]
            for r in backup_rows:
                lab = str(r.get("股票", r.get("_code", "")) or "").strip() or str(r.get("_code", ""))
                cod = r.get("_code")
                if cod:
                    pick_options.append((lab, cod))
            labels_only = [p[0] for p in pick_options]
            pick_label = st.selectbox("選擇標的後按下方按鈕載入詳細分析（非推薦）", labels_only, key="auto_pick_backup_pick_label")
            if st.button("載入至單股詳細分析", key="auto_pick_backup_load_detail_btn"):
                code_pick = None
                for lab, cod in pick_options:
                    if lab == pick_label:
                        code_pick = cod
                        break
                if code_pick:
                    st.session_state.auto_pick_backup_detail_code = code_pick
                    st.rerun()

    else:
        st.info("請先輸入股票後按『搜尋』，或使用『自動挑股』。")

    if backup_detail_code and backup_rows:
        row = next(
            (r for r in backup_rows if str(r.get("_code", "")).strip().upper() == str(backup_detail_code).strip().upper()),
            None,
        )
        if row:
            st.divider()
            hb1, hb2 = st.columns([4, 1])
            with hb1:
                st.caption("以下為**後備觀察股**之單股詳細檢視，**非**正式做空／做多推薦名單。")
            with hb2:
                if st.button("關閉檢視", key="auto_pick_backup_close_detail_btn"):
                    st.session_state.auto_pick_backup_detail_code = None
                    st.rerun()
            df_one = dataframe_from_analysis_records([dict(row)])
            bm = st.session_state.get("auto_pick_backup_mode")
            if bm == "short":
                df_one = ensure_dataframe_columns(
                    df_one,
                    ["空方風報比", "_bearish_hits", "空方建議進場", "空方停損", "空方短期壓力", "空方短期支撐", "空方中繼目標", "空方跌破目標"],
                )
            render_single_stock_detail_panel(df_one, df_one, name_map, market_info)

if st.session_state.current_page == "市場儀表板":
    st.markdown('<div class="main-shell"><h3>📊 市場儀表板</h3><p>正式版市場總覽頁：集中顯示大盤快照、官方補資料狀態與分析結果聚合模組，視覺也開始往全站一致化靠攏。</p></div>', unsafe_allow_html=True)
    st.caption(f"目前使用者：{st.session_state.current_user}")
    market_info = market_filter()
    st.session_state["market_info_cache"] = market_info

    if "dashboard_last_update" not in st.session_state:
        st.session_state.dashboard_last_update = None

    st.markdown("### 市場概況")

    action_c1, action_c2, action_c3 = st.columns([1.2, 1.2, 3])
    if action_c1.button("重新整理儀表板", use_container_width=True):
        try:
            st.cache_data.clear()
        except Exception:
            pass
        st.session_state.dashboard_last_update = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.rerun()
    if action_c2.button("回分析中心", use_container_width=True):
        st.session_state.current_page = "分析中心"
        st.rerun()
    if st.session_state.dashboard_last_update:
        action_c3.caption(f"最後更新時間：{st.session_state.dashboard_last_update}")
    else:
        action_c3.caption("需要更新時可直接按「重新整理儀表板」。")

    mc1, mc2 = st.columns(2)
    mc1.metric("大盤濾網", market_info["label"])
    mc2.metric("加權指數", f'{market_info["close"]:.2f}' if market_info["close"] else "N/A")
    if market_info.get("taiex_static_caption"):
        st.markdown(market_info["taiex_static_caption"])
    if market_info.get("taiex_hint"):
        st.caption(market_info["taiex_hint"])
    render_market_taiex_diagnostics(market_info)
    st.info(market_info["text"])

    with st.container(border=True):
        st.markdown("### 市場快照（穩定版）")
        df_mkt = get_market_data()
        close_val = market_info.get("close")
        prev_close = None
        day_change = None
        day_pct = None
        ma5 = None
        ma20 = None
        vol_last = None
        if df_mkt is not None and not df_mkt.empty:
            try:
                ad = market_info.get("taiex_anchor_date")
                res_close = market_info.get("close")
                if ad is not None and res_close is not None:
                    df_mkt = _ensure_twii_bar_through_anchor(df_mkt, ad, float(res_close))
                df_mkt2 = indicators(df_mkt.copy())
                if len(df_mkt2) >= 2:
                    prev_close = float(df_mkt2["Close"].iloc[-2])
                    close_val = float(df_mkt2["Close"].iloc[-1])
                    day_change = close_val - prev_close
                    day_pct = (day_change / prev_close * 100) if prev_close else None
                    ma5 = float(df_mkt2["ma5"].iloc[-1]) if pd.notna(df_mkt2["ma5"].iloc[-1]) else None
                    ma20 = float(df_mkt2["ma20"].iloc[-1]) if pd.notna(df_mkt2["ma20"].iloc[-1]) else None
                    vol_last = float(df_mkt2["Volume"].iloc[-1]) if pd.notna(df_mkt2["Volume"].iloc[-1]) else None
            except Exception:
                pass

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("加權指數", f"{close_val:.2f}" if close_val is not None else "--", f"{day_change:+.2f}" if day_change is not None else None)
        k2.metric("單日漲跌幅", f"{day_pct:+.2f}%" if day_pct is not None else "--")
        k3.metric("5日均線", f"{ma5:.2f}" if ma5 is not None else "--")
        k4.metric("20日均線", f"{ma20:.2f}" if ma20 is not None else "--")
        if vol_last is not None:
            st.caption(f"最近一日大盤成交量：約 {vol_last:,.0f}")
        dashboard_meta = load_latest_meta()
        status_chips = []
        if dashboard_meta.get("latest_date"):
            status_chips.append(f"最新資料日：{dashboard_meta.get('latest_date')}")
        if dashboard_meta.get("ssl_mode"):
            status_chips.append(f"連線模式：{dashboard_meta.get('ssl_mode')}")
        if dashboard_meta.get("combined_rows"):
            status_chips.append(f"補資料列數：{dashboard_meta.get('combined_rows')}")
        chip_html = "".join([f'<span class="status-chip">{html_escape(x)}</span>' for x in status_chips])
        st.markdown(
            f'<div class="soft-status"><div class="soft-status-title">官方補資料狀態</div><div class="soft-status-note">目前主系統維持正式版穩定流程，TWSE / TPEx 補資料功能保留啟用；若官方端點偶發異常，主分析會自動降級，不中斷主要流程。</div><div style="margin-top:0.25rem;">{chip_html}</div></div>',
            unsafe_allow_html=True,
        )

    render_market_dashboard(st.session_state.results_data)
    if not st.session_state.results_data:
        st.info("下方聚合模組需先在分析中心跑出結果後才會顯示。")

    with st.expander("官方補資料說明", expanded=False):
        dashboard_meta = load_latest_meta()
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("最新資料日", str(dashboard_meta.get("latest_date", "")))
        m2.metric("TWSE 列數", int(dashboard_meta.get("twse_rows", 0) or 0))
        m3.metric("TPEx 列數", int(dashboard_meta.get("tpex_rows", 0) or 0))
        m4.metric("連線模式", str(dashboard_meta.get("ssl_mode", "")))
        st.caption("正式版原則：先維持主流程穩定，再逐步優化官方資料直連；SSL 直連問題目前列為後續技術債，不阻擋主專案開發。")

if st.session_state.current_page == "持倉中心":
    st.markdown('<div class="main-shell"><h3>💼 持倉中心</h3><p>正式版持倉追蹤區：先看你的持倉判讀、危險區與出場位置；交易紀錄與快照驗證改放到頁面下方附加區，不再擋住主閱讀動線。</p></div>', unsafe_allow_html=True)
    st.caption(f"目前使用者：{st.session_state.current_user}")

    if st.session_state.get("position_stock_pending"):
        st.session_state.position_stock_input = str(st.session_state.get("position_stock_pending", ""))
        st.session_state.position_stock_pending = ""
    elif st.session_state.get("selected_code") and not st.session_state.get("position_stock_input"):
        st.session_state.position_stock_input = str(st.session_state.get("selected_code", "")).split(".")[0]

    ctl1, ctl2, ctl3, ctl4, ctl5 = st.columns([2.0, 1.0, 1.2, 1.0, 1.0] if not st.session_state.mobile_mode else [1,1,1,1,1])
    with ctl1:
        pos_stock = st.text_input("輸入持倉股票", key="position_stock_input", placeholder="例如：1815、2330、7728")
    with ctl2:
        pos_side = st.radio("部位方向", ["做多", "做空"], horizontal=True, key="position_side")
    with ctl3:
        pos_entry = st.number_input("實際進場價", min_value=0.0, value=0.0, step=0.1, format="%.2f", key="position_entry")
    with ctl4:
        pos_shares = st.number_input("股數", min_value=1, value=1000, step=1000, key="position_shares")
    with ctl5:
        pos_mode = "當沖"
        st.text_input("交易模式", value="當沖", disabled=True, key="position_mode_display")

    btn1, btn2 = st.columns([1.2, 1.8])
    calc_position = btn1.button("計算持倉判讀", use_container_width=True)
    if btn2.button("帶入分析中心目前個股", use_container_width=True):
        if st.session_state.get("selected_code"):
            st.session_state.position_stock_pending = str(st.session_state.get("selected_code", "")).split(".")[0]
            st.rerun()
        else:
            st.info("目前分析中心尚未選定股票。")

    st.info("V183 持倉中心：交易時段內以「盤中持倉判讀」為主（價格＋當日區間推算停損／目標）；盤前分析收在折疊僅供對照。")
    st.caption("盤中價優先使用延遲參考價（約 20 分鐘快取）；失敗或非交易時段會回退日線／盤前位階，並於上方標示判讀模式。")


    if calc_position:
        if not str(pos_stock).strip():
            st.warning("請先輸入股票代號。")
        elif float(pos_entry or 0) <= 0:
            st.warning("請先輸入你的實際進場價格。")
        else:
            base_row = analyze_one(str(pos_stock).strip(), market_filter()["score_adj"], name_map)
            if base_row is None:
                st.error("目前無法分析這檔股票，請確認代號是否正確。")
                st.session_state.position_result = None
            else:
                enriched_rows, _, _ = try_apply_twse_hybrid([base_row])
                base_row = enriched_rows[0] if enriched_rows else base_row
                position_quote = resolve_position_reference_quote(base_row)
                jmode, jnote = resolve_position_judgment_mode(position_quote)
                level_ov = None
                if jmode == "盤中":
                    level_ov = compute_intraday_position_levels(float(pos_entry), pos_side, position_quote)
                    if level_ov is None:
                        jmode, jnote = "回退", "盤中價已取得但當日高低區間不足以推算位階，改以盤前／日線位階為主。"
                intraday_primary = jmode == "盤中"
                base_plan = build_position_plan(
                    base_row,
                    float(pos_entry),
                    pos_side,
                    int(pos_shares),
                    pos_mode,
                    position_quote,
                    level_override=level_ov,
                )
                tracking = build_position_tracking(base_row, base_plan)
                hold_decision = build_dynamic_hold_decision(base_row, base_plan, tracking, intraday_primary=intraday_primary)
                st.session_state.position_result = {
                    "row": base_row,
                    "plan": base_plan,
                    "tracking": tracking,
                    "hold_decision": hold_decision,
                    "judgment_mode": jmode,
                    "judgment_note": jnote,
                    "intraday_primary": intraday_primary,
                }

    position_pack = st.session_state.get("position_result")
    if not position_pack:
        st.caption("請輸入股票與實際進場價後按『計算持倉判讀』。")
    else:
        row = position_pack["row"]
        plan = position_pack["plan"]
        tracking = position_pack.get("tracking") or build_position_tracking(row, plan)
        judgment_mode = position_pack.get("judgment_mode")
        judgment_note = position_pack.get("judgment_note")
        intraday_primary = bool(position_pack.get("intraday_primary"))
        if not judgment_mode:
            judgment_mode, judgment_note = resolve_position_judgment_mode({
                "price": plan.get("current_price"),
                "source": str(plan.get("price_source", "") or ""),
            })
            intraday_primary = judgment_mode == "盤中" and (plan.get("level_source") == "盤中波動區間推算")
        hold_decision = position_pack.get("hold_decision")
        if hold_decision is None:
            hold_decision = build_dynamic_hold_decision(row, plan, tracking, intraday_primary=intraday_primary)

        st.markdown(
            f'<div class="main-shell" style="margin-bottom:0.75rem;"><div style="font-size:1.05rem;font-weight:800;color:#f8fafc;">判讀模式：<span style="color:#60a5fa;">{html_escape(str(judgment_mode))}</span></div>'
            f'<div style="font-size:0.88rem;color:#cbd5e1;margin-top:0.35rem;line-height:1.5;">{html_escape(str(judgment_note or ""))}</div>'
            f'<div style="font-size:0.82rem;color:#94a3b8;margin-top:0.45rem;">價格來源：<b>{html_escape(fmt_text(plan.get("price_source", "")) or "--")}</b>'
            f'　｜　更新時間：<b>{html_escape(fmt_text(plan.get("price_time", "")) or "--")}</b>'
            f'　｜　位階來源：<b>{html_escape(fmt_text(plan.get("level_source", "")) or "--")}</b></div></div>',
            unsafe_allow_html=True,
        )

        with st.container(border=True):
            st.markdown("### 盤中持倉判讀（主判讀）")
            st.caption("此區以「你的實際成本 vs 目前價格與當日區間推算的位階」為主；盤前分析請展開下方次判讀。")
            a1, a2, a3, a4, a5 = st.columns(5)
            a1.metric("實際進場價", fmt_price_with_unit(plan.get("entry_price")))
            a2.metric("盤中最新價", fmt_price_with_unit(plan.get("current_price")))
            a3.metric("未實現損益", (f"{plan.get('pnl_amount'):,.0f} 元" if plan.get("pnl_amount") is not None else "--"))
            a4.metric("報酬率", (f"{plan.get('pnl_pct'):.2f}%" if plan.get("pnl_pct") is not None else "--"))
            a5.metric("留倉建議", hold_decision.get("advice", "--"))
            h1, h2, h3 = st.columns(3)
            h1.metric("盤中高參考", fmt_price_with_unit(plan.get("day_high")))
            h2.metric("盤中低參考", fmt_price_with_unit(plan.get("day_low")))
            h3.metric("盤中參考量", fmt_lots(plan.get("day_volume")))

            lb_stop = "到盤中停損剩餘" if intraday_primary else "到停損剩餘"
            lb_res = "到盤中壓力空間" if intraday_primary else "到短壓空間"
            lb_m = "到盤中第一目標空間" if intraday_primary else "到第一目標空間"
            lb_b = "到盤中第二目標空間" if intraday_primary else "到第二目標空間"
            lb_sn = "盤中當沖停損" if intraday_primary else "停損"
            lb_sup = "盤中支撐參考" if intraday_primary else "支撐參考"
            lb_pr = "盤中壓力參考" if intraday_primary else "壓力參考"
            lb_t1 = "盤中第一目標" if intraday_primary else "第一目標"
            lb_t2 = "盤中第二目標" if intraday_primary else "第二目標"

            st.markdown("#### 距離與位階（對應主判讀）")
            lv1, lv2, lv3, lv4 = st.columns(4)
            lv1.metric(lb_stop, (f"{tracking.get('to_stop_pct'):.2f}%" if tracking.get("to_stop_pct") is not None else "--"))
            lv2.metric(lb_res, (f"{tracking.get('to_resistance_pct'):.2f}%" if tracking.get("to_resistance_pct") is not None else "--"))
            lv3.metric(lb_m, (f"{tracking.get('to_mid_pct'):.2f}%" if tracking.get("to_mid_pct") is not None else "--"))
            lv4.metric(lb_b, (f"{tracking.get('to_breakout_pct'):.2f}%" if tracking.get("to_breakout_pct") is not None else "--"))
            lv5, lv6, lv7, lv8 = st.columns(4)
            lv5.metric(lb_sup, fmt_price_with_unit(plan.get("support")))
            lv6.metric(lb_pr, fmt_price_with_unit(plan.get("resistance")))
            lv7.metric(lb_sn, fmt_price_with_unit(plan.get("stop")))
            lv8.metric(lb_t1, fmt_price_with_unit(plan.get("target1")))
            t2a, _t2b, _t2c, _t2d = st.columns(4)
            t2a.metric(lb_t2, fmt_price_with_unit(plan.get("target2")))

            st.markdown("#### 續抱／減碼／出場（盤中邏輯）")
            d1, d2 = st.columns([1.2, 2.8] if not st.session_state.mobile_mode else [1, 1])
            with d1:
                st.metric("建議動作", hold_decision.get("action_hint", "--"))
                st.metric("技術結構", hold_decision.get("technical_structure", "--"))
                st.metric("風險等級", hold_decision.get("risk_level", "--"))
                st.metric("部位狀態", hold_decision.get("position_state", "--"))
            with d2:
                st.write(hold_decision.get("summary", "--"))
                for i, reason in enumerate(hold_decision.get("reasons", []), start=1):
                    st.write(f"{i}. {reason}")
            st.caption(f"成本區間描述：{fmt_text(plan.get('cost_zone', '')) or '--'}　｜　快節奏提示：**{plan.get('action', '--')}** — {plan.get('reason', '--')}")

        chips = [
            f"方向：{plan.get('side', '--')}",
            f"模式：{plan.get('mode', '--')}",
            f"位階：{plan.get('level_source', '--')}",
            f"風險：{hold_decision.get('risk_level', '--')}",
            f"部位：{hold_decision.get('position_state', '--')}",
            f"計畫對齊（對盤前進場）：{tracking.get('alignment', '--')}",
        ]
        if not intraday_primary:
            chips.extend([
                f"盤前結論：{tracking.get('plan_conclusion', '--')}",
                f"盤前訊號：{tracking.get('plan_signal', '--')}",
            ])
        else:
            chips.append("盤前結論／訊號：見下方「原始盤前計畫」折疊")
        render_position_status_chips(chips)

        with st.expander("原始盤前計畫（次判讀／僅供對照）", expanded=False):
            st.caption("以下為 analyze_one 產出的盤前／日線計畫，用來檢查實際進場是否偏離原規劃，不作盤中主判讀。")
            p1, p2, p3, p4 = st.columns(4)
            p1.metric("原始結論", tracking.get("plan_conclusion", "--"))
            p2.metric("原始訊號", tracking.get("plan_signal", "--"))
            p3.metric("原始進場", fmt_price_with_unit(tracking.get("plan_entry")))
            p4.metric("原始風報比", (f"{tracking.get('plan_rr'):.2f}" if tracking.get("plan_rr") is not None else "--"))
            p5, p6, p7, p8 = st.columns(4)
            p5.metric("原始停損", fmt_price_with_unit(tracking.get("plan_stop")))
            p6.metric("原始短期壓力", fmt_price_with_unit(tracking.get("plan_resistance")))
            p7.metric("原始中繼目標", fmt_price_with_unit(tracking.get("plan_mid_target")))
            p8.metric("原始突破目標", fmt_price_with_unit(tracking.get("plan_breakout_target")))

            st.markdown("##### 你的成本 vs 原始盤前進場")
            c1, c2 = st.columns([1.2, 2.8] if not st.session_state.mobile_mode else [1, 1])
            with c1:
                st.metric("實際成本", fmt_price_with_unit(plan.get("entry_price")))
                st.metric("偏離原始進場", (f"{tracking.get('entry_gap_pct'):+.2f}%" if tracking.get("entry_gap_pct") is not None else "--"))
                st.write(tracking.get("alignment", "--"))
            with c2:
                st.write(tracking.get("alignment_note", "--"))
                st.caption(tracking.get("status_note", ""))

            st.markdown("##### 盤前技術欄位（日線分析）")
            core1, core2, core3, core4 = st.columns(4)
            core1.metric("收盤（分析基準）", fmt_price_with_unit(row.get("收盤")))
            core2.metric("盤前進場", fmt_price_with_unit(tracking.get("plan_entry")))
            core3.metric("盤前停損", fmt_price_with_unit(tracking.get("plan_stop")))
            core4.metric("盤前短壓", fmt_price_with_unit(tracking.get("plan_resistance")))
            core5, core6, core7, core8 = st.columns(4)
            core5.metric("盤前中繼", fmt_price_with_unit(tracking.get("plan_mid_target")))
            core6.metric("盤前突破目標", fmt_price_with_unit(tracking.get("plan_breakout_target")))
            core7.metric("量能變化", fmt_text(row.get("量能變化", "")) or "--")
            core8.metric("突破狀態", fmt_text(row.get("突破狀態", "")) or "--")
            rv1, rv2, rv3, rv4, rv5 = st.columns(5)
            rv1.metric("KDJ判讀", kdj_signal(row))
            rv2.metric("MACD判讀", macd_signal(row))
            rv3.metric("價量結論", fmt_text(row.get("價量結論", "")) or "--")
            rv4.metric("官方日資料", fmt_text(row.get("TWSE來源", "")) or "未命中")
            rv5.metric("星級", fmt_text(row.get("星級", "")) or "--")

        lower_cols = st.columns(2) if not st.session_state.mobile_mode else None
        if lower_cols is None:
            with st.container(border=True):
                st.markdown("#### 出場重點")
                st.write(f"1. 留倉建議：**{hold_decision.get('advice', '--')}**")
                st.write(f"2. 建議動作：**{hold_decision.get('action_hint', '--')}**")
                st.write(f"3. 風險等級：**{hold_decision.get('risk_level', '--')}**")
                st.write(f"4. 判斷重點：{hold_decision.get('summary', '--')}")
            with st.container(border=True):
                st.markdown("#### 參考摘要")
                for key in ["摘要1", "摘要2", "摘要3"]:
                    val = fmt_text(row.get(key, ""))
                    if val:
                        st.write(f"- {val}")
        else:
            with lower_cols[0]:
                with st.container(border=True):
                    st.markdown("#### 出場重點")
                    st.write(f"1. 留倉建議：**{hold_decision.get('advice', '--')}**")
                    st.write(f"2. 建議動作：**{hold_decision.get('action_hint', '--')}**")
                    st.write(f"3. 風險等級：**{hold_decision.get('risk_level', '--')}**")
                    st.write(f"4. 判斷重點：{hold_decision.get('summary', '--')}")
            with lower_cols[1]:
                with st.container(border=True):
                    st.markdown("#### 參考摘要")
                    for key in ["摘要1", "摘要2", "摘要3"]:
                        val = fmt_text(row.get(key, ""))
                        if val:
                            st.write(f"- {val}")

        with st.expander("更多持倉參考欄位", expanded=False):
            ref1, ref2, ref3, ref4 = st.columns(4)
            ref1.metric("星級", fmt_text(row.get("星級", "")) or "--")
            ref2.metric("操作評級", fmt_text(row.get("操作評級", "")) or "--")
            ref3.metric("原始風報比", (f"{tracking.get('plan_rr'):.2f}" if tracking.get("plan_rr") is not None else "--"))
            ref4.metric("成本位置", fmt_text(plan.get("cost_zone", "")) or "--")
            ref4.caption(f"技術結構：{hold_decision.get('technical_structure', '--')}")
            ref5, ref6, ref7, ref8, ref9 = st.columns(5)
            ref5.metric("KD-K", fmt_price(row.get("KD_K", "")) or "--")
            ref6.metric("KD-D", fmt_price(row.get("KD_D", "")) or "--")
            ref7.metric("KDJ-J", fmt_price(row.get("KDJ_J", "")) or "--")
            ref8.metric("MACD DIF", fmt_price(row.get("MACD_DIF", "")) or "--")
            ref9.metric("MACD柱體", fmt_price(row.get("MACD_BAR", "")) or "--")
            st.caption(f"5日乖離：{_safe_float(row.get('乖離率5日')):.2f}%" if _safe_float(row.get("乖離率5日")) is not None else "5日乖離：--")
            if str(row.get("TWSE命中", "")) == "是":
                st.caption(f"官方最新日資料：日期 {fmt_text(row.get('TWSE日期', '')) or '--'}；收盤 {fmt_price_with_unit(row.get('TWSE收盤', ''))}；成交量 {fmt_lots(row.get('TWSE成交量', ''))}；成交值 {fmt_trade_value_yi(row.get('TWSE成交值', ''))}。")



    render_section_divider("交易紀錄與快照驗證（附加區）", "這兩塊保留，但改移到最下面並預設收合；先看持倉判讀，需要時再展開回顧。")
    with st.expander("展開交易紀錄與快照驗證", expanded=False):
        st.caption("這裡放你的交易回顧、未平倉部位與快照驗證，不再擋在持倉判讀中間。")
        default_trade_stock = str(st.session_state.get("position_stock_input", "") or "").strip()
        tc1, tc2, tc3, tc4, tc5, tc6 = st.columns([1.5, 1.0, 1.0, 1.1, 1.1, 1.3] if not st.session_state.mobile_mode else [1,1,1,1,1,1])
        with tc1:
            trade_stock = st.text_input("交易股票", value=default_trade_stock, key="trade_stock_input", placeholder="例如：2330、1815")
        with tc2:
            trade_side = st.selectbox("方向", ["做多", "做空"], index=0, key="trade_side")
        with tc3:
            trade_action_options = ["買進", "賣出"] if trade_side == "做多" else ["賣出", "買進"]
            trade_action = st.selectbox("動作", trade_action_options, index=0, key="trade_action")
        with tc4:
            trade_price = st.number_input("成交價", min_value=0.0, value=float(st.session_state.get("position_entry", 0.0) or 0.0), step=0.1, format="%.2f", key="trade_price")
        with tc5:
            trade_qty = st.number_input("數量", min_value=1, value=int(st.session_state.get("position_shares", 1000) or 1000), step=1000, key="trade_qty")
        with tc6:
            trade_note = st.text_input("備註", key="trade_note", placeholder="可留空")

        tb1, tb2 = st.columns([1.2, 1.8])
        save_trade_now = tb1.button("記錄交易", use_container_width=True, key="save_trade_now")
        if tb2.button("帶入目前持倉欄位", use_container_width=True, key="fill_trade_from_position"):
            st.session_state.trade_stock_input = str(st.session_state.get("position_stock_input", "") or "")
            st.session_state.trade_side = st.session_state.get("position_side", "做多")
            st.session_state.trade_price = float(st.session_state.get("position_entry", 0.0) or 0.0)
            st.session_state.trade_qty = int(st.session_state.get("position_shares", 1000) or 1000)
            st.rerun()

        if save_trade_now:
            if not str(trade_stock).strip():
                st.warning("請先輸入交易股票。")
            elif float(trade_price or 0) <= 0:
                st.warning("請先輸入有效成交價。")
            else:
                trade_code, _ = resolve_symbol(str(trade_stock).strip())
                trade_context = build_trade_context(str(trade_stock).strip(), trade_side, name_map)
                trades = load_trades()
                trades.append({
                    "時間": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "股票": trade_code or str(trade_stock).strip().upper(),
                    "方向": normalize_trade_side(trade_side),
                    "動作": trade_action,
                    "價格": float(trade_price),
                    "數量": int(trade_qty),
                    "備註": str(trade_note or ""),
                    "剩餘數量": int(trade_qty) if ((normalize_trade_side(trade_side) == "做多" and trade_action == "買進") or (normalize_trade_side(trade_side) == "做空" and trade_action == "賣出")) else 0,
                    **trade_context
                })
                save_trades(trades)
                st.success(f"已記錄 {trade_code or trade_stock}｜{trade_side}｜{trade_action}。")
                st.rerun()

        trades_all = load_trades()
        closed_df, open_df = pair_trades(trades_all)
        total_pnl, total_count, total_win_rate, total_avg_ret = summary_stats(closed_df if isinstance(closed_df, pd.DataFrame) else pd.DataFrame())
        trade_side_stats = signal_stats(closed_df if isinstance(closed_df, pd.DataFrame) else pd.DataFrame())

        sm1, sm2, sm3, sm4 = st.columns(4)
        sm1.metric("已完成交易", total_count)
        sm2.metric("累計已實現損益", f"{total_pnl:,.0f} 元")
        sm3.metric("整體勝率", f"{total_win_rate:.2f}%")
        sm4.metric("平均報酬率", f"{total_avg_ret:.2f}%")

        if trade_side_stats is not None and not trade_side_stats.empty:
            stat_cards = st.columns(len(trade_side_stats))
            for col, rec in zip(stat_cards, trade_side_stats.to_dict(orient="records")):
                with col:
                    with st.container(border=True):
                        st.markdown(f"#### {rec.get('方向','--')}")
                        st.write(f"完成筆數：**{rec.get('完成筆數', 0)}**")
                        st.write(f"累計損益：**{rec.get('累計損益', 0):,.0f} 元**")
                        st.write(f"勝率：**{rec.get('勝率%', 0):.2f}%**")
                        st.write(f"平均報酬：**{rec.get('平均報酬率%', 0):.2f}%**")

        if isinstance(open_df, pd.DataFrame) and not open_df.empty:
            with st.expander("未平倉部位總覽", expanded=False):
                show_cols = [c for c in ["股票", "方向", "開倉時間", "開倉價", "剩餘數量", "計畫結論", "計畫訊號", "法人方向", "盤前快照時間"] if c in open_df.columns]
                st.dataframe(open_df[show_cols], use_container_width=True, hide_index=True)
        if isinstance(closed_df, pd.DataFrame) and not closed_df.empty:
            with st.expander("已完成交易回顧", expanded=False):
                show_cols = [c for c in ["股票", "方向", "開倉時間", "平倉時間", "開倉價", "平倉價", "數量", "損益", "報酬率%", "計畫結論", "計畫訊號", "法人方向", "盤前快照時間", "盤後快照時間"] if c in closed_df.columns]
                st.dataframe(closed_df.sort_values("平倉時間", ascending=False)[show_cols], use_container_width=True, hide_index=True)

if st.session_state.current_page == "快照中心":
    st.markdown('<div class="main-shell"><h3>🕘 快照中心</h3><p>正式版快照工作區：保留快照中心三層閱讀架構，這版把盤前快照、盤後對照與驗證摘要串起來，不再只是看差異表。</p></div>', unsafe_allow_html=True)
    st.caption(f"目前使用者：{st.session_state.current_user}｜維持 TWSE / TPEx 補資料邏輯；若官方資料異常，視為技術債，不阻擋正式版整理。")

    results = st.session_state.results_data
    market_info = market_filter()
    st.session_state["market_info_cache"] = market_info
    snapshots = load_snapshots()
    df_snap = pd.DataFrame(snapshots) if snapshots else pd.DataFrame()

    pre_count = 0
    post_count = 0
    unique_stocks = 0
    latest_time = "--"
    batch_summary = pd.DataFrame()

    if not df_snap.empty:
        if "類型" in df_snap.columns:
            pre_count = int((df_snap["類型"] == "盤前").sum())
            post_count = int((df_snap["類型"] == "盤後").sum())
        if "股票" in df_snap.columns:
            unique_stocks = int(df_snap["股票"].dropna().nunique())
        if "時間" in df_snap.columns:
            latest_time = str(df_snap["時間"].astype(str).max())

        batch_base = df_snap.copy()
        if "類型" in batch_base.columns:
            batch_base = batch_base[batch_base["類型"].isin(["盤前", "盤後"])]
        if not batch_base.empty and {"時間", "類型", "股票"}.issubset(batch_base.columns):
            batch_summary = (
                batch_base.groupby(["時間", "類型"], as_index=False)
                .agg(檔數=("股票", "count"), 涵蓋股票=("股票", "nunique"))
                .sort_values(["時間", "類型"], ascending=[False, True])
            )
            preview_map = (
                batch_base.groupby(["時間", "類型"])["股票"]
                .apply(lambda s: "、".join(list(dict.fromkeys([str(x) for x in s if str(x).strip()]))[:5]))
                .reset_index(name="股票預覽")
            )
            batch_summary = batch_summary.merge(preview_map, on=["時間", "類型"], how="left")

    top_left, top_right = st.columns([1.25, 1.0])

    with top_left.container(border=True):
        st.markdown("### 快照摘要")
        q1, q2, q3, q4 = st.columns(4)
        q1.metric("總快照數", int(len(df_snap)) if not df_snap.empty else 0)
        q2.metric("盤前快照", pre_count)
        q3.metric("盤後快照", post_count)
        q4.metric("涵蓋股票", unique_stocks)
        st.caption(f"最近一筆快照時間：{latest_time}")
        if batch_summary.empty:
            st.info("目前尚未建立可用批次摘要。你可以先到分析中心跑出結果，再回來儲存盤前／盤後快照。")
        else:
            st.markdown("#### 最近批次")
            st.dataframe(batch_summary.head(8), use_container_width=True, hide_index=True)

    with top_right.container(border=True):
        st.markdown("### 快照儲存")
        current_result_count = len(results) if results else 0
        act1, act2 = st.columns(2)
        act1.metric("目前分析名單", current_result_count)
        act2.metric("可直接儲存", "是" if current_result_count > 0 else "否")
        st.caption("盤前快照負責定義名單；盤後可另外保存，或直接用下方的盤前名單一鍵產生盤後對照。")
        if results:
            st.caption("快照儲存改成明確分流：直接選要存做多快照或做空快照，避免盤前欄位混用。舊快照不會自動改寫，新存的才會套用正確方向。")
            s1, s2 = st.columns(2)
            if s1.button("儲存盤前做多快照", use_container_width=True):
                save_snapshot("盤前", results, forced_mode="多方")
                st.success("已儲存盤前做多快照。")
                st.rerun()
            if s2.button("儲存盤前做空快照", use_container_width=True):
                save_snapshot("盤前", results, forced_mode="空方")
                st.success("已儲存盤前做空快照。")
                st.rerun()
            s3, s4 = st.columns(2)
            if s3.button("儲存盤後做多快照", use_container_width=True):
                save_snapshot("盤後", results, forced_mode="多方")
                st.success("已儲存盤後做多快照。")
                st.rerun()
            if s4.button("儲存盤後做空快照", use_container_width=True):
                save_snapshot("盤後", results, forced_mode="空方")
                st.success("已儲存盤後做空快照。")
                st.rerun()
        else:
            st.info("請先到『分析中心』跑出分析結果，再來儲存快照。")

    tab_hist, tab_pair, tab_batch = st.tabs(["快照歷史", "雙快照比對", "盤前名單盤後對照"])

    with tab_hist:
        st.markdown("### 快照歷史")
        if df_snap.empty:
            st.caption("目前沒有快照紀錄。")
        else:
            if not batch_summary.empty:
                st.caption("先看每次儲存批次，再決定是否往下展開原始快照明細。")
                st.dataframe(batch_summary, use_container_width=True, hide_index=True)

            with st.expander("查看原始快照與清理", expanded=False):
                c1, c2, c3 = st.columns(3)
                with c1:
                    stock_options = ["全部"] + sorted(df_snap["股票"].dropna().unique().tolist()) if "股票" in df_snap.columns else ["全部"]
                    hist_stock = st.selectbox("選擇股票", stock_options, index=0, key="hist_stock")
                with c2:
                    type_options = ["全部", "盤前", "盤後"]
                    hist_type = st.selectbox("選擇類型", type_options, index=0, key="hist_type")

                hist_base = df_snap.copy()
                if "類型" in hist_base.columns:
                    hist_base = hist_base[hist_base["類型"].isin(["盤前", "盤後"])]
                if hist_stock != "全部" and "股票" in hist_base.columns:
                    hist_base = hist_base[hist_base["股票"] == hist_stock]
                if hist_type != "全部" and "類型" in hist_base.columns:
                    hist_base = hist_base[hist_base["類型"] == hist_type]

                batch_options = []
                if not hist_base.empty and "時間" in hist_base.columns:
                    batch_counts = hist_base.groupby("時間").size().sort_index(ascending=False)
                    batch_options = [f"{t}｜{int(n)}檔" for t, n in batch_counts.items()]

                with c3:
                    hist_batch = st.selectbox("選擇儲存時段", batch_options, index=0, key="hist_batch") if batch_options else ""

                hist = hist_base.copy()
                selected_time = ""
                if hist_batch:
                    selected_time = hist_batch.split("｜")[0]
                    hist = hist[hist["時間"].astype(str) == selected_time]

                if not hist.empty:
                    hist = hist.sort_values(["時間", "股票"], ascending=[False, True])
                    st.caption(f"目前篩選共 {len(hist)} 筆；刪除只會作用在下表看到的資料。")
                else:
                    st.caption("目前篩選後沒有資料。")

                snap_cols = [c for c in ["時間", "類型", "快照邏輯", "股票", "收盤", "支撐", "進場", "停損", "短期壓力", "中繼目標", "突破目標", "風報比", "結論", "交易訊號"] if c in hist.columns]
                if snap_cols:
                    st.dataframe(hist[snap_cols], use_container_width=True, hide_index=True)

                d1, d2 = st.columns(2)
                if d1.button("刪除目前篩選結果", use_container_width=True):
                    delete_keys = set(hist.apply(lambda r: (r.get("時間", ""), r.get("類型", ""), r.get("股票", "")), axis=1).tolist()) if not hist.empty else set()
                    remain = []
                    for s in snapshots:
                        key = (s.get("時間", ""), s.get("類型", ""), s.get("股票", ""))
                        if key not in delete_keys:
                            remain.append(s)
                    save_snapshots(remain)
                    st.success("已刪除目前篩選結果的快照。")
                    st.rerun()

                if d2.button("清空全部快照", use_container_width=True):
                    save_snapshots([])
                    st.success("已清空全部快照。")
                    st.rerun()

    with tab_pair:
        st.markdown("### 盤前 / 盤後雙比對")
        if df_snap.empty:
            st.caption("目前沒有可比對的快照。")
        else:
            compare_df = df_snap.copy()
            if "類型" in compare_df.columns:
                compare_df = compare_df[compare_df["類型"].isin(["盤前", "盤後"])]

            if compare_df.empty or "股票" not in compare_df.columns:
                st.caption("目前沒有盤前 / 盤後快照可供比對。")
            else:
                cmp_stock_options = sorted(compare_df["股票"].dropna().unique().tolist())
                selected_cmp_stock = st.selectbox("比對股票", cmp_stock_options, index=0, key="cmp_stock")
                cmp_stock_df = compare_df[compare_df["股票"] == selected_cmp_stock].sort_values("時間", ascending=False)

                pre_df = cmp_stock_df[cmp_stock_df["類型"] == "盤前"].sort_values("時間", ascending=False)
                post_df = cmp_stock_df[cmp_stock_df["類型"] == "盤後"].sort_values("時間", ascending=False)
                st.caption("V144 維持快照中心分級狀態；這版新增盤前流動性門檻，避免不適合當沖觀察的股票進榜。")

                pre_options = pre_df["時間"].tolist() if not pre_df.empty else ["無資料"]
                post_options = post_df["時間"].tolist() if not post_df.empty else ["無資料"]
                sel1, sel2 = st.columns(2)
                chosen_pre = sel1.selectbox("選擇盤前時間", pre_options, index=0, key="pre_time")
                chosen_post = sel2.selectbox("選擇盤後時間", post_options, index=0, key="post_time")

                if not pre_df.empty and not post_df.empty:
                    pre_row = pre_df[pre_df["時間"] == chosen_pre].iloc[0].to_dict()
                    post_row = post_df[post_df["時間"] == chosen_post].iloc[0].to_dict()
                    compare_table = build_pre_post_compare_table(pre_row, post_row)
                    diff_count = int((compare_table["是否變化"] == "是").sum()) if not compare_table.empty else 0
                    pair_price_change = compare_price_change(pre_row.get("收盤", ""), post_row.get("收盤", ""))
                    pair_structure = build_pair_structure_label(pre_row, post_row)
                    changed_preview = "、".join(compare_table.loc[compare_table["是否變化"] == "是", "欄位"].tolist()[:5]) if not compare_table.empty else ""
                    pair_detail_row = {
                        "價量結論": post_row.get("價量結論", ""),
                        "價格變化": pair_price_change,
                        "結構變化": pair_structure,
                        "盤前收盤": pre_row.get("收盤", ""),
                        "盤後收盤": post_row.get("收盤", ""),
                        "盤前支撐": pre_row.get("支撐", ""),
                        "盤後最低": post_row.get("支撐", ""),
                        "盤前壓力": pre_row.get("短期壓力", ""),
                        "盤後最高": post_row.get("短期壓力", ""),
                        "盤前建議進場": pre_row.get("進場", ""),
                        "模擬進場價": pre_row.get("進場", ""),
                        "盤前停損": pre_row.get("停損", ""),
                        "盤前風報比": pre_row.get("風報比", ""),
                        "盤後風報比": post_row.get("風報比", ""),
                        "盤前結論": pre_row.get("結論", ""),
                        "盤後結論": post_row.get("結論", ""),
                        "盤前訊號": pre_row.get("交易訊號", ""),
                        "盤後訊號": post_row.get("交易訊號", ""),
                        "股票": selected_cmp_stock,
                        "股票代碼": str(pre_row.get("股票代碼", "") or post_row.get("股票代碼", "")),
                    }

                    layer_overview, layer_diff, layer_detail = st.tabs(["總覽層", "差異層", "單股細節層"])

                    with layer_overview:
                        a1, a2, a3, a4 = st.columns(4)
                        a1.metric("盤前時間", pre_row.get("時間", ""))
                        a2.metric("盤後時間", post_row.get("時間", ""))
                        a3.metric("有變化欄位", diff_count)
                        a4.metric("結構判讀", pair_structure)
                        render_compare_status_cards(pair_detail_row)
                        if changed_preview:
                            st.info(f"這次最明顯的變化欄位：{changed_preview}")
                        else:
                            st.info("這組盤前 / 盤後快照主要欄位沒有明顯差異。")
                        overview_cols = [c for c in ["欄位", "盤前", "盤後", "差異"] if c in compare_table.columns]
                        st.dataframe(compare_table[overview_cols].head(6), use_container_width=True, hide_index=True)

                    with layer_diff:
                        pair_left, pair_right = st.columns([1.0, 1.5])
                        pair_left.metric("未變動欄位", max(len(compare_table) - diff_count, 0))
                        changed_only = pair_right.checkbox("只顯示有變化欄位", value=True, key="pair_changed_only")
                        show_table = compare_table[compare_table["是否變化"] == "是"] if changed_only else compare_table
                        st.dataframe(show_table, use_container_width=True, hide_index=True)

                    with layer_detail:
                        render_compare_status_cards(pair_detail_row)
                        render_compare_matrix(pair_detail_row)
                        left_detail, right_detail = st.columns(2)
                        with left_detail:
                            st.markdown("#### 盤前快照")
                            l1, l2, l3 = st.columns(3)
                            l1.metric("盤前收盤", fmt_price(pre_row.get("收盤", "")))
                            l2.metric("盤前進場", fmt_price(pre_row.get("進場", "")))
                            l3.metric("盤前停損", fmt_price(pre_row.get("停損", "")))
                            l4, l5, l6 = st.columns(3)
                            l4.metric("盤前壓力", fmt_price(pre_row.get("短期壓力", "")))
                            l5.metric("盤前風報比", fmt_price(pre_row.get("風報比", "")))
                            l6.metric("盤前結論", fmt_text(pre_row.get("結論", "")))
                        with right_detail:
                            st.markdown("#### 盤後快照")
                            r1, r2, r3 = st.columns(3)
                            r1.metric("盤後收盤", fmt_price(post_row.get("收盤", "")))
                            r2.metric("盤後進場", fmt_price(post_row.get("進場", "")))
                            r3.metric("盤後停損", fmt_price(post_row.get("停損", "")))
                            r4, r5, r6 = st.columns(3)
                            r4.metric("盤後壓力", fmt_price(post_row.get("短期壓力", "")))
                            r5.metric("盤後風報比", fmt_price(post_row.get("風報比", "")))
                            r6.metric("盤後結論", fmt_text(post_row.get("結論", "")))
                else:
                    st.warning("這檔股票目前需要同時有盤前與盤後快照，才能做雙比對。")

    with tab_batch:
        st.markdown("### 盤前名單一鍵盤後對照")
        pre_groups = grouped_prefast_snapshots()
        if not pre_groups:
            st.caption("目前沒有盤前快照名單可供盤後對照。")
        else:
            group_map = {g["批次ID"]: g for g in pre_groups}
            batch_ids = [g["批次ID"] for g in pre_groups]
            if st.session_state.get("batch_pre_group_id") not in batch_ids:
                st.session_state["batch_pre_group_id"] = batch_ids[0]

            selected_batch_id = st.selectbox(
                "選擇盤前名單",
                batch_ids,
                index=batch_ids.index(st.session_state["batch_pre_group_id"]),
                key="batch_pre_group_id",
                format_func=lambda bid: format_prefast_group_label(group_map[bid]) if bid in group_map else str(bid),
            )
            chosen_group = group_map[selected_batch_id]
            chosen_label = format_prefast_group_label(chosen_group)
            chosen_batch_time = chosen_group["時間"]
            post_market_status = get_post_market_status(snapshot_time_str=chosen_batch_time)

            group_df = pd.DataFrame([
                {
                    "批次時間": g["時間"],
                    "批次ID": g["批次ID"],
                    "快照邏輯": g.get("快照邏輯", "多方"),
                    "檔數": g["檔數"],
                    "股票預覽": "、".join(g["股票清單"][:5])
                }
                for g in pre_groups
            ])

            active_batch_id = st.session_state.get("batch_compare_batch_id", "")
            active_compare_df = st.session_state.get("batch_compare_df") if isinstance(st.session_state.get("batch_compare_df"), pd.DataFrame) else pd.DataFrame()
            has_active_for_selected = isinstance(active_compare_df, pd.DataFrame) and not active_compare_df.empty and active_batch_id == selected_batch_id

            top_shell = st.container(border=True)
            with top_shell:
                render_section_divider("批次操作區", f"{APP_VERSION} 將快照中心盤後狀態改成分級文案；同時維持批次選單、摘要、差異總覽與單股明細的同批次綁定。")
                top_left, top_right = st.columns([1.15, 0.85])
                with top_left:
                    preview_text = "、".join(chosen_group["股票清單"][:8]) if chosen_group.get("股票清單") else "--"
                    render_info_card_grid([
                        {"label": "批次時間", "value": chosen_batch_time, "sub": "盤前名單建立時間"},
                        {"label": "快照邏輯", "value": chosen_group.get("快照邏輯", "多方"), "sub": "這批盤前快照使用的策略方向"},
                        {"label": "名單預覽", "value": f"{chosen_group['檔數']} 檔", "sub": f"股票預覽：{preview_text}"},
                    ])
                    with st.expander("查看全部批次與編輯這次盤前名單", expanded=False):
                        st.dataframe(group_df.drop(columns=["批次ID"]), use_container_width=True, hide_index=True)
                        st.caption(f"目前正在編輯批次：{chosen_label}｜{chosen_group.get('快照邏輯', '多方')}。可直接多選要排除的股票，刪除後會保留這次盤前名單的其他股票。")
                        stock_options = chosen_group["股票清單"]
                        selected_remove = st.multiselect("選擇要從這次盤前名單刪除的股票", stock_options, key=f"remove_pre_stocks_{selected_batch_id}")
                        c1, c2 = st.columns(2)
                        c1.dataframe(pd.DataFrame({"目前盤前名單": stock_options}), use_container_width=True, hide_index=True)
                        if c2.button("刪除所選股票", use_container_width=True, key=f"delete_pre_stocks_{selected_batch_id}"):
                            if selected_remove:
                                delete_selected_from_pre_snapshot(chosen_batch_time, selected_remove)
                                st.session_state.pop("batch_compare_df", None)
                                st.session_state.pop("batch_compare_label", None)
                                st.session_state.pop("batch_compare_batch_id", None)
                                st.success(f"已從 {chosen_batch_time} 的盤前名單刪除 {len(selected_remove)} 檔股票。")
                                st.rerun()
                            else:
                                st.warning("請先選擇要刪除的股票。")

                with top_right:
                    status_text = f"盤後狀態：{post_market_status.get('stage', '')}｜{post_market_status.get('message', '')}"
                    status_tone = post_market_status.get("tone", "info")
                    if post_market_status.get("ready", False) or status_tone == "success":
                        st.success(status_text)
                    elif status_tone == "warning":
                        st.warning(status_text)
                    else:
                        st.info(status_text)
                    b1, b2 = st.columns(2)
                    if b1.button("產生盤後資訊對照", use_container_width=True, key=f"create_batch_compare_{selected_batch_id}"):
                        compare_batch_df = compare_pre_snapshot_with_current(chosen_group["rows"], market_info["score_adj"], name_map, snapshot_time_str=chosen_batch_time)
                        st.session_state["batch_compare_df"] = compare_batch_df
                        st.session_state["batch_compare_label"] = chosen_label
                        st.session_state["batch_compare_batch_id"] = selected_batch_id
                        save_compare_result_history(selected_batch_id, chosen_label, chosen_group.get("快照邏輯", "多方"), compare_batch_df)
                        st.rerun()
                    if b2.button("清除對照結果", use_container_width=True, key=f"clear_batch_compare_{selected_batch_id}"):
                        st.session_state.pop("batch_compare_df", None)
                        st.session_state.pop("batch_compare_label", None)
                        st.session_state.pop("batch_compare_batch_id", None)
                        clear_compare_result_history(selected_batch_id)
                        st.rerun()
                    if active_batch_id and active_batch_id != selected_batch_id and isinstance(active_compare_df, pd.DataFrame) and not active_compare_df.empty:
                        active_group = group_map.get(active_batch_id)
                        active_label = format_prefast_group_label(active_group) if active_group else st.session_state.get("batch_compare_label", "")
                        st.info(f"目前載入中的對照結果仍是：{active_label}。若要切到現在選的批次，請重新按一次『產生盤後資訊對照』。")
                    elif has_active_for_selected:
                        st.caption(f"目前載入中的對照結果：{chosen_label}")
                    else:
                        st.caption("目前尚未產生這批盤前名單的盤後對照。")
                    st.caption("操作順序：先確認盤後狀態，再產生對照；若切換批次，請重新產生一次。")

            batch_summary_tab, batch_diff_tab, batch_detail_tab = st.tabs(["批次摘要", "差異總覽", "單股明細"])

            compare_batch_df = st.session_state.get("batch_compare_df") if isinstance(st.session_state.get("batch_compare_df"), pd.DataFrame) else pd.DataFrame()
            saved_label = st.session_state.get("batch_compare_label", "")
            saved_batch_id = st.session_state.get("batch_compare_batch_id", "")
            has_active_result = isinstance(compare_batch_df, pd.DataFrame) and not compare_batch_df.empty and saved_batch_id == selected_batch_id

            if not has_active_result:
                history_df = load_compare_result_history_df(selected_batch_id)
                if not history_df.empty:
                    compare_batch_df = history_df.copy()
                    st.session_state["batch_compare_df"] = compare_batch_df
                    st.session_state["batch_compare_label"] = chosen_label
                    st.session_state["batch_compare_batch_id"] = selected_batch_id
                    saved_label = chosen_label
                    saved_batch_id = selected_batch_id
                    has_active_result = True

            compare_batch_view = pd.DataFrame()
            structure_filter = "全部"
            price_filter = "全部"
            stock_keyword = ""
            if has_active_result:
                compare_batch_view = compare_batch_df.copy()

            with batch_summary_tab:
                st.markdown(f"#### 批次摘要：{chosen_label}")
                st.caption(f"目前鎖定批次 ID：{selected_batch_id}。這裡只放批次層資訊與最重要摘要，不直接塞全部差異與單股明細。")
                render_info_card_grid([
                    {"label": "批次檔數", "value": f"{chosen_group.get('檔數', 0)} 檔", "sub": "這次盤前名單的股票數量"},
                    {"label": "預覽股票數", "value": str(min(len(chosen_group.get("股票清單", [])), 8)), "sub": "摘要區只先顯示前八檔"},
                    {"label": "盤後可判定", "value": "是" if post_market_status.get("ready", False) else "否", "sub": post_market_status.get("stage", "")},
                    {"label": "已產生對照", "value": "是" if has_active_result else "否", "sub": "若切換批次需重新產生"},
                ])

                selected_row_df = group_df[group_df["批次ID"] == selected_batch_id].copy()
                if not selected_row_df.empty:
                    st.dataframe(selected_row_df.drop(columns=["批次ID"]), use_container_width=True, hide_index=True)

                if has_active_result:
                    validation_pack = build_snapshot_validation_summary(compare_batch_df)
                    summary1, summary2, summary3, summary4 = st.columns(4)
                    summary1.metric("結構變強", int((compare_batch_df["結構變化"] == "變強").sum()))
                    summary2.metric("結構持平", int((compare_batch_df["結構變化"] == "持平").sum()))
                    summary3.metric("結構變弱", int((compare_batch_df["結構變化"] == "變弱").sum()))
                    summary4.metric("已完成對照", len(compare_batch_df))
                    render_section_divider("快照驗證摘要", "這批盤前名單不是只看差異，也要回頭看命中、中性與失真。")
                    vv1, vv2, vv3, vv4, vv5 = st.columns(5)
                    vv1.metric("快照方向", validation_pack.get("mode", chosen_group.get("快照邏輯", "多方")))
                    vv2.metric("驗證命中", validation_pack.get("hit", 0))
                    vv3.metric("驗證中性", validation_pack.get("neutral", 0))
                    vv4.metric("驗證失真", validation_pack.get("miss", 0))
                    vv5.metric("命中率", f"{validation_pack.get('hit_rate'):.2f}%" if validation_pack.get("hit_rate") is not None else "--")

                    nv1, nv2 = st.columns(2)
                    with nv1:
                        with st.container(border=True):
                            st.markdown("#### 命中重點")
                            st.write(validation_pack.get("top_hit") or "目前沒有明確命中名單。")
                    with nv2:
                        with st.container(border=True):
                            st.markdown("#### 失真重點")
                            st.write(validation_pack.get("top_miss") or "目前沒有明確失真名單。")

                    overview_cols = [c for c in [
                        "股票", "股票代碼", "價格變化", "結構變化", "盤前收盤", "盤後收盤", "支撐驗證", "壓力驗證", "停損觸發"
                    ] if c in compare_batch_df.columns]
                    if overview_cols:
                        st.markdown("#### 本批次重點總覽")
                        st.dataframe(compare_batch_df[overview_cols].head(8), use_container_width=True, hide_index=True)
                else:
                    st.info("目前還沒有這批盤前名單的盤後對照結果。先按上方『產生盤後資訊對照』，再往下看差異總覽與單股明細。")

            with batch_diff_tab:
                render_section_divider("差異總覽", "這裡先看最常變的欄位與目前篩選後的差異輪廓，再決定要不要進到單股明細。")
                if not has_active_result:
                    st.caption("請先產生這批盤前名單的盤後對照結果。")
                else:
                    f1, f2, f3 = st.columns([1.05, 1.05, 1.4])
                    structure_filter = f1.selectbox("結構變化篩選", ["全部", "變強", "持平", "變弱"], index=0, key="batch_structure_filter_v129")
                    price_filter = f2.selectbox("價格變化篩選", ["全部", "上漲", "下跌", "持平", "待盤後", "資料不足"], index=0, key="batch_price_filter_v129")
                    stock_keyword = f3.text_input("股票關鍵字篩選", value="", key="batch_stock_keyword_v129", placeholder="輸入代碼或名稱")

                    compare_batch_view = compare_batch_df.copy()
                    if structure_filter != "全部" and "結構變化" in compare_batch_view.columns:
                        compare_batch_view = compare_batch_view[compare_batch_view["結構變化"] == structure_filter]
                    if price_filter != "全部" and "價格變化" in compare_batch_view.columns:
                        compare_batch_view = compare_batch_view[compare_batch_view["價格變化"] == price_filter]
                    if stock_keyword.strip() and "股票" in compare_batch_view.columns:
                        kw = stock_keyword.strip()
                        compare_batch_view = compare_batch_view[
                            compare_batch_view["股票"].astype(str).str.contains(kw, case=False, na=False)
                            | compare_batch_view.get("股票代碼", pd.Series(index=compare_batch_view.index, dtype=str)).astype(str).str.contains(kw, case=False, na=False)
                        ]

                    field_summary = build_batch_field_diff_summary(compare_batch_view)
                    top_field_summary = field_summary.head(5).copy() if not field_summary.empty else pd.DataFrame()
                    render_field_highlight_cards(top_field_summary, top_n=3)

                    st.markdown("#### 欄位差異 Top 5")
                    if not top_field_summary.empty:
                        st.dataframe(top_field_summary, use_container_width=True, hide_index=True)
                    else:
                        st.caption("目前沒有可顯示的欄位差異。")

                    diff_focus_cols = [c for c in [
                        "股票", "價格變化", "結構變化", "盤前結論", "盤後結論", "盤前訊號", "盤後訊號", "判斷理由"
                    ] if c in compare_batch_view.columns]
                    with st.expander("查看完整欄位差異與差異股票名單", expanded=False):
                        if not field_summary.empty:
                            st.dataframe(field_summary, use_container_width=True, hide_index=True)
                        if diff_focus_cols:
                            st.dataframe(compare_batch_view[diff_focus_cols], use_container_width=True, hide_index=True)

            with batch_detail_tab:
                render_section_divider("單股明細", "這裡只保留名單與單股細節，不再讓總覽與明細同時塞滿同一個區塊。")
                if not has_active_result:
                    st.caption("請先產生這批盤前名單的盤後對照結果。")
                else:
                    compare_batch_view = compare_batch_df.copy()
                    if structure_filter != "全部" and "結構變化" in compare_batch_view.columns:
                        compare_batch_view = compare_batch_view[compare_batch_view["結構變化"] == structure_filter]
                    if price_filter != "全部" and "價格變化" in compare_batch_view.columns:
                        compare_batch_view = compare_batch_view[compare_batch_view["價格變化"] == price_filter]
                    if stock_keyword.strip() and "股票" in compare_batch_view.columns:
                        kw = stock_keyword.strip()
                        compare_batch_view = compare_batch_view[
                            compare_batch_view["股票"].astype(str).str.contains(kw, case=False, na=False)
                            | compare_batch_view.get("股票代碼", pd.Series(index=compare_batch_view.index, dtype=str)).astype(str).str.contains(kw, case=False, na=False)
                        ]

                    if compare_batch_view.empty:
                        st.caption("目前篩選條件下沒有可顯示的股票。")
                    else:
                        left_list, right_detail = st.columns([0.95, 1.55])
                        with left_list:
                            st.caption(f"目前顯示 {len(compare_batch_view)} / {len(compare_batch_df)} 檔。")
                            list_cols = [c for c in ["股票", "價格變化", "結構變化", "盤後結論", "盤後訊號"] if c in compare_batch_view.columns]
                            st.dataframe(compare_batch_view[list_cols], use_container_width=True, hide_index=True)
                            detail_options = compare_batch_view["股票"].tolist()
                            selected_detail_stock = st.selectbox("選擇要查看的股票", detail_options, key=f"detail_compare_stock_{selected_batch_id}")
                        with right_detail:
                            detail_row = compare_batch_view[compare_batch_view["股票"] == selected_detail_stock].iloc[0].to_dict()
                            render_batch_compare_detail(detail_row, key_prefix="v129")



