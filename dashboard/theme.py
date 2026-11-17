"""Light theme tokens and CSS for the dashboard."""
from __future__ import annotations

import streamlit as st

# Status palette: (text/accent, soft background)
STATUS = {
    "HEALTHY": ("#15803d", "#dcfce7"),
    "SUSPECTED": ("#b45309", "#fef3c7"),
    "RECOVERING": ("#0369a1", "#e0f2fe"),
    "FAILED": ("#b91c1c", "#fee2e2"),
    "UNKNOWN": ("#475569", "#f1f5f9"),
    "UNREACHABLE": ("#b45309", "#fef3c7"),
}
ACCENT = "#4f46e5"
ACCENT_SOFT = "#eef2ff"
INK = "#1e293b"
MUTED = "#64748b"
LINE = "#e5e9f2"

CATEGORY_COLORS = {
    "election": "#4f46e5",
    "failure": "#dc2626",
    "suspicion": "#d97706",
    "recovery": "#16a34a",
    "fault": "#9333ea",
    "role": "#0891b2",
    "info": "#64748b",
    "heartbeat": "#cbd5e1",
}

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap');
.stApp, .stMarkdown, .stMarkdown p, h1, h2, h3, h4, label, .stButton button, .stTextInput input,
[data-testid="stMetricValue"], .dm-root, .dm-root div, .dm-root p, .dm-root b {
    font-family: 'Plus Jakarta Sans', 'Segoe UI', system-ui, -apple-system, sans-serif;
}
.block-container { padding-top: 1.6rem; padding-bottom: 3rem; max-width: 1280px; }
h1, h2, h3 { letter-spacing: -0.01em; color: #1e293b; }
[data-testid="stSidebar"] { background: #ffffff; border-right: 1px solid #e5e9f2; }
[data-testid="stSidebar"] .block-container { padding-top: 1rem; }
#MainMenu, footer { visibility: hidden; }

/* ---------- hero ---------- */
.dm-hero { display:flex; justify-content:space-between; align-items:center; gap:1.2rem; flex-wrap:wrap;
    background: linear-gradient(120deg, #eef2ff 0%, #e8f4fd 60%, #f0fdf4 100%);
    border:1px solid #dbe3f5; border-radius:18px; padding:1.25rem 1.5rem; margin-bottom:1.1rem; }
.dm-hero h1 { margin:0; font-size:1.55rem; font-weight:800; color:#1e1b4b; }
.dm-hero p { margin:.25rem 0 0; color:#475569; font-size:.92rem; }
.dm-chip { display:inline-flex; align-items:center; gap:.5rem; padding:.45rem .9rem; border-radius:999px;
    font-weight:700; font-size:.88rem; border:1px solid transparent; white-space:nowrap; }
.dm-dot { width:.62rem; height:.62rem; border-radius:50%; display:inline-block; }
.dm-pulse { animation: dmpulse 1.4s ease-in-out infinite; }
@keyframes dmpulse { 0%,100% { opacity:1; transform:scale(1);} 50% { opacity:.45; transform:scale(1.35);} }
@media (prefers-reduced-motion: reduce) { .dm-pulse { animation:none; } }

/* ---------- KPI cards ---------- */
.dm-kpi { background:#fff; border:1px solid #e5e9f2; border-radius:14px; padding:.95rem 1.1rem;
    box-shadow:0 1px 2px rgba(15,23,42,.04); position:relative; overflow:hidden; height:100%; }
.dm-kpi::before { content:""; position:absolute; left:0; top:0; bottom:0; width:4px; background:var(--accent,#4f46e5); }
.dm-kpi .lbl { color:#64748b; font-size:.8rem; font-weight:600; }
.dm-kpi .val { font-size:2rem; font-weight:800; color:#1e293b; line-height:1.15; margin-top:.15rem; }
.dm-kpi .sub { color:#94a3b8; font-size:.76rem; margin-top:.1rem; }

/* ---------- node cards ---------- */
.dm-node { background:#fff; border:1px solid #e5e9f2; border-radius:14px; padding:.95rem 1rem 1rem;
    box-shadow:0 1px 2px rgba(15,23,42,.04); border-top:4px solid var(--accent,#94a3b8); height:100%; }
.dm-node.coord { box-shadow:0 0 0 2px #c7d2fe, 0 4px 14px rgba(79,70,229,.12); }
.dm-node .top { display:flex; justify-content:space-between; align-items:center; margin-bottom:.55rem; }
.dm-node .name { font-weight:800; font-size:1.05rem; color:#1e293b; }
.dm-node .row { display:flex; justify-content:space-between; font-size:.82rem; padding:.18rem 0;
    border-bottom:1px dashed #eef1f7; color:#64748b; }
.dm-node .row b { color:#1e293b; font-weight:600; }
.dm-pill { display:inline-block; padding:.12rem .6rem; border-radius:999px; font-size:.74rem; font-weight:700; }
.dm-tags { margin-top:.5rem; display:flex; gap:.35rem; flex-wrap:wrap; }

/* ---------- event feed ---------- */
.dm-feed { background:#fff; border:1px solid #e5e9f2; border-radius:14px; padding:.4rem .9rem; max-height:395px;
    overflow-y:auto; }
.dm-ev { display:flex; gap:.7rem; padding:.55rem 0; border-bottom:1px solid #f1f4f9; align-items:flex-start; }
.dm-ev:last-child { border-bottom:none; }
.dm-ev .dot { margin-top:.35rem; flex:0 0 auto; }
.dm-ev .txt { font-size:.84rem; color:#1e293b; line-height:1.35; }
.dm-ev .when { font-size:.74rem; color:#94a3b8; }
.dm-empty { color:#94a3b8; font-size:.9rem; padding:1.2rem .2rem; text-align:center; }

/* ---------- misc ---------- */
.dm-section { font-size:1.02rem; font-weight:700; color:#1e293b; margin:.2rem 0 .5rem; }
.dm-note { background:#f8fafc; border:1px solid #e5e9f2; border-radius:12px; padding:.7rem .95rem; color:#475569;
    font-size:.86rem; }
.dm-warn { background:#fffbeb; border:1px solid #fde68a; color:#92400e; border-radius:12px; padding:.7rem .95rem;
    font-size:.88rem; }
.dm-danger { background:#fef2f2; border:1px solid #fecaca; color:#991b1b; border-radius:12px; padding:.7rem .95rem;
    font-size:.88rem; }
[data-testid="stTabs"] button[role="tab"] { font-weight:600; }
div[data-testid="stDataFrame"] { border:1px solid #e5e9f2; border-radius:12px; overflow:hidden; }
.stButton button[kind="primary"] { background:#4f46e5; border-color:#4f46e5; }
</style>
"""


def inject_css() -> None:
    st.markdown(CSS, unsafe_allow_html=True)


def status_colors(health: str):
    return STATUS.get(health, STATUS["UNKNOWN"])
