"""BarrioScout — Streamlit dashboard entry point.

Run with:
    streamlit run dashboard/app.py
"""

from __future__ import annotations

import hashlib
import re

import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

from data_loader import load_neighborhood_geometries, load_neighborhood_scores
from detail_panel import render_default, render_detail
from map_component import create_map

# ── Page config ───────────────────────────────────────────────────────────────
# Must be the very first Streamlit call in the script.
st.set_page_config(
    page_title="BarrioScout",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🏘️",
)

# ── Global CSS ────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

    /* ── Base ── */
    html, body, [class*="css"], .stApp {
        font-family: 'Inter', sans-serif !important;
        background-color: #F8F9FA !important;
        color: #191C1D !important;
    }

    /* ── Hide Streamlit chrome ── */
    #MainMenu                               { visibility: hidden !important; }
    header[data-testid="stHeader"]          { visibility: hidden !important; }
    footer                                  { visibility: hidden !important; }
    [data-testid="stToolbar"]               { visibility: hidden !important; }
    [data-testid="stDecoration"]            { display: none !important; }
    [data-testid="stAppViewBlockContainer"] { padding-top: 0 !important; }

    /* ── Layout ── */
    .block-container {
        padding-top:    0.6rem  !important;
        padding-bottom: 3.2rem  !important;
        max-width:      100%    !important;
        padding-left:   1.5rem  !important;
        padding-right:  1.5rem  !important;
    }

    /* ── Header ── */
    .bs-logo {
        font-size: 1.25rem;
        font-weight: 900;
        color: #191C1D;
        letter-spacing: -0.02em;
        margin: 0;
        line-height: 1.2;
    }
    .bs-tagline {
        font-size: 0.8rem;
        font-weight: 400;
        color: #777587;
        margin: 0;
        line-height: 1.4;
    }

    /* ── City toggle: two st.button pills ──
       type="primary"  → active city   (filled indigo)
       type="secondary" → inactive city (outline)
       Real DOM: stElementContainer gets class st-key-{key}, inner button gets
       data-testid="stBaseButton-primary" or "stBaseButton-secondary". */
    .st-key-city_btn_Madrid [data-testid="stBaseButton-primary"],
    .st-key-city_btn_Madrid [data-testid="stBaseButton-secondary"],
    .st-key-city_btn_Granada [data-testid="stBaseButton-primary"],
    .st-key-city_btn_Granada [data-testid="stBaseButton-secondary"] {
        font-family: 'Inter', sans-serif !important;
        font-size: 14px !important;
        font-weight: 600 !important;
        border-radius: 9999px !important;
        padding: 7px 16px !important;
        width: 100% !important;
        transition: all 0.15s !important;
        line-height: 1.2 !important;
        cursor: pointer !important;
    }
    /* Active city: filled indigo pill */
    .st-key-city_btn_Madrid [data-testid="stBaseButton-primary"],
    .st-key-city_btn_Granada [data-testid="stBaseButton-primary"] {
        background: #3525CD !important;
        color: #ffffff !important;
        border: none !important;
        box-shadow: 0 1px 3px rgba(53,37,205,0.25) !important;
    }
    .st-key-city_btn_Madrid [data-testid="stBaseButton-primary"]:hover,
    .st-key-city_btn_Granada [data-testid="stBaseButton-primary"]:hover {
        background: #2a1eb0 !important;
        color: #ffffff !important;
    }
    /* Inactive city: outline pill */
    .st-key-city_btn_Madrid [data-testid="stBaseButton-secondary"],
    .st-key-city_btn_Granada [data-testid="stBaseButton-secondary"] {
        background: transparent !important;
        color: #191C1D !important;
        border: 1px solid #D9DADB !important;
        box-shadow: none !important;
    }
    .st-key-city_btn_Madrid [data-testid="stBaseButton-secondary"]:hover,
    .st-key-city_btn_Granada [data-testid="stBaseButton-secondary"]:hover {
        background: #F3F4F5 !important;
        color: #191C1D !important;
        border: 1px solid #C4C5C6 !important;
    }

    /* ── Filter chips: st.pills ──
       Real DOM: container = data-testid="stButtonGroup",
       inactive pill = data-testid="stBaseButton-pills",
       active pill   = data-testid="stBaseButton-pillsActive". */
    [data-testid="stButtonGroup"] {
        background: #F3F4F5 !important;
        padding: 4px !important;
        border-radius: 9999px !important;
        display: inline-flex !important;
        gap: 2px !important;
        flex-wrap: nowrap !important;
        align-items: center !important;
    }
    /* Inactive pill */
    [data-testid="stBaseButton-pills"] {
        border-radius: 9999px !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 12px !important;
        font-weight: 600 !important;
        padding: 4px 14px !important;
        border: none !important;
        background: transparent !important;
        color: #777587 !important;
        cursor: pointer !important;
        white-space: nowrap !important;
        transition: all 0.12s !important;
    }
    [data-testid="stBaseButton-pills"]:hover {
        background: rgba(0,0,0,0.05) !important;
        color: #191C1D !important;
    }
    /* Active pill — filled indigo */
    [data-testid="stBaseButton-pillsActive"] {
        border-radius: 9999px !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 12px !important;
        font-weight: 700 !important;
        padding: 4px 14px !important;
        border: none !important;
        background: #3525CD !important;
        color: #ffffff !important;
        cursor: pointer !important;
        white-space: nowrap !important;
        box-shadow: 0 1px 3px rgba(53,37,205,0.25) !important;
    }
    [data-testid="stBaseButton-pillsActive"]:hover {
        background: #2a1eb0 !important;
        color: #ffffff !important;
    }

    /* ── Radio fallback (if st.pills not available) ──
       Pill-group appearance with hidden radio circles */
    [data-testid="stRadio"] [role="radiogroup"] {
        display: inline-flex !important;
        background: #F3F4F5 !important;
        border-radius: 9999px !important;
        padding: 3px !important;
        gap: 0 !important;
        flex-wrap: nowrap !important;
        align-items: center !important;
    }
    [data-testid="stRadio"] label {
        display: inline-flex !important;
        align-items: center !important;
        padding: 4px 12px !important;
        border-radius: 9999px !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 12px !important;
        font-weight: 600 !important;
        color: #777587 !important;
        cursor: pointer !important;
        background: transparent !important;
        border: none !important;
        transition: all 0.15s !important;
        user-select: none !important;
        white-space: nowrap !important;
    }
    [data-testid="stRadio"] label p,
    [data-testid="stRadio"] label span {
        font-family: 'Inter', sans-serif !important;
        font-size: 12px !important;
        font-weight: 600 !important;
        margin: 0 !important;
        line-height: 1 !important;
    }
    [data-testid="stRadio"] label:has(input[type="radio"]:checked) {
        background: #ffffff !important;
        color: #191C1D !important;
        font-weight: 700 !important;
        box-shadow: 0 1px 2px rgba(0,0,0,0.06), 0 0 0 0.5px rgba(0,0,0,0.04) !important;
    }
    [data-testid="stRadio"] input[type="radio"] {
        position: absolute !important;
        opacity: 0 !important;
        width: 1px !important;
        height: 1px !important;
        pointer-events: none !important;
    }

    /* ── Suppress Leaflet popup bubble (click detection still works) ── */
    .leaflet-popup-content-wrapper,
    .leaflet-popup-tip,
    .leaflet-popup-tip-container { display: none !important; }

    /* ── Leaflet attribution — discreet ── */
    .leaflet-control-attribution {
        font-size: 9px !important;
        opacity: 0.55 !important;
        background: rgba(255,255,255,0.55) !important;
    }

    /* ── Detail panel — typography ── */
    .bs-neighborhood-header {
        font-size: 2rem;
        font-weight: 900;
        color: #191C1D;
        letter-spacing: -0.02em;
        margin: 0;
        line-height: 1.15;
        display: inline;
    }
    .bs-district {
        font-size: 0.9rem;
        font-weight: 500;
        color: #777587;
        margin: 0;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .bs-refresh-text {
        font-size: 11px;
        font-weight: 500;
        color: #94A3B8;
        margin: 0;
    }

    /* ── Confidence badges ── */
    .bs-badge {
        display: inline-block;
        padding: 0.18rem 0.52rem;
        border-radius: 4px;
        font-size: 0.66rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.07em;
        vertical-align: middle;
        white-space: nowrap;
        line-height: 1.6;
    }
    .bs-badge-high { background: #ACF4A4; color: #307231; }
    .bs-badge-low  { background: #FFF3CD; color: #856404; }

    /* ── Composite score ── */
    .bs-score-num  {
        font-size: 2.8rem;
        font-weight: 900;
        color: #3525CD;
        letter-spacing: -0.03em;
        line-height: 1;
    }
    .bs-score-denom {
        font-size: 1rem;
        font-weight: 500;
        color: #777587;
    }
    .bs-score-caption {
        font-size: 10px;
        font-weight: 700;
        color: #777587;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        margin: 0.2rem 0 0;
        white-space: normal;
        word-break: normal;
    }

    /* ── KPI cards ── */
    .bs-kpi-card {
        background: #ffffff;
        border: 1px solid #E7E8E9;
        border-radius: 8px;
        padding: 1.25rem;
        min-height: 140px;
        display: flex;
        flex-direction: column;
        justify-content: flex-start;
        box-sizing: border-box;
    }
    .bs-kpi-label {
        font-size: 10px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: #777587;
        margin: 0 0 8px;
        line-height: 1.3;
    }
    .bs-kv       { font-size: 1.25rem; font-weight: 800; color: #191C1D; line-height: 1.1; }
    .bs-ku       { font-size: 0.75rem; font-weight: 400; color: #777587; }
    .bs-kv-empty { font-size: 0.8rem; font-style: italic; color: #94A3B8; }

    /* ── Buttons: "Explore →" in Top 5 ── */
    .stButton > button {
        font-family: 'Inter', sans-serif !important;
        font-size: 0.78rem !important;
        padding: 0.3rem 0.6rem !important;
        border-radius: 6px !important;
        border: 1px solid #3525CD !important;
        color: #3525CD !important;
        background: transparent !important;
        transition: background 0.15s, color 0.15s;
    }
    .stButton > button:hover {
        background: #3525CD !important;
        color: #fff !important;
    }

    /* ── Back-to-overview: plain text link, no button appearance ──
       Real DOM: stElementContainer gets class st-key-back_btn. */
    .st-key-back_btn [data-testid="stBaseButton-secondary"] {
        border: none !important;
        background: none !important;
        box-shadow: none !important;
        outline: none !important;
        color: #777587 !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 13px !important;
        font-weight: 500 !important;
        padding: 0 !important;
        margin: 0 0 4px !important;
        min-height: unset !important;
        height: auto !important;
        line-height: 1.5 !important;
        letter-spacing: 0 !important;
        text-decoration: none !important;
    }
    .st-key-back_btn [data-testid="stBaseButton-secondary"]:hover {
        color: #3525CD !important;
        background: none !important;
        border: none !important;
        box-shadow: none !important;
    }
    .st-key-back_btn [data-testid="stBaseButton-secondary"]:focus,
    .st-key-back_btn [data-testid="stBaseButton-secondary"]:active {
        box-shadow: none !important;
        outline: none !important;
        border: none !important;
        background: none !important;
    }

    /* ── Market Inventory section title (with bottom divider) ── */
    .bs-section-title {
        display: block !important;
        font-size: 1.1rem !important;
        font-weight: 700 !important;
        color: #191C1D !important;
        letter-spacing: 0.02em !important;
        margin: 0 0 16px !important;
        padding-bottom: 12px !important;
        border-bottom: 1px solid #E7E8E9 !important;
    }

    /* ── Filter label above pill group ── */
    .bs-filter-label {
        font-size: 10px;
        font-weight: 900;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: #777587;
        margin: 0 0 0.3rem;
    }

    /* ── Listings table ── */
    .bs-lt-wrapper {
        overflow-x: auto;
        border-radius: 8px;
        border: 1px solid #E7E8E9;
    }
    .bs-lt-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 14px;
        font-family: 'Inter', sans-serif;
    }
    .bs-lt-table thead th {
        background: #F8F9FA;
        color: #777587;
        font-size: 10px;
        font-weight: 800;
        text-transform: uppercase;
        letter-spacing: 0.15em;
        padding: 0.55rem 0.75rem;
        text-align: left;
        border-bottom: 1px solid #E7E8E9;
        white-space: nowrap;
        position: sticky;
        top: 0;
        z-index: 1;
    }
    .bs-lt-table thead th.num { text-align: right; }
    .bs-lt-table thead th.ctr { text-align: center; }
    .bs-lt-table tbody tr { border-bottom: 1px solid #F0F1F2; transition: background 0.1s; }
    .bs-lt-table tbody tr:last-child { border-bottom: none; }
    .bs-lt-table tbody tr:hover { background: rgba(243,244,245,0.6); }
    .bs-lt-table td {
        padding: 0.5rem 0.75rem;
        color: #191C1D;
        vertical-align: middle;
        white-space: nowrap;
    }
    .bs-lt-table td.num { text-align: right; }
    .bs-lt-table td.ctr { text-align: center; }
    .bs-lt-price { font-weight: 600; text-align: right; }
    .bs-lt-num   { color: #3D4043; text-align: right; }
    .bs-lt-link a { color: #3525CD; text-decoration: none; font-weight: 500; }
    .bs-lt-link a:hover { text-decoration: underline; }
    .bs-drop-arrow { color: #D32F2F; font-size: 0.72rem; vertical-align: middle; }

    /* Operation badges — explicit !important to prevent any theme override */
    .bs-op-sale, .bs-op-rent, .bs-op-other {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 10px;
        font-weight: 700;
        letter-spacing: 0.05em;
        white-space: nowrap;
    }
    .bs-op-sale  { background: rgba(172,244,164,0.3) !important; color: #2A6B2C !important; }
    .bs-op-rent  { background: rgba(79,70,229,0.1)   !important; color: #3525CD !important; }
    .bs-op-other { background: #F5F5F5; color: #616161; }

    /* ── Radar / score caption ── */
    .bs-score-note {
        font-size: 11px !important;
        font-style: italic !important;
        font-weight: 500 !important;
        color: #94A3B8 !important;
        text-align: center;
        margin-top: -0.4rem;
    }

    /* ── Neighbourhood search selectbox ── */

    /* Outer control: border + magnifying-glass icon as background */
    [data-testid="stSelectbox"] > div > div {
        border-radius: 8px !important;
        border-color: #E7E8E9 !important;
        background-color: #ffffff !important;
        background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='%23C0C5CC' stroke-width='2.5' stroke-linecap='round' stroke-linejoin='round'%3E%3Ccircle cx='11' cy='11' r='8'/%3E%3Cline x1='21' y1='21' x2='16.65' y2='16.65'/%3E%3C/svg%3E") !important;
        background-repeat: no-repeat !important;
        background-position: 12px center !important;
    }
    /* Shift inner content right to clear the icon (overrides BaseWeb's 8 px) */
    [data-testid="stSelectbox"] [data-baseweb="select"] > div:first-child > div:first-child {
        padding-left: 28px !important;
    }

    /* ALWAYS hide the empty-option div[value=""] — no placeholder text ever shown */
    [data-testid="stSelectbox"] [data-baseweb="select"] div[value=""] {
        display: none !important;
    }
    /* Hide any selected-value display when the dropdown is open (user is typing).
       aria-expanded lives on the <input> itself → climb up with :has(). */
    [data-testid="stSelectbox"] [data-baseweb="select"]:has(input[aria-expanded="true"]) div[value] {
        display: none !important;
    }
    /* Selected neighbourhood name: dark text, normal weight */
    [data-testid="stSelectbox"] [data-baseweb="select"] div[value]:not([value=""]) {
        color: #191C1D !important;
        font-style: normal !important;
    }
    /* Input caret + text colour */
    [data-testid="stSelectbox"] input {
        color: #191C1D !important;
        caret-color: #191C1D !important;
    }

    /* ── Top 5 row hover ──
       A sentinel div (.bs-top5-sentinel) is emitted immediately before each
       st.columns() row; the adjacent-sibling + :has() selector then targets
       the resulting stHorizontalBlock for the hover effect. */
    [data-testid="stMarkdownContainer"]:has(> .bs-top5-sentinel)
        + div[data-testid="stHorizontalBlock"] {
        border-radius: 8px;
        transition: background 0.15s ease;
        padding: 2px 6px;
    }
    [data-testid="stMarkdownContainer"]:has(> .bs-top5-sentinel)
        + div[data-testid="stHorizontalBlock"]:hover {
        background: #F3F4F5;
    }

    /* ── Error banner ── */
    .bs-error-box {
        background: #FFF3F3;
        border: 1px solid #FFCDD2;
        border-radius: 8px;
        padding: 1.2rem 1.5rem;
        font-size: 0.9rem;
        color: #C62828;
        margin: 2rem auto;
        max-width: 600px;
        text-align: center;
    }

    /* ── Footer ── */
    .bs-footer {
        position: fixed;
        bottom: 0; left: 0; right: 0;
        background: #F8F9FA;
        border-top: 1px solid #E7E8E9;
        padding: 0.45rem 1.5rem;
        font-size: 11px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: #64748B;
        display: flex;
        align-items: center;
        justify-content: center;
        gap: 1rem;
        z-index: 9999;
    }
    .bs-footer .accent { font-weight: 800; color: #64748B; }
    .bs-footer .sep    { color: #94A3B8; }

    /* ── Score label verbal ── */
    .bs-score-label {
        font-size: 11px;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        margin: 2px 0 0;
        text-align: right;
    }

    /* ── Map legend ── */
    .bs-map-legend {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-top: 4px;
        font-size: 11px;
        color: #64748B;
        font-weight: 500;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Session state defaults ────────────────────────────────────────────────────
if "selected_city" not in st.session_state:
    st.session_state.selected_city = "Madrid"
if "selected_neighborhood_id" not in st.session_state:
    st.session_state.selected_neighborhood_id = None

# active_city is read before the sidebar renders so data can load early.
active_city: str = st.session_state.selected_city

# ── Load data (early — needed to populate municipality list in sidebar) ────────
try:
    with st.spinner(f"Loading {active_city} data…"):
        scores_df = load_neighborhood_scores(metro_area=active_city)
        geojson   = load_neighborhood_geometries(metro_area=active_city)
except Exception as exc:
    st.markdown(
        f"""
        <div class="bs-error-box">
            <strong>⚠️ Could not reach BigQuery</strong><br><br>
            Make sure you are authenticated (<code>gcloud auth application-default login</code>)
            and that the GCP project is accessible.<br><br>
            <span style="font-size:0.8rem; opacity:0.7;">{exc}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.stop()

# ── Sidebar ───────────────────────────────────────────────────────────────────

def _on_city_change() -> None:
    """Reset neighbourhood selection and municipality checkboxes on city change."""
    st.session_state.selected_neighborhood_id = None
    st.session_state.pop("muni_multiselect", None)
    # Drop city-scoped search state so the selectbox resets cleanly
    for city in ("Madrid", "Granada"):
        st.session_state.pop(f"nb_search_{city}", None)
        st.session_state.pop(f"nb_search_prev_{city}", None)


with st.sidebar:
    st.markdown(
        '<p class="bs-logo">BarrioScout</p>'
        '<p class="bs-tagline">Real estate intelligence</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div style="border-top:2px solid #3525CD; margin:0.3rem 0 0.8rem;"></div>',
        unsafe_allow_html=True,
    )

    # City toggle (radio styled as pill group via existing CSS)
    st.radio(
        "Metropolitan area",
        options=["Madrid", "Granada"],
        key="selected_city",
        horizontal=True,
        on_change=_on_city_change,
        label_visibility="collapsed",
        help="Switch between Madrid and Granada metropolitan areas",
    )
    # Re-read in case the radio just updated session state
    active_city = st.session_state.selected_city

    st.divider()

    # Capital section — always visible
    _has_zone_type = "zone_type" in scores_df.columns
    if _has_zone_type:
        _n_capital = int((scores_df["zone_type"] == "capital_neighborhood").sum())
    else:
        _n_capital = len(scores_df)

    st.markdown(
        f"<p style='font-size:0.8rem; font-weight:700; color:#191C1D; margin:0 0 0.2rem;'>"
        f"📍 {active_city} capital</p>"
        f"<p style='font-size:0.75rem; color:#777587; margin:0 0 0.6rem;'>"
        f"{_n_capital} neighbourhoods</p>",
        unsafe_allow_html=True,
    )

    # Metro municipalities section
    selected_municipalities: list[str] = []
    if _has_zone_type:
        _metro_df = (
            scores_df[scores_df["zone_type"] != "capital_neighborhood"]
            .groupby("city", as_index=False)
            .agg(n_zones=("neighborhood_id", "count"))
            .sort_values("city")
        )

        if not _metro_df.empty:
            st.markdown(
                "<p style='font-size:0.8rem; font-weight:700; color:#191C1D; margin:0 0 0.4rem;'>"
                "Metro municipalities</p>",
                unsafe_allow_html=True,
            )

            _all_muni_names = _metro_df["city"].tolist()
            _muni_display = {
                row["city"]: f"{row['city']} ({int(row['n_zones'])})"
                for _, row in _metro_df.iterrows()
            }
            _display_to_city = {v: k for k, v in _muni_display.items()}

            _selected_display = st.multiselect(
                "Metro municipalities",
                options=list(_muni_display.values()),
                default=[],
                placeholder="Type to search…",
                key="muni_multiselect",
                label_visibility="collapsed",
                help="Add metropolitan municipalities to the map. Only capital neighbourhoods are shown by default.",
            )
            selected_municipalities = [_display_to_city[d] for d in _selected_display]

# ── Filter data based on sidebar selection ────────────────────────────────────
_has_zone_type = "zone_type" in scores_df.columns
if _has_zone_type:
    _show_mask = (scores_df["zone_type"] == "capital_neighborhood") | (
        scores_df["city"].isin(selected_municipalities)
    )
    filtered_scores_df = scores_df[_show_mask].copy()
else:
    filtered_scores_df = scores_df.copy()

_visible_ids = set(filtered_scores_df["neighborhood_id"])
filtered_geojson: dict = {
    "type": "FeatureCollection",
    "features": [
        f
        for f in geojson["features"]
        if f["properties"].get("neighborhood_id") in _visible_ids
    ],
}

# ── Sidebar neighbourhood counter (needs filtered_scores_df, computed above) ──
with st.sidebar:
    st.caption(
        f"Showing {len(filtered_scores_df)} of {len(scores_df)} neighbourhoods"
    )

# ── Main area header ──────────────────────────────────────────────────────────
st.markdown(
    '<div style="border-top:2px solid #3525CD; margin:0.2rem 0 0.75rem;"></div>',
    unsafe_allow_html=True,
)

# ── Two-panel layout ──────────────────────────────────────────────────────────
map_col, detail_col = st.columns([58, 42])

# ── Left panel: interactive choropleth map ────────────────────────────────────
with map_col:
    folium_map = create_map(
        geojson=filtered_geojson,
        scores_df=filtered_scores_df,
        selected_neighborhood_id=st.session_state.selected_neighborhood_id,
        metro_area=active_city,
    )

    # Key includes a hash of selected municipalities so the map fully re-initializes
    # (and fit_bounds is re-applied) whenever the selection changes.
    _muni_hash = hashlib.md5(
        "_".join(sorted(selected_municipalities)).encode()
    ).hexdigest()[:8]
    map_data: dict = st_folium(
        folium_map,
        key=f"map_{active_city}_{_muni_hash}",   # key scoped to city+selection
        height=620,
        use_container_width=True,
    )

    # ── Colour-scale legend (replaces branca legend inside the iframe) ─────────
    st.markdown(
        """
        <div style="display:flex; align-items:center; gap:8px; margin-top:4px;
                    padding:0 4px; font-size:11px; color:#64748B;
                    font-family:Inter,sans-serif;">
            <span style="font-weight:600;">Score</span>
            <span>Low</span>
            <div style="width:140px; height:8px; border-radius:4px;
                 background:linear-gradient(to right,
                     #ffffd9, #7fcdbb, #2c7fb8, #253494);"></div>
            <span>High</span>
            <span style="margin-left:8px; color:#C0C5CC;">·</span>
            <span style="color:#C0C5CC;">Grey = insufficient data</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Click handler ─────────────────────────────────────────────────────────
    popup_html: str = (map_data or {}).get("last_object_clicked_popup") or ""
    if popup_html:
        clicked_id = re.sub(r"<[^>]+>", "", popup_html).strip()
        if clicked_id and clicked_id != st.session_state.selected_neighborhood_id:
            st.session_state.selected_neighborhood_id = clicked_id
            st.rerun()

    # ── Hint below the map — only shown when no neighbourhood is selected ──────
    if st.session_state.selected_neighborhood_id is None:
        st.markdown(
            """
            <div style="text-align:center; margin-top:0.4rem;">
                <span style="
                    display:inline-flex; align-items:center; gap:0.45rem;
                    color:#adb5bd; font-size:0.8rem; font-weight:500;
                    letter-spacing:0.01em;
                ">
                    <span style="font-size:1rem; line-height:1;">🖱️</span>
                    Click a neighbourhood on the map to explore it
                </span>
            </div>
            """,
            unsafe_allow_html=True,
        )

# ── Right panel: neighbourhood detail ────────────────────────────────────────
with detail_col:
    # ── Neighbourhood search — always visible above the scrollable panel ──────
    # Build lookup from filtered scores: "Name (District)" → neighborhood_id
    _nb_lookup: dict[str, str] = {
        f"{r['neighborhood_name']} ({r['district_name']})": r["neighborhood_id"]
        for _, r in filtered_scores_df.iterrows()
    }
    _search_key  = f"nb_search_{active_city}"
    _search_prev = f"nb_search_prev_{active_city}"
    if _search_prev not in st.session_state:
        st.session_state[_search_prev] = ""

    # Back-button deferred clear: the detail panel sets this flag because it
    # cannot set a widget's session_state key after that widget has already
    # been rendered in the same script run.  We consume the flag here, before
    # the selectbox is instantiated, so the reset is safe.
    if st.session_state.pop(f"_clear_search_{active_city}", False):
        st.session_state.pop(_search_key, None)   # drop widget state → resets to placeholder
        st.session_state[_search_prev] = ""

    search_result: str = st.selectbox(
        "neighbourhood_search",
        options=[""] + sorted(_nb_lookup.keys()),
        format_func=lambda x: "" if x == "" else x,
        key=_search_key,
        label_visibility="collapsed",
        index=0,
    )

    # Only navigate when the user actively changes the selectbox value — NOT
    # on every rerun.  Comparing to the previous value prevents a loop where
    # a stale search value keeps overriding a map-click or Back-button action.
    if search_result and search_result != st.session_state[_search_prev]:
        st.session_state[_search_prev] = search_result
        _target = _nb_lookup.get(search_result)
        if _target:
            st.session_state.selected_neighborhood_id = _target
            st.rerun()
    if not search_result:
        st.session_state[_search_prev] = ""

    nid = st.session_state.selected_neighborhood_id

    # Scrollable container — matches the map height so the two panels align.
    with st.container(height=640, border=False):
        if nid is None:
            render_default(filtered_scores_df)
        else:
            match_rows = scores_df[scores_df["neighborhood_id"] == nid]
            if match_rows.empty:
                st.info("No score data available for this neighbourhood.")
                render_default(filtered_scores_df)
            else:
                render_detail(match_rows.iloc[0], active_city, scores_df)

# ── Footer ────────────────────────────────────────────────────────────────────
_n_nb   = len(filtered_scores_df)
_n_list = int(filtered_scores_df["total_listings"].sum()) if not filtered_scores_df.empty else 0

st.markdown(
    f"""
    <div class="bs-footer">
        <span>Data refreshed daily</span>
        <span class="sep">·</span>
        <span><span class="accent">{_n_list:,}</span> listings tracked</span>
        <span class="sep">·</span>
        <span><span class="accent">{_n_nb}</span> neighbourhoods scored</span>
        <span class="sep">·</span>
        <span>⚡ Open source</span>
    </div>
    """,
    unsafe_allow_html=True,
)
