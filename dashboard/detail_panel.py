"""Right-panel detail view for BarrioScout.

Exports two public functions:
  render_default(scores_df)            — shown when nothing is selected
  render_detail(row, active_city)      — shown when a neighborhood is selected
"""

from __future__ import annotations

import html as html_module

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from data_loader import load_listings


# ── KPI formatting helpers ────────────────────────────────────────────────────
# Each returns an HTML snippet using .bs-kv / .bs-ku (value/unit) classes.
# When the value is NULL, returns a styled "No data" span instead of a bare "—".

_NO_DATA = "<span class='bs-kv-empty'>No data</span>"


def _kpi_price(val) -> str:
    if pd.isna(val):
        return _NO_DATA
    num = f"€{int(val):,}".replace(",", ".")
    return f"<span class='bs-kv'>{num}</span><span class='bs-ku'>/m²</span>"


def _kpi_int(val) -> str:
    if pd.isna(val):
        return _NO_DATA
    return f"<span class='bs-kv'>{int(val):,}</span>"


def _kpi_year(val) -> str:
    if pd.isna(val):
        return _NO_DATA
    return f"<span class='bs-kv'>{int(val)}</span>"


def _kpi_pois(val) -> str:
    if pd.isna(val):
        return _NO_DATA
    return f"<span class='bs-kv'>{int(val)}</span><span class='bs-ku'>/km²</span>"


# ── Radar chart ───────────────────────────────────────────────────────────────

_RADAR_LABELS = ["Walkability", "Building", "Price", "Yield", "Market"]
_RADAR_COLS = [
    "walkability_score",
    "building_quality_score",
    "price_score",
    "yield_score",
    "market_dynamics_score",
]


def _radar_chart(row: pd.Series) -> go.Figure:
    """Build a Plotly Scatterpolar radar for the five sub-scores."""
    values = [float(row[c]) if pd.notna(row[c]) else 0.0 for c in _RADAR_COLS]
    r     = values + [values[0]]
    theta = _RADAR_LABELS + [_RADAR_LABELS[0]]

    fig = go.Figure()
    fig.add_trace(
        go.Scatterpolar(
            r=r,
            theta=theta,
            fill="toself",
            fillcolor="rgba(53,37,205,0.15)",
            line=dict(color="#3525CD", width=2),
            name="",
            hovertemplate="%{theta}: %{r:.1f}<extra></extra>",
        )
    )
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100],
                gridcolor="#E7E8E9",
                tickfont=dict(size=10, color="#adb5bd"),
                tickvals=[0, 25, 50, 75, 100],
            ),
            angularaxis=dict(
                tickfont=dict(size=12, color="#191C1D", family="Inter, sans-serif"),
                linecolor="#E7E8E9",
            ),
            bgcolor="rgba(0,0,0,0)",
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        # Extra margins so axis labels are never clipped
        margin=dict(l=80, r=80, t=40, b=60),
        height=320,
    )
    return fig


# ── Default state ─────────────────────────────────────────────────────────────

def render_default(scores_df: pd.DataFrame) -> None:
    """Render the empty-state panel with a Top-5 ranking table."""
    top5 = scores_df.dropna(subset=["composite_score"]).head(5)
    if top5.empty:
        return

    st.markdown(
        "<p style='font-size: 10px; font-weight: 900; text-transform: uppercase; "
        "letter-spacing: 0.1em; color: #777587; margin: 0.3rem 0 0.6rem;'>"
        "Top 5 in this city</p>",
        unsafe_allow_html=True,
    )

    for rank, (_, r) in enumerate(top5.iterrows(), start=1):
        score_str = f"{r['composite_score']:.1f}" if pd.notna(r["composite_score"]) else "—"
        conf_dot = (
            '<span style="color:#307231; font-size:0.65rem;">●</span>'
            if (pd.notna(r["data_completeness"]) and float(r["data_completeness"]) >= 0.6)
            else '<span style="color:#856404; font-size:0.65rem;">●</span>'
        )

        # Sentinel div — immediately followed by the stHorizontalBlock so that
        # the CSS :has() adjacent-sibling hover selector in app.py applies.
        st.markdown('<div class="bs-top5-sentinel"></div>', unsafe_allow_html=True)

        col_rank, col_info, col_score, col_btn = st.columns([1, 6, 2, 2])

        with col_rank:
            st.markdown(
                f"<p style='font-size:1rem; font-weight:700; color:#adb5bd; "
                f"margin:0.6rem 0 0; text-align:right;'>{rank}</p>",
                unsafe_allow_html=True,
            )
        with col_info:
            st.markdown(
                f"<p style='font-size:0.88rem; font-weight:600; color:#191C1D; margin:0;'>"
                f"{r['neighborhood_name']} {conf_dot}</p>"
                f"<p style='font-size:0.76rem; color:#6c757d; margin:0;'>{r['district_name']}</p>",
                unsafe_allow_html=True,
            )
        with col_score:
            st.markdown(
                f"<p style='font-size:1.15rem; font-weight:800; color:#3525CD; "
                f"margin:0.35rem 0 0; text-align:right;'>{score_str}"
                f"<span style='font-size:0.72rem; color:#adb5bd; font-weight:400;'>/100</span></p>",
                unsafe_allow_html=True,
            )
        with col_btn:
            if st.button(
                "Explore →",
                key=f"top5_{r['neighborhood_id']}",
                use_container_width=True,
            ):
                st.session_state.selected_neighborhood_id = r["neighborhood_id"]
                st.rerun()

        st.markdown(
            "<hr style='border:none; border-top:1px solid #F0F1F2; margin:0.4rem 0;'>",
            unsafe_allow_html=True,
        )


# ── Detail state ──────────────────────────────────────────────────────────────

def render_detail(row: pd.Series, active_city: str) -> None:
    """Render header, KPI cards, radar chart, and listings for *row*."""
    score        = row["composite_score"]
    completeness = row["data_completeness"]
    score_str    = f"{score:.1f}" if pd.notna(score) else "—"
    is_low_conf  = pd.isna(completeness) or float(completeness) < 0.6

    badge_html = (
        '<span class="bs-badge bs-badge-low">LOW CONFIDENCE</span>'
        if is_low_conf
        else '<span class="bs-badge bs-badge-high">HIGH CONFIDENCE</span>'
    )

    # ── 0. Back button (styled as plain text link via CSS in app.py) ──────────
    if st.button("← Back to overview", key="back_btn"):
        st.session_state.selected_neighborhood_id = None
        # Cannot set the selectbox widget key here — it was already rendered
        # earlier this run and Streamlit would raise StreamlitAPIException.
        # Instead, raise a flag that app.py consumes *before* the selectbox
        # is instantiated on the next rerun.
        st.session_state[f"_clear_search_{active_city}"] = True
        st.rerun()

    # ── 1. Header row ─────────────────────────────────────────────────────────
    hdr_left, hdr_right = st.columns([3, 1.4], vertical_alignment="top")

    with hdr_left:
        st.markdown(
            f"""
            <div style="padding-top: 0.25rem;">
                <div style="display:flex; align-items:center; gap:0.55rem; flex-wrap:wrap;
                            margin-bottom:0.4rem;">
                    <span class="bs-neighborhood-header">{row['neighborhood_name']}</span>
                    {badge_html}
                </div>
                <p class="bs-district">{row['district_name']} · {active_city}</p>
                <p class="bs-refresh-text" style="margin-top:0.2rem;">Data refreshed daily</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with hdr_right:
        st.markdown(
            f"""
            <div style="text-align:right; padding-top:0.15rem; padding-right:4px; line-height:1;">
                <span class="bs-score-num">{score_str}</span>
                <span class="bs-score-denom">/100</span>
                <p class="bs-score-caption" style="text-align:right;">Score</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<div style='margin:1rem 0 0.65rem;'></div>", unsafe_allow_html=True)

    # ── 2. KPI cards ──────────────────────────────────────────────────────────
    kpis = [
        ("Median Price",   _kpi_price(row.get("median_sale_price_m2"))),
        ("Listings",       _kpi_int(row.get("total_listings"))),
        ("Building Stock", _kpi_year(row.get("median_year_built"))),
        ("Walkability",    _kpi_pois(row.get("pois_per_km2"))),
    ]

    for col, (label, value_html) in zip(st.columns(4), kpis):
        with col:
            st.markdown(
                f"""
                <div class="bs-kpi-card">
                    <div class="bs-kpi-label">{label}</div>
                    <div>{value_html}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown("<div style='margin:0.9rem 0 0;'></div>", unsafe_allow_html=True)

    # ── 3. Radar chart ────────────────────────────────────────────────────────
    st.plotly_chart(
        _radar_chart(row),
        use_container_width=True,
        config={"displayModeBar": False},
    )
    # Caption: must be muted gray — color enforced with !important in app.py CSS
    st.markdown(
        "<p class='bs-score-note'>"
        "Scores: 0–100 percentile rank within city · Higher = better opportunity"
        "</p>",
        unsafe_allow_html=True,
    )

    # ── 4. Market inventory ───────────────────────────────────────────────────
    render_listings_section(row)


# ── Listings section ──────────────────────────────────────────────────────────

def _eu_price(val, suffix: str = " €") -> str:
    """European dot-thousands format: 245000 → '245.000 €'."""
    if pd.isna(val):
        return "—"
    return f"{int(val):,}{suffix}".replace(",", ".")


def _eu_price_m2(val) -> str:
    return _eu_price(val, " €/m²")


def _fmt_area(val) -> str:
    if pd.isna(val):
        return "—"
    return f"{int(val)} m²"


def _fmt_beds(val) -> str:
    if pd.isna(val):
        return "—"
    return str(int(val))


def _est_yield(price, area_m2, median_rent_price_m2) -> str:
    """Gross yield estimate for a SALE listing using neighbourhood median rent."""
    try:
        if any(pd.isna(v) for v in [price, area_m2, median_rent_price_m2]):
            return "—"
        if float(price) <= 0 or float(area_m2) <= 0:
            return "—"
        pct = float(median_rent_price_m2) * 12 * float(area_m2) / float(price) * 100
        return f"{pct:.1f}%"
    except Exception:
        return "—"


def _op_badge(op: str) -> str:
    op_lower = str(op).lower()
    if op_lower == "sale":
        return '<span class="bs-op-sale">SALE</span>'
    if op_lower == "rent":
        return '<span class="bs-op-rent">RENT</span>'
    return f'<span class="bs-op-other">{html_module.escape(op.upper())}</span>'


def _build_listings_html(df: pd.DataFrame, median_rent_price_m2) -> str:
    """Build the full HTML table for the listings."""
    rows_html = []
    for _, r in df.iterrows():
        op_lower = str(r["operation_type"]).lower()

        # Price drop: BQ BOOL lands as numpy bool_ or float NaN
        _hpd = r.get("has_price_drop")
        has_drop = pd.notna(_hpd) and bool(_hpd)

        price_raw = _eu_price(r["price"])
        if has_drop:
            price_cell = (
                f'<td class="bs-lt-price">'
                f'{price_raw} <span class="bs-drop-arrow">▼</span>'
                f'</td>'
            )
        else:
            price_cell = f'<td class="bs-lt-price">{price_raw}</td>'

        yield_str = (
            _est_yield(r["price"], r["area_m2"], median_rent_price_m2)
            if op_lower == "sale"
            else "—"
        )

        url = r.get("property_url")
        if pd.notna(url) and str(url).startswith("http"):
            link_cell = (
                f'<td class="bs-lt-link">'
                f'<a href="{html_module.escape(str(url))}" target="_blank" '
                f'rel="noopener noreferrer">View →</a>'
                f'</td>'
            )
        else:
            link_cell = '<td class="bs-lt-link">—</td>'

        rows_html.append(
            f"<tr>"
            f"{link_cell}"
            f"{price_cell}"
            f'<td class="bs-lt-num">{_fmt_area(r["area_m2"])}</td>'
            f'<td class="bs-lt-num">{_eu_price_m2(r["price_per_m2"])}</td>'
            f'<td class="bs-lt-num">{_fmt_beds(r["bedrooms"])}</td>'
            f'<td class="ctr">{_op_badge(r["operation_type"])}</td>'
            f'<td class="bs-lt-num">{yield_str}</td>'
            f"</tr>"
        )

    rows = "\n".join(rows_html)
    return f"""
<div class="bs-lt-wrapper">
<table class="bs-lt-table">
  <thead>
    <tr>
      <th>Link</th>
      <th class="num">Price</th>
      <th class="num">Area</th>
      <th class="num">€/m²</th>
      <th class="num">Beds</th>
      <th class="ctr">Type</th>
      <th class="num">Est. Yield</th>
    </tr>
  </thead>
  <tbody>
    {rows}
  </tbody>
</table>
</div>
"""


def render_listings_section(neighborhood_row: pd.Series) -> None:
    """Render the Market Inventory section below the radar chart."""
    nid        = neighborhood_row["neighborhood_id"]
    nname      = neighborhood_row["neighborhood_name"]
    median_rent = neighborhood_row.get("median_rent_price_m2")

    # Section title with bottom divider (enforced via CSS .bs-section-title)
    st.markdown(
        "<p class='bs-section-title'>Market Inventory</p>",
        unsafe_allow_html=True,
    )

    # ── Filters ───────────────────────────────────────────────────────────────
    # Use st.pills if available (Streamlit ≥ 1.40); fall back to styled radio.
    _use_pills = hasattr(st, "pills")

    f_left, f_right = st.columns(2)
    with f_left:
        st.markdown('<p class="bs-filter-label">Operation</p>', unsafe_allow_html=True)
        if _use_pills:
            op_filter = st.pills(
                "Operation",
                options=["All", "Sale", "Rent"],
                default="All",
                key=f"lt_op_{nid}",
                label_visibility="collapsed",
            ) or "All"
        else:
            op_filter = st.radio(
                "Operation",
                options=["All", "Sale", "Rent"],
                horizontal=True,
                key=f"lt_op_{nid}",
                label_visibility="collapsed",
            )

    with f_right:
        st.markdown('<p class="bs-filter-label">Beds</p>', unsafe_allow_html=True)
        if _use_pills:
            bed_filter = st.pills(
                "Bedrooms",
                options=["All", "1", "2", "3+"],
                default="All",
                key=f"lt_bed_{nid}",
                label_visibility="collapsed",
            ) or "All"
        else:
            bed_filter = st.radio(
                "Bedrooms",
                options=["All", "1", "2", "3+"],
                horizontal=True,
                key=f"lt_bed_{nid}",
                label_visibility="collapsed",
            )

    # ── Load ──────────────────────────────────────────────────────────────────
    try:
        with st.spinner("Loading listings…"):
            df = load_listings(nid)
    except Exception as exc:
        st.markdown(
            f"<p style='color:#C62828; font-size:0.85rem; margin:0.5rem 0;'>"
            f"⚠️ Could not load listings: {exc}</p>",
            unsafe_allow_html=True,
        )
        return

    total_count = len(df)

    if df.empty:
        st.markdown(
            "<p style='color:#adb5bd; font-size:0.88rem; margin:1rem 0;'>"
            "No listings found in this neighbourhood.</p>",
            unsafe_allow_html=True,
        )
        return

    # ── Filter ────────────────────────────────────────────────────────────────
    if op_filter != "All":
        df = df[df["operation_type"].str.lower() == op_filter.lower()]

    if bed_filter == "1":
        df = df[df["bedrooms"] == 1]
    elif bed_filter == "2":
        df = df[df["bedrooms"] == 2]
    elif bed_filter == "3+":
        df = df[df["bedrooms"].notna() & (df["bedrooms"] >= 3)]

    df = df.sort_values("price", ascending=True, na_position="last")

    if df.empty:
        st.markdown(
            "<p style='color:#adb5bd; font-size:0.88rem; margin:1rem 0;'>"
            "No listings match your filters.</p>",
            unsafe_allow_html=True,
        )
        return

    # ── Table ─────────────────────────────────────────────────────────────────
    st.markdown(_build_listings_html(df, median_rent), unsafe_allow_html=True)

    # ── Count line ────────────────────────────────────────────────────────────
    shown = len(df)
    note = (
        f"Showing {shown} of {total_count} listings in "
        f"<strong>{html_module.escape(nname)}</strong>"
        if shown < total_count
        else f"All {total_count} listings in "
             f"<strong>{html_module.escape(nname)}</strong>"
    )
    st.markdown(
        f"<p style='font-size:0.77rem; color:#adb5bd; margin:0.6rem 0 1rem;'>"
        f"{note} · sorted by price ascending</p>",
        unsafe_allow_html=True,
    )
