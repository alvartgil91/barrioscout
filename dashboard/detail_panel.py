"""Right-panel detail view for BarrioScout.

Exports two public functions:
  render_default(scores_df)                       — shown when nothing is selected
  render_detail(row, active_city, scores_df)      — shown when a zone is selected
"""

from __future__ import annotations

import html as html_module
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from data_loader import load_listings


# ── Zone type badge ───────────────────────────────────────────────────────────

def _zone_badge(zone_type: Optional[str]) -> str:
    """Return an inline HTML badge for the zone type."""
    cfg: dict[str, tuple[str, str, str]] = {
        "capital_neighborhood": ("Neighbourhood", "#1a9850", "#e8f5e9"),
        "metro_neighborhood":   ("Metro zone",    "#1565c0", "#e3f2fd"),
        "metro_municipality":   ("Municipality",  "#616161", "#f5f5f5"),
    }
    label, fg, bg = cfg.get(zone_type or "", ("", "#333", "#eee"))
    if not label:
        return ""
    return (
        f'<span style="background:{bg};color:{fg};padding:2px 8px;'
        f'border-radius:4px;font-size:12px;font-weight:500">{label}</span>'
    )


# ── Ranking ───────────────────────────────────────────────────────────────────

def _get_rank(row: pd.Series, scores_df: pd.DataFrame) -> Optional[tuple[int, int]]:
    """Return (rank, total) for the zone within its city, or None if not rankable.

    metro_municipality zones are always rank 1 of 1 within their own city, so
    ranking is omitted for them.  Zones with no composite_score are also skipped.
    """
    zone_type = row.get("zone_type")
    if zone_type == "metro_municipality":
        return None
    if pd.isna(row.get("composite_score")):
        return None

    city_scored = (
        scores_df[
            (scores_df["city"] == row["city"]) & scores_df["composite_score"].notna()
        ]
        .sort_values("composite_score", ascending=False)
        .reset_index(drop=True)
    )
    if city_scored.empty:
        return None

    matches = city_scored[city_scored["neighborhood_id"] == row["neighborhood_id"]]
    if matches.empty:
        return None

    return int(matches.index[0]) + 1, len(city_scored)


# ── KPI formatting helpers ────────────────────────────────────────────────────

_NO_DATA = "<span class='bs-kv-empty'>No data</span>"


def _score_label(score: float) -> tuple[str, str]:
    """Return (label, color_hex) for a composite score."""
    if score >= 80: return "Excellent", "#1a9850"
    if score >= 65: return "Very Good", "#2a6b2c"
    if score >= 50: return "Good", "#3525CD"
    if score >= 35: return "Fair", "#e67e22"
    return "Needs Improvement", "#c0392b"


def _kpi_price(val) -> str:
    if pd.isna(val):
        return _NO_DATA
    num = f"{int(val):,}".replace(",", ".")
    return (
        f'<div style="white-space:nowrap;">'
        f"<span class='bs-kv'>{num}</span>"
        f"<span class='bs-ku'> €/m²</span>"
        f"</div>"
    )


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
    return f"<span class='bs-kv'>{int(val)}</span><span class='bs-ku'> per km²</span>"


def _kpi_listings(total, sale, rent) -> str:
    if pd.isna(total):
        return _NO_DATA
    s = int(sale) if pd.notna(sale) else 0
    r = int(rent) if pd.notna(rent) else 0
    _sale_badge = (
        '<span style="background:#E8F5E9; color:#2E7D32; font-size:9px; font-weight:700; '
        'padding:2px 6px; border-radius:3px; white-space:nowrap;">SALE</span>'
    )
    _rent_badge = (
        '<span style="background:#E3F2FD; color:#1565C0; font-size:9px; font-weight:700; '
        'padding:2px 6px; border-radius:3px; white-space:nowrap;">RENT</span>'
    )
    return (
        f"<span class='bs-kv'>{int(total)}</span>"
        f"<span style='display:block; margin-top:4px; white-space:nowrap;'>"
        f"{_sale_badge}"
        f"<span style='font-size:11px; color:#64748B;'> {s}</span>"
        f"<span style='margin-left:6px;'>{_rent_badge}</span>"
        f"<span style='font-size:11px; color:#64748B;'> {r}</span>"
        f"</span>"
    )


def _kpi_area(val) -> str:
    if pd.isna(val):
        return _NO_DATA
    return f"<span class='bs-kv'>{float(val):.1f}</span><span class='bs-ku'> km²</span>"


# ── Radar chart ───────────────────────────────────────────────────────────────

_RADAR_LABELS = ["Services", "Building", "Price", "Yield", "Market"]
_RADAR_COLS = [
    "services_score",
    "building_quality_score",
    "price_score",
    "yield_score",
    "market_dynamics_score",
]
_RADAR_DESCRIPTIONS = [
    "Nearby amenities: transport, healthcare, education & retail. Higher = better served.",
    "Share of buildings built after 2000. Higher = newer housing stock.",
    "Affordability: median price/m². Higher = lower prices vs. the area.",
    "Gross annual rental yield. Higher = better estimated return.",
    "Market liquidity: listing density & price drops. Higher = more activity & motivated sellers.",
]


def _radar_chart(row: pd.Series) -> Optional[go.Figure]:
    """Build a Plotly Scatterpolar radar.  Returns None if all values are zero/null."""
    values = [float(row[c]) if pd.notna(row[c]) else 0.0 for c in _RADAR_COLS]
    if all(v == 0.0 for v in values):
        return None

    r      = values + [values[0]]
    theta  = _RADAR_LABELS + [_RADAR_LABELS[0]]
    descs  = _RADAR_DESCRIPTIONS + [_RADAR_DESCRIPTIONS[0]]
    customdata = [[d] for d in descs]

    fig = go.Figure()
    fig.add_trace(
        go.Scatterpolar(
            r=r,
            theta=theta,
            fill="toself",
            fillcolor="rgba(53,37,205,0.15)",
            line=dict(color="#3525CD", width=2),
            mode="lines+markers",
            marker=dict(size=12, opacity=0),
            name="",
            customdata=customdata,
            hovertemplate=(
                "<b>%{theta}</b>: %{r:.1f}/100"
                "<br><span style='color:#777;font-size:11px'>%{customdata[0]}</span>"
                "<extra></extra>"
            ),
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
                tickfont=dict(size=11, color="#191C1D", family="Inter, sans-serif"),
                linecolor="#E7E8E9",
            ),
            bgcolor="rgba(0,0,0,0)",
        ),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        hoverdistance=100,
        margin=dict(l=60, r=60, t=30, b=30),
        height=290,
    )
    return fig


# ── Sub-score progress bars ───────────────────────────────────────────────────

_SCORE_COMPONENTS: list[tuple[str, str, int]] = [
    ("Services", "services_score",          30),
    ("Building", "building_quality_score",  20),
    ("Price",    "price_score",             20),
    ("Yield",    "yield_score",             20),
    ("Market",   "market_dynamics_score",   10),
]


def _render_score_bars(row: pd.Series) -> None:
    """Render horizontal progress bars for each sub-score with its weight."""
    bars_html_parts = []
    for label, col, weight in _SCORE_COMPONENTS:
        raw = row.get(col)
        has_val = pd.notna(raw)
        val = float(raw) if has_val else 0.0
        val_display = f"{val:.0f}" if has_val else "—"
        fill_pct    = f"{val:.2f}%" if has_val else "0%"
        fill_color  = "#3525CD" if has_val else "#E0E0E0"
        opacity     = "1" if has_val else "0.4"
        bars_html_parts.append(
            f'<div style="display:flex; align-items:center; gap:8px; '
            f'margin-bottom:6px; opacity:{opacity};">'
            f'<span style="font-size:11px; font-weight:600; color:#64748B; '
            f'width:90px; flex-shrink:0;">{label} {weight}%</span>'
            f'<div style="flex:1; height:6px; background:#F0F1F2; '
            f'border-radius:3px; overflow:hidden;">'
            f'<div style="height:100%; width:{fill_pct}; background:{fill_color}; '
            f'border-radius:3px;"></div></div>'
            f'<span style="font-size:12px; font-weight:700; color:#191C1D; '
            f'width:36px; text-align:right; flex-shrink:0;">{val_display}</span>'
            f'</div>'
        )
    st.markdown("\n".join(bars_html_parts), unsafe_allow_html=True)


# ── Default state ─────────────────────────────────────────────────────────────

def render_default(scores_df: pd.DataFrame) -> None:
    """Render the empty-state panel with a Top-5 ranking of capital neighbourhoods."""
    # Only capital_neighborhood rows are meaningfully comparable at a glance.
    if "zone_type" in scores_df.columns:
        pool = scores_df[scores_df["zone_type"] == "capital_neighborhood"]
    else:
        pool = scores_df
    top5 = (
        pool
        .dropna(subset=["composite_score"])
        .sort_values("composite_score", ascending=False)
        .head(5)
    )
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

        # Sentinel div — triggers the CSS :has() hover selector in app.py.
        st.markdown('<div class="bs-top5-sentinel"></div>', unsafe_allow_html=True)

        col_rank, col_info, col_score, col_btn = st.columns([1, 6, 2.5, 1.5])

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
                "→",
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

def render_detail(row: pd.Series, active_city: str, scores_df: pd.DataFrame) -> None:
    """Render header, KPI cards, radar chart, and listings for *row*.

    Args:
        row:         One row from the scores DataFrame (the selected zone).
        active_city: Metro area name ("Madrid" or "Granada").
        scores_df:   Full (unfiltered) scores for the current metro area, used
                     to compute the zone's rank within its city.
    """
    score        = row["composite_score"]
    completeness = row["data_completeness"]
    score_str    = f"{score:.1f}" if pd.notna(score) else "—"
    is_low_conf  = pd.isna(completeness) or float(completeness) < 0.6
    zone_type    = str(row.get("zone_type") or "capital_neighborhood")

    conf_badge_html = (
        '<span class="bs-badge bs-badge-low">LOW CONFIDENCE</span>'
        if is_low_conf
        else '<span class="bs-badge bs-badge-high">HIGH CONFIDENCE</span>'
    )

    # ── 0. Back button ────────────────────────────────────────────────────────
    if st.button("← Back to overview", key="back_btn"):
        st.session_state.selected_neighborhood_id = None
        st.session_state[f"_clear_search_{active_city}"] = True
        st.rerun()

    # ── 1. Header ─────────────────────────────────────────────────────────────
    hdr_left, hdr_right = st.columns([3, 1.4], vertical_alignment="top")

    # Subtitle: depends on zone_type
    district = html_module.escape(str(row.get("district_name") or ""))
    city     = html_module.escape(str(row.get("city") or active_city))
    city_esc = html_module.escape(active_city)

    if zone_type == "capital_neighborhood":
        subtitle = f"{district} · {city_esc}"
    elif zone_type == "metro_neighborhood":
        subtitle = f"📍 {city} · {city_esc} metro"
    else:
        subtitle = f"{city_esc} metro area"

    with hdr_left:
        st.markdown(
            f"""
            <div style="padding-top: 0.25rem;">
                <div style="display:flex; align-items:center; gap:0.55rem; flex-wrap:wrap;
                            margin-bottom:0.35rem;">
                    <span class="bs-neighborhood-header">
                        {html_module.escape(str(row['neighborhood_name']))}
                    </span>
                    {_zone_badge(zone_type)}
                    {conf_badge_html}
                </div>
                <p class="bs-district">{subtitle}</p>
                <p class="bs-refresh-text" style="margin-top:0.2rem;">Data refreshed daily</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with hdr_right:
        _label_html = ""
        if pd.notna(score):
            _label, _label_color = _score_label(float(score))
            _label_html = (
                f'<p style="font-size:11px; color:{_label_color}; font-weight:700; '
                f'text-transform:uppercase; letter-spacing:0.08em; margin:2px 0 0; '
                f'text-align:right;">{_label}</p>'
            )
        st.markdown(
            f"""
            <div style="text-align:right; padding-top:0.15rem; padding-right:4px; line-height:1;">
                <span class="bs-score-num">{score_str}</span>
                <span class="bs-score-denom">/100</span>
                <p class="bs-score-caption" style="text-align:right;">Score</p>
                {_label_html}
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ── Rank line ─────────────────────────────────────────────────────────────
    rank_result = _get_rank(row, scores_df)
    if rank_result:
        rank, total = rank_result
        zone_label = "neighbourhood" if zone_type == "capital_neighborhood" else "zone"
        st.markdown(
            f"<p style='font-size:0.75rem; color:#777587; margin:0.1rem 0 0;'>"
            f"Rank <strong>#{rank}</strong> of {total} {zone_label}s in {city}</p>",
            unsafe_allow_html=True,
        )
    elif zone_type == "metro_municipality":
        st.markdown(
            "<p style='font-size:0.75rem; color:#adb5bd; margin:0.1rem 0 0;'>"
            "Undivided municipality · not comparable with capital rankings</p>",
            unsafe_allow_html=True,
        )

    st.markdown("<div style='margin:0.8rem 0 0.5rem;'></div>", unsafe_allow_html=True)

    # ── 2. KPI cards ──────────────────────────────────────────────────────────
    listings_html = _kpi_listings(
        row.get("total_listings"),
        row.get("sale_count"),
        row.get("rent_count"),
    )

    if zone_type in ("capital_neighborhood", "metro_neighborhood"):
        kpis = [
            ("Price / m²",  _kpi_price(row.get("median_sale_price_m2"))),
            ("Listings",    listings_html),
            ("Buildings",   _kpi_year(row.get("median_year_built"))),
            ("Services",    _kpi_pois(row.get("pois_per_km2"))),
        ]
    else:  # metro_municipality
        kpis = [
            ("Services",  _kpi_pois(row.get("pois_per_km2"))),
            ("Listings",  listings_html),
            ("Buildings", _kpi_year(row.get("median_year_built"))),
            ("Area",      _kpi_area(row.get("area_km2"))),
        ]

    for col, (label, value_html) in zip(st.columns(4), kpis):
        with col:
            st.markdown(
                f'<div class="bs-kpi-card">'
                f'<div class="bs-kpi-label">{label}</div>'
                f'<div>{value_html}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    if zone_type == "metro_municipality":
        st.markdown(
            "<p style='font-size:0.78rem; color:#adb5bd; margin:0.5rem 0 0; font-style:italic;'>"
            "This municipality has not been subdivided into zones. "
            "Scores are not comparable with capital neighbourhood rankings.</p>",
            unsafe_allow_html=True,
        )

    st.markdown("<div style='margin:0.6rem 0 0;'></div>", unsafe_allow_html=True)

    # ── 3. Radar chart (visual) + score breakdown expander ───────────────────
    _n_scores = sum(1 for col in _RADAR_COLS if pd.notna(row.get(col)))

    if _n_scores >= 3:
        radar = _radar_chart(row)
        if radar is not None:
            st.plotly_chart(radar, use_container_width=True, config={"displayModeBar": False})
            st.markdown(
                "<p class='bs-score-note'>"
                "Scores: 0–100 percentile rank within city · Hover for details"
                "</p>",
                unsafe_allow_html=True,
            )
    else:
        st.markdown(
            f"""
            <div style="text-align:center; padding:32px 16px; color:#94A3B8; font-size:13px;">
                <div style="font-size:24px; margin-bottom:8px;">📊</div>
                Radar chart requires at least 3 sub-scores.<br>
                This neighbourhood has {_n_scores} of 5.
            </div>
            """,
            unsafe_allow_html=True,
        )

    with st.expander("Score breakdown", expanded=False):
        _render_score_bars(row)

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
    _style = (
        "font-size:9px; font-weight:700; padding:2px 5px; border-radius:3px; "
        "white-space:nowrap; letter-spacing:0.04em;"
    )
    if op_lower == "sale":
        return (
            f'<span style="{_style} background:#E8F5E9; color:#2E7D32;">SALE</span>'
        )
    if op_lower == "rent":
        return (
            f'<span style="{_style} background:#E3F2FD; color:#1565C0;">RENT</span>'
        )
    return (
        f'<span style="{_style} background:#F5F5F5; color:#616161;">'
        f'{html_module.escape(op.upper())}</span>'
    )


def _build_listings_html(df: pd.DataFrame, median_rent_price_m2) -> str:
    """Build the full HTML table for the listings."""
    rows_html = []
    for _, r in df.iterrows():
        op_lower = str(r["operation_type"]).lower()

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
      <th class="ctr"></th>
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
    nid         = neighborhood_row["neighborhood_id"]
    nname       = neighborhood_row["neighborhood_name"]
    median_rent = neighborhood_row.get("median_rent_price_m2")
    zone_type   = str(neighborhood_row.get("zone_type") or "capital_neighborhood")

    st.markdown("<p class='bs-section-title'>Market Inventory</p>", unsafe_allow_html=True)

    if zone_type == "metro_municipality":
        st.markdown(
            f"<p style='font-size:0.78rem; color:#adb5bd; margin:0 0 0.6rem; "
            f"font-style:italic;'>Showing all listings tracked in "
            f"{html_module.escape(str(nname))}</p>",
            unsafe_allow_html=True,
        )

    # ── Filters ───────────────────────────────────────────────────────────────
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

    st.markdown('<p class="bs-filter-label" style="margin-top:6px;">Sort by</p>', unsafe_allow_html=True)
    _sort_options = ["Price ↑", "Price ↓", "€/m² ↑", "€/m² ↓", "Area ↓"]
    sort_by = st.selectbox(
        "Sort by",
        options=_sort_options,
        index=0,
        key=f"lt_sort_{nid}",
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

    # Drop confirmed-inactive listings.
    if "current_status" in df.columns:
        df = df[df["current_status"] != "INACTIVE"].copy()

    total_count = len(df)

    if df.empty:
        st.markdown(
            "<p style='color:#adb5bd; font-size:0.88rem; margin:1rem 0;'>"
            "No active listings found in this zone.</p>",
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

    _sort_map = {
        "Price ↑":  ("price",        True),
        "Price ↓":  ("price",        False),
        "€/m² ↑":  ("price_per_m2", True),
        "€/m² ↓":  ("price_per_m2", False),
        "Area ↓":   ("area_m2",      False),
    }
    _sort_col, _sort_asc = _sort_map.get(sort_by, ("price", True))
    df = df.sort_values(_sort_col, ascending=_sort_asc, na_position="last")

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
        f"Showing {shown} of {total_count} active listings in "
        f"<strong>{html_module.escape(str(nname))}</strong>"
        if shown < total_count
        else f"All {total_count} active listings in "
             f"<strong>{html_module.escape(str(nname))}</strong>"
    )
    st.markdown(
        f"<p style='font-size:0.77rem; color:#adb5bd; margin:0.6rem 0 1rem;'>"
        f"{note} · sorted by {sort_by}</p>",
        unsafe_allow_html=True,
    )
