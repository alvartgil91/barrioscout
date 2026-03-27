"""Folium map component for the BarrioScout dashboard.

Renders neighbourhood polygons coloured by composite_score with an optional
selected-neighbourhood highlight.
"""

import copy
from typing import Optional

import folium
import pandas as pd

try:
    from branca.element import Element as _BrancaElement
except ImportError:  # pragma: no cover
    _BrancaElement = None  # type: ignore

try:
    import branca.colormap as _branca_cm
    _COLORMAP = _branca_cm.LinearColormap(
        colors=["#d73027", "#fc8d59", "#fee08b", "#d9ef8b", "#91cf60", "#1a9850"],
        vmin=0,
        vmax=100,
        caption="Composite Score (0–100)",
    )
except ImportError:  # pragma: no cover
    _COLORMAP = None  # type: ignore

# Fallback bands (used only if branca unavailable)
_SCORE_BANDS: list[tuple[float, str]] = [
    (80, "#1B5E20"),
    (60, "#2A6B2C"),
    (40, "#66BB6A"),
    (20, "#A5D6A7"),
    (0,  "#C8E6C9"),
]
_LOW_CONFIDENCE_FILL      = "#E0E0E0"
_SELECTED_BORDER          = "#3525CD"
_DEFAULT_BORDER           = "#FFFFFF"
_LOW_CONFIDENCE_THRESHOLD = 0.6

# Fixed map centers per metro area — prevents outlier polygons from
# pulling the viewport away from the city.
_CITY_CENTERS: dict[str, tuple[float, float]] = {
    "Madrid":  (40.4168, -3.7038),
    "Granada": (37.1773, -3.5986),
}

_CARTO_POSITRON = (
    "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png"
)
_CARTO_ATTRIBUTION = (
    '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> '
    'contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
)

# CSS injected into the Folium map iframe to style Leaflet tooltips.
# nth-child row selectors style: (1) neighbourhood name, (2) score,
# (3) "Click to explore" hint.
_TOOLTIP_CSS = """
<style>
.leaflet-tooltip {
    font-family: 'Inter', sans-serif !important;
    font-size: 13px !important;
    font-weight: 600 !important;
    color: #191C1D !important;
    background: #ffffff !important;
    border: 1px solid #E7E8E9 !important;
    border-radius: 8px !important;
    padding: 8px 12px !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.10) !important;
    min-width: 160px;
}
.leaflet-tooltip table { border-collapse: collapse; width: 100%; }
.leaflet-tooltip td { padding: 1px 0; }
/* Row 1: neighbourhood name — large bold */
.leaflet-tooltip tr:nth-child(1) td {
    font-weight: 700 !important;
    font-size: 13px !important;
    color: #191C1D !important;
    padding-bottom: 3px !important;
}
/* Row 2: score value — indigo */
.leaflet-tooltip tr:nth-child(2) td {
    font-weight: 700 !important;
    font-size: 12px !important;
    color: #3525CD !important;
}
/* Row 3: "Click to explore" hint — muted gray, small */
.leaflet-tooltip tr:nth-child(3) td {
    font-size: 11px !important;
    color: #94A3B8 !important;
    font-weight: 400 !important;
    padding-top: 4px !important;
}
.leaflet-tooltip-left::before,
.leaflet-tooltip-right::before {
    border-right-color: #E7E8E9 !important;
    border-left-color: #E7E8E9 !important;
}
</style>
"""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _score_to_color(score: Optional[float]) -> str:
    """Map a composite_score (0–100 or None) to a fill hex colour."""
    if score is None or (isinstance(score, float) and pd.isna(score)):
        return _LOW_CONFIDENCE_FILL
    if _COLORMAP is not None:
        return _COLORMAP(max(0.0, min(100.0, float(score))))
    for threshold, color in _SCORE_BANDS:
        if score >= threshold:
            return color
    return _SCORE_BANDS[-1][1]


def _extract_all_coords(geometry: dict) -> list[list[float]]:
    """Return every [lon, lat] coordinate pair from a GeoJSON geometry dict.

    Handles Point, MultiPoint, LineString, MultiLineString, Polygon,
    MultiPolygon, and GeometryCollection.
    """
    gtype = geometry.get("type", "")
    raw = geometry.get("coordinates")
    coords: list[list[float]] = []

    if raw is None:
        # GeometryCollection
        for geom in geometry.get("geometries", []):
            coords.extend(_extract_all_coords(geom))
        return coords

    if gtype == "Point":
        coords.append(raw)
    elif gtype in ("MultiPoint", "LineString"):
        coords.extend(raw)
    elif gtype in ("MultiLineString", "Polygon"):
        for ring in raw:
            coords.extend(ring)
    elif gtype == "MultiPolygon":
        for polygon in raw:
            for ring in polygon:
                coords.extend(ring)

    return coords


def _compute_bounds(geojson: dict) -> tuple[list[float], list[float]]:
    """Return ([min_lat, min_lon], [max_lat, max_lon]) for a FeatureCollection."""
    all_coords: list[list[float]] = []
    for feature in geojson.get("features", []):
        geometry = feature.get("geometry")
        if geometry:
            all_coords.extend(_extract_all_coords(geometry))

    if not all_coords:
        # Fallback: centre of Spain
        return ([36.0, -9.5], [44.0, 4.5])

    lons = [c[0] for c in all_coords]
    lats = [c[1] for c in all_coords]
    return ([min(lats), min(lons)], [max(lats), max(lons)])


def _enrich_geojson(geojson: dict, scores_df: pd.DataFrame) -> dict:
    """Return a deep-copied FeatureCollection with score columns merged into
    each feature's properties.

    Merges on neighborhood_id.  Also adds ``score_display`` (pre-formatted
    string for the tooltip, e.g. "67.3 / 100") and ``click_hint`` (static
    "Click to explore" text for the tooltip's third row).
    """
    score_lookup: dict[str, dict] = {}
    if not scores_df.empty:
        for _, row in scores_df.iterrows():
            score_lookup[row["neighborhood_id"]] = {
                "composite_score": (
                    None if pd.isna(row["composite_score"]) else float(row["composite_score"])
                ),
                "data_completeness": (
                    None
                    if pd.isna(row["data_completeness"])
                    else float(row["data_completeness"])
                ),
                "available_sub_scores": (
                    None
                    if pd.isna(row["available_sub_scores"])
                    else int(row["available_sub_scores"])
                ),
                "zone_type": row.get("zone_type"),
                "city": row.get("city"),
            }

    enriched = copy.deepcopy(geojson)
    for feature in enriched.get("features", []):
        nid    = feature["properties"].get("neighborhood_id")
        scores = score_lookup.get(nid, {})
        score_val = scores.get("composite_score")

        zone_type = scores.get("zone_type")
        city      = scores.get("city") or feature["properties"].get("city", "")

        feature["properties"]["composite_score"]      = score_val
        feature["properties"]["data_completeness"]    = scores.get("data_completeness")
        feature["properties"]["available_sub_scores"] = scores.get("available_sub_scores")
        feature["properties"]["zone_type"]            = zone_type

        # Tooltip display fields
        score_str = f"{score_val:.1f} / 100" if score_val is not None else "—"
        if zone_type and zone_type != "capital_neighborhood" and city:
            feature["properties"]["score_display"] = f"{city} · {score_str}"
        else:
            feature["properties"]["score_display"] = score_str
        feature["properties"]["click_hint"] = "Click to explore"

    return enriched


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def create_map(
    geojson: dict,
    scores_df: pd.DataFrame,
    selected_neighborhood_id: Optional[str] = None,
    metro_area: Optional[str] = None,
) -> folium.Map:
    """Build a Folium choropleth map of neighbourhood composite scores.

    Args:
        geojson:
            GeoJSON FeatureCollection as returned by
            ``data_loader.load_neighborhood_geometries()``.  Each feature's
            ``properties`` dict must contain at least ``neighborhood_id`` and
            ``neighborhood_name``.
        scores_df:
            DataFrame returned by ``data_loader.load_neighborhood_scores()``.
            Must contain columns: ``neighborhood_id``, ``composite_score``,
            ``data_completeness``, ``available_sub_scores``.
        selected_neighborhood_id:
            Optional ``neighborhood_id`` of the neighbourhood to highlight
            with an indigo (#3525CD) border.
        metro_area:
            Metro area name ("Madrid" or "Granada"). Used to centre the map
            on the capital city rather than fitting all polygons.

    Returns:
        A configured ``folium.Map`` object ready to be rendered with
        ``streamlit_folium.st_folium()``.
    """
    # 1. Merge score data into GeoJSON properties.
    enriched = _enrich_geojson(geojson, scores_df)

    # Drop features with no geometry — Folium's bounds calculation raises
    # KeyError: 'coordinates' if geometry is None.
    enriched["features"] = [
        f for f in enriched["features"] if f.get("geometry") is not None
    ]

    # 2. Determine if any metro (non-capital) zones are present.
    has_metro = any(
        f["properties"].get("zone_type") not in ("capital_neighborhood", None)
        for f in enriched["features"]
        if f.get("geometry")
    )

    # 3. Centre the map. For capital-only views use fixed coordinates + tighter
    #    zoom; when metro municipalities are included, fit all bounds instead.
    if metro_area and metro_area in _CITY_CENTERS:
        center_lat, center_lon = _CITY_CENTERS[metro_area]
    else:
        sw, ne = _compute_bounds(enriched)
        center_lat = (sw[0] + ne[0]) / 2
        center_lon = (sw[1] + ne[1]) / 2

    zoom = 10 if has_metro else 12

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=zoom,
        tiles=_CARTO_POSITRON,
        attr=_CARTO_ATTRIBUTION,
        prefer_canvas=True,
    )

    if has_metro:
        sw, ne = _compute_bounds(enriched)
        m.fit_bounds([sw, ne])

    # 4. Inject custom tooltip CSS into the map's HTML head.
    if _BrancaElement is not None:
        try:
            m.get_root().html.add_child(_BrancaElement(_TOOLTIP_CSS))
        except Exception:
            pass  # CSS injection failed — tooltip still works, just unstyled

    # 4b. Add continuous colour-scale legend to the map.
    if _COLORMAP is not None:
        try:
            _COLORMAP.add_to(m)
        except Exception:
            pass

    # 5. Style and highlight functions.
    def style_function(feature: dict) -> dict:
        props = feature["properties"]
        score        = props.get("composite_score")
        completeness = props.get("data_completeness")
        zone_type    = props.get("zone_type", "capital_neighborhood")

        low_confidence = (
            completeness is None or completeness < _LOW_CONFIDENCE_THRESHOLD
        )
        fill_color = _LOW_CONFIDENCE_FILL if low_confidence else _score_to_color(score)

        style: dict = {
            "fillColor": fill_color,
            "color": _DEFAULT_BORDER,
            "weight": 1,
            "fillOpacity": 0.7,
        }

        if zone_type == "metro_municipality":
            # Whole municipality (not subdivided): dashed border, lower opacity
            style["dashArray"] = "5, 5"
            style["fillOpacity"] = 0.45
            style["weight"] = 1.5
        elif zone_type == "metro_neighborhood":
            # Subdivided metro zone: slightly thinner border
            style["weight"] = 0.8
            style["color"] = "#555555"

        return style

    def highlight_function(feature: dict) -> dict:  # noqa: ARG001
        return {
            "weight": 2.5,
            "color": "#444444",
            "fillOpacity": 0.9,
        }

    # 6. Tooltip — three rows: name | score | click hint
    #    labels=False so only values appear; CSS nth-child selectors in
    #    _TOOLTIP_CSS style each row differently.
    tooltip = folium.GeoJsonTooltip(
        fields=["neighborhood_name", "score_display", "click_hint"],
        aliases=["", "", ""],
        localize=False,
        sticky=True,
        labels=False,
        style=(
            "background: #ffffff;"
            "border: 1px solid #E7E8E9;"
            "border-radius: 8px;"
            "padding: 8px 12px;"
            "font-family: Inter, system-ui, sans-serif;"
            "box-shadow: 0 2px 8px rgba(0,0,0,0.10);"
            "min-width: 160px;"
        ),
    )

    # 7. Popup carries neighborhood_id as the sole field so that
    #    st_folium's last_object_clicked_popup can be stripped of HTML tags
    #    to recover the raw ID.
    popup = folium.GeoJsonPopup(
        fields=["neighborhood_id"],
        labels=False,
        style=(
            "font-family: monospace; font-size: 11px; color: #888;"
            "padding: 2px 6px; background: #f9f9f9;"
        ),
    )

    folium.GeoJson(
        enriched,
        name="Neighbourhoods",
        style_function=style_function,
        highlight_function=highlight_function,
        tooltip=tooltip,
        popup=popup,
    ).add_to(m)

    # 8. Persistent highlight layer for the selected neighbourhood.
    if selected_neighborhood_id is not None:
        selected_features = [
            f
            for f in enriched["features"]
            if f["properties"].get("neighborhood_id") == selected_neighborhood_id
        ]
        if selected_features:
            selected_geojson = {
                "type": "FeatureCollection",
                "features": selected_features,
            }
            folium.GeoJson(
                selected_geojson,
                name="Selected neighbourhood",
                style_function=lambda _: {
                    "fillColor": _SELECTED_BORDER,
                    "color": _SELECTED_BORDER,
                    "weight": 3.5,
                    "fillOpacity": 0.08,
                    "dashArray": None,
                },
                tooltip=None,
            ).add_to(m)

    return m
