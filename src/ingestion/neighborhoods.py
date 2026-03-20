"""
Ingestion module for neighbourhood and district polygons.

Sources:
  - Madrid: Ayuntamiento de Madrid open data — TopoJSON (quantized, WGS84)
    Barrios (131): geoportal.madrid.es/.../Barrios/TopoJSON/Barrios.json
    Distritos (21): geoportal.madrid.es/.../Distritos/TopoJSON/Distritos.json
  - Granada: IDE Andalucía DEA100 WFS — GeoJSON (EPSG:23030, reprojected to WGS84)
    Layer: dea100:da04_barrio (37 barrios, 8 distritos derived via dissolve)

Schema target: barrioscout_raw.neighborhoods

Dependencies: requests, shapely, pyproj, pandas
"""

from __future__ import annotations

import argparse
import json
import time

import pandas as pd
import requests
from pyproj import Transformer
from shapely.geometry import MultiPolygon, Polygon, shape
from shapely.ops import unary_union
from shapely.validation import make_valid

from config.settings import CITIES

# ---------------------------------------------------------------------------
# URLs
# ---------------------------------------------------------------------------

_MADRID_BARRIOS_URL = (
    "https://geoportal.madrid.es/fsdescargas/IDEAM_WBGEOPORTAL/"
    "LIMITES_ADMINISTRATIVOS/Barrios/TopoJSON/Barrios.json"
)
_MADRID_DISTRITOS_URL = (
    "https://geoportal.madrid.es/fsdescargas/IDEAM_WBGEOPORTAL/"
    "LIMITES_ADMINISTRATIVOS/Distritos/TopoJSON/Distritos.json"
)
_GRANADA_WFS_URL = "https://www.ideandalucia.es/services/dea100/wfs"

# EPSG:23030 (ED50 / UTM 30N) → WGS84
_TO_WGS84 = Transformer.from_crs("EPSG:23030", "EPSG:4326", always_xy=True)


# ---------------------------------------------------------------------------
# TopoJSON decoder (no external dependency)
# ---------------------------------------------------------------------------


def _decode_topojson(topo: dict, object_name: str) -> list[dict]:
    """Decode a quantized TopoJSON object into a list of GeoJSON-like features.

    Handles Polygon and MultiPolygon geometry types.

    Args:
        topo: Parsed TopoJSON dict (must have 'arcs', 'transform', 'objects').
        object_name: Key inside topo['objects'] to decode.

    Returns:
        List of dicts with 'properties' and 'geometry' (shapely object).
    """
    arcs = topo["arcs"]
    scale = topo["transform"]["scale"]
    translate = topo["transform"]["translate"]

    # Delta-decode + dequantize all arcs upfront
    decoded_arcs: list[list[tuple[float, float]]] = []
    for arc in arcs:
        coords: list[tuple[float, float]] = []
        x, y = 0, 0
        for dx, dy in arc:
            x += dx
            y += dy
            coords.append((x * scale[0] + translate[0], y * scale[1] + translate[1]))
        decoded_arcs.append(coords)

    def _resolve_ring(arc_indices: list[int]) -> list[tuple[float, float]]:
        """Concatenate arcs (reversing negative indices) into a coordinate ring."""
        ring: list[tuple[float, float]] = []
        for idx in arc_indices:
            if idx >= 0:
                segment = decoded_arcs[idx]
            else:
                segment = decoded_arcs[~idx][::-1]
            # Skip first point of subsequent arcs to avoid duplicates
            ring.extend(segment if not ring else segment[1:])
        return ring

    features: list[dict] = []
    for geom in topo["objects"][object_name]["geometries"]:
        geom_type = geom["type"]
        arc_refs = geom["arcs"]

        if geom_type == "Polygon":
            rings = [_resolve_ring(ring_idx) for ring_idx in arc_refs]
            polygon = Polygon(rings[0], rings[1:])
            features.append({"properties": geom.get("properties", {}), "geometry": polygon})

        elif geom_type == "MultiPolygon":
            polygons = []
            for poly_arcs in arc_refs:
                rings = [_resolve_ring(ring_idx) for ring_idx in poly_arcs]
                polygons.append(Polygon(rings[0], rings[1:]))
            features.append({"properties": geom.get("properties", {}), "geometry": MultiPolygon(polygons)})

    return features


# ---------------------------------------------------------------------------
# Madrid extract
# ---------------------------------------------------------------------------


def _extract_madrid() -> list[dict]:
    """Download and decode Madrid barrios + distritos from TopoJSON.

    Returns:
        List of raw dicts with city, level, name, code, district_name, geometry.
    """
    records: list[dict] = []

    # --- Barrios (131) ---
    print("  Downloading Madrid barrios TopoJSON...")
    resp = requests.get(_MADRID_BARRIOS_URL, timeout=30)
    resp.raise_for_status()
    # Use json.loads(resp.content) instead of resp.json() to force UTF-8 decoding.
    # resp.json() relies on resp.encoding, which requests may mis-detect as latin-1
    # when the server omits charset in Content-Type, causing mojibake on accented chars.
    topo = json.loads(resp.content)
    features = _decode_topojson(topo, "BARRIOS")
    print(f"  Decoded {len(features)} barrios")

    for feat in features:
        props = feat["properties"]
        records.append({
            "city": "Madrid",
            "level": "neighborhood",
            "name": props.get("NOMBRE", "").strip(),
            "code": props.get("COD_BAR", "").strip(),
            "district_name": props.get("NOMDIS", "").strip(),
            "geometry": feat["geometry"],
        })

    # --- Distritos (21) ---
    print("  Downloading Madrid distritos TopoJSON...")
    resp = requests.get(_MADRID_DISTRITOS_URL, timeout=30)
    resp.raise_for_status()
    topo = json.loads(resp.content)
    features = _decode_topojson(topo, "DISTRITOS")
    print(f"  Decoded {len(features)} distritos")

    for feat in features:
        props = feat["properties"]
        records.append({
            "city": "Madrid",
            "level": "district",
            "name": props.get("NOMBRE", "").strip(),
            "code": props.get("COD_DIS_TX", "").strip(),
            "district_name": None,
            "geometry": feat["geometry"],
        })

    return records


# ---------------------------------------------------------------------------
# Granada extract
# ---------------------------------------------------------------------------


def _reproject_coords(
    coords: list,
    transformer: Transformer,
) -> list:
    """Recursively reproject nested coordinate lists (handles Polygon and MultiPolygon)."""
    if isinstance(coords[0], (int, float)):
        # Base case: single coordinate pair [x, y]
        lon, lat = transformer.transform(coords[0], coords[1])
        return [lon, lat]
    return [_reproject_coords(c, transformer) for c in coords]


def _extract_granada() -> list[dict]:
    """Fetch Granada barrios from IDE Andalucía WFS and derive district polygons.

    The WFS returns GeoJSON in EPSG:23030. Coordinates are reprojected to WGS84.
    District polygons are generated by dissolving barrios that share the same
    'distrito' field using shapely.ops.unary_union.

    Returns:
        List of raw dicts with city, level, name, code, district_name, geometry.
    """
    print("  Querying IDE Andalucía WFS for Granada barrios...")
    params = {
        "SERVICE": "WFS",
        "VERSION": "1.1.0",
        "REQUEST": "GetFeature",
        "TYPENAME": "dea100:da04_barrio",
        "OUTPUTFORMAT": "application/json",
        "CQL_FILTER": "municipio='Granada'",
        "MAXFEATURES": "500",
    }
    resp = requests.get(_GRANADA_WFS_URL, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    features = data.get("features", [])
    print(f"  Received {len(features)} barrio features")

    if not features:
        print("  WARNING: No features returned for Granada — WFS may be down")
        return []

    records: list[dict] = []
    # Group barrios by distrito for dissolve
    distrito_barrios: dict[str, list[Polygon | MultiPolygon]] = {}
    barrio_counter: dict[str, int] = {}

    for feat in features:
        props = feat["properties"]
        barrio_name = (props.get("barrio") or "").strip()
        distrito_name = (props.get("distrito") or "").strip()

        # Reproject geometry from EPSG:23030 to WGS84
        geom_dict = feat["geometry"]
        geom_dict["coordinates"] = _reproject_coords(geom_dict["coordinates"], _TO_WGS84)
        geom = shape(geom_dict)

        # Generate unique code: distrito prefix + sequential number
        barrio_counter.setdefault(distrito_name, 0)
        barrio_counter[distrito_name] += 1
        prefix = distrito_name[:3].upper()
        code = f"{prefix}-{barrio_counter[distrito_name]:02d}"

        records.append({
            "city": "Granada",
            "level": "neighborhood",
            "name": barrio_name,
            "code": code,
            "district_name": distrito_name,
            "geometry": geom,
        })

        # Collect for distrito dissolve
        distrito_barrios.setdefault(distrito_name, []).append(geom)

    # --- Dissolve barrios into distrito polygons ---
    print(f"  Dissolving {len(distrito_barrios)} distritos from barrio polygons...")
    for distrito_name, geoms in sorted(distrito_barrios.items()):
        merged = unary_union(geoms)
        records.append({
            "city": "Granada",
            "level": "district",
            "name": distrito_name,
            "code": None,
            "district_name": None,
            "geometry": merged,
        })

    time.sleep(1)  # Courtesy pause after WFS request
    return records


# ---------------------------------------------------------------------------
# Extract / Transform / Load
# ---------------------------------------------------------------------------


def extract(city: str | None = None) -> list[dict]:
    """Fetch neighbourhood and district polygons for configured cities.

    Args:
        city: Optional city key ('granada' or 'madrid'). If None, fetches both.

    Returns:
        List of raw feature dicts with shapely geometry objects.
    """
    city_keys = [city.lower()] if city else list(CITIES.keys())
    records: list[dict] = []

    for key in city_keys:
        if key == "madrid":
            print("\n--- Madrid ---")
            records.extend(_extract_madrid())
        elif key == "granada":
            print("\n--- Granada ---")
            records.extend(_extract_granada())
        else:
            print(f"  WARNING: Unknown city '{key}' — skipping")

    return records


def transform(raw: list[dict]) -> pd.DataFrame:
    """Validate geometries and convert to WKT for BigQuery storage.

    Args:
        raw: List of feature dicts from extract() with shapely geometry objects.

    Returns:
        DataFrame with columns: city, level, name, code, district_name, geometry_wkt.
    """
    if not raw:
        return pd.DataFrame(
            columns=["city", "level", "name", "code", "district_name", "geometry_wkt"]
        )

    records: list[dict] = []
    invalid_count = 0

    for feat in raw:
        geom = feat["geometry"]

        # Validate and repair if needed
        if not geom.is_valid:
            geom = make_valid(geom)
            invalid_count += 1

        records.append({
            "city": feat["city"],
            "level": feat["level"],
            "name": feat["name"],
            "code": feat["code"],
            "district_name": feat["district_name"],
            "geometry_wkt": geom.wkt,
        })

    if invalid_count:
        print(f"  Repaired {invalid_count} invalid geometries")

    df = pd.DataFrame(records)
    print(f"  Transform complete: {len(df)} rows "
          f"({df['level'].value_counts().to_dict()})")
    return df


def load(df: pd.DataFrame) -> int:
    """Load transformed DataFrame into BigQuery raw layer.

    Args:
        df: Transformed neighbourhoods DataFrame.

    Returns:
        Number of rows loaded.
    """
    from src.processing.bq_loader import load_to_bigquery

    return load_to_bigquery(df, "barrioscout_raw.neighborhoods")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Neighbourhoods and districts polygons pipeline"
    )
    parser.add_argument(
        "--city",
        choices=list(CITIES.keys()),
        default=None,
        help="City to process (default: all cities)",
    )
    args = parser.parse_args()

    print("=== Neighbourhoods Pipeline ===")
    raw = extract(args.city)
    if not raw:
        print("No features extracted.")
        return

    df = transform(raw)

    print(f"\n--- Summary ---")
    for city in df["city"].unique():
        city_df = df[df["city"] == city]
        districts = city_df[city_df["level"] == "district"]
        neighborhoods = city_df[city_df["level"] == "neighborhood"]
        print(f"  {city}: {len(districts)} districts, {len(neighborhoods)} neighbourhoods")

    rows = load(df)
    print(f"\nLoaded {rows} rows → barrioscout_raw.neighborhoods")
    print("Done.")


if __name__ == "__main__":
    main()
