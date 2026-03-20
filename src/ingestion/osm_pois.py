"""
Ingestion module for OpenStreetMap POIs via Overpass API.

Source: https://overpass-api.de/
Schema target: barrioscout_raw.osm_pois

Each tag value in OSM_POI_TAGS may appear as amenity=X, shop=X, or (for
transport) railway/public_transport=X. The query searches all relevant keys
so POIs like supermarkets (shop=supermarket) and metro entrances
(railway=subway_entrance) are captured correctly.
"""

from __future__ import annotations

import time
from typing import Any

import pandas as pd
import requests

from config.settings import CITIES, OSM_POI_TAGS, OVERPASS_URL

# Extra transport tags that use non-amenity/shop OSM keys
_TRANSPORT_EXTRA: list[tuple[str, str]] = [
    ("railway", "subway_entrance"),
    ("railway", "station"),
    ("railway", "tram_stop"),
    ("public_transport", "station"),
    ("highway", "bus_stop"),
]

_OSM_TYPES = ("node", "way", "relation")


def build_overpass_query(
    bbox: tuple[float, float, float, float],
    tags: list[str],
    category: str = "",
) -> str:
    """Build an Overpass QL query covering amenity, shop and transport keys.

    For each tag value, generates node/way/relation queries for both
    amenity=value and shop=value, so POIs like supermarkets are captured.
    When category="transport", also adds railway and public_transport keys.

    Args:
        bbox: (south, west, north, east) in decimal degrees.
        tags: List of tag values from OSM_POI_TAGS (e.g. ["hospital", "clinic"]).
        category: Category name — used to inject extra transport keys.

    Returns:
        Overpass QL query string ready for POST to the Overpass API.
    """
    south, west, north, east = bbox
    bbox_str = f"{south},{west},{north},{east}"

    parts: list[str] = []
    for tag_val in tags:
        for osm_key in ("amenity", "shop"):
            for osm_type in _OSM_TYPES:
                parts.append(f'{osm_type}["{osm_key}"="{tag_val}"]({bbox_str});')

    if category == "transport":
        for key, val in _TRANSPORT_EXTRA:
            for osm_type in _OSM_TYPES:
                parts.append(f'{osm_type}["{key}"="{val}"]({bbox_str});')

    union = "\n  ".join(parts)
    return f"[out:json][timeout:60];\n(\n  {union}\n);\nout center;"


def extract(
    bbox: tuple[float, float, float, float],
    category: str,
) -> dict[str, Any]:
    """Fetch POIs from Overpass API for one category within a bounding box.

    Args:
        bbox: (south, west, north, east) in decimal degrees.
        category: One of the keys in OSM_POI_TAGS (e.g. "health", "transport").

    Returns:
        Raw Overpass JSON response as a dict.
    """
    tags = OSM_POI_TAGS.get(category, [])
    query = build_overpass_query(bbox, tags, category=category)
    response = requests.post(OVERPASS_URL, data={"data": query}, timeout=60)
    response.raise_for_status()
    return response.json()


def transform(raw: dict[str, Any], city: str, category: str) -> pd.DataFrame:
    """Flatten an Overpass JSON response into a DataFrame.

    For ways and relations, uses the precomputed center coordinates.
    Elements without coordinates are discarded.

    Args:
        raw: Raw JSON dict from extract().
        city: City identifier (e.g. "granada").
        category: POI category label (e.g. "health").

    Returns:
        DataFrame with columns: osm_id, city, category, osm_type, name, lat, lon.
    """
    records: list[dict] = []
    for el in raw.get("elements", []):
        tags = el.get("tags", {})

        # Resolve coordinates: nodes have top-level lat/lon,
        # ways/relations have a center object from "out center;"
        if el.get("type") == "node":
            lat = el.get("lat")
            lon = el.get("lon")
        else:
            center = el.get("center", {})
            lat = center.get("lat")
            lon = center.get("lon")

        if lat is None or lon is None:
            continue

        # osm_type: the concrete tag value that matched
        osm_type = (
            tags.get("amenity")
            or tags.get("shop")
            or tags.get("railway")
            or tags.get("public_transport")
        )

        records.append(
            {
                "osm_id":   el.get("id"),
                "city":     city,
                "category": category,
                "osm_type": osm_type,
                "name":     tags.get("name"),
                "lat":      lat,
                "lon":      lon,
            }
        )

    return pd.DataFrame(records) if records else pd.DataFrame(
        columns=["osm_id", "city", "category", "osm_type", "name", "lat", "lon"]
    )


def load(df: pd.DataFrame) -> int:
    """Load the transformed DataFrame into BigQuery raw layer.

    Args:
        df: Transformed DataFrame from transform().

    Returns:
        Number of rows loaded.
    """
    from src.processing.bq_loader import load_to_bigquery

    return load_to_bigquery(df, "barrioscout_raw.osm_pois")


def main() -> None:
    frames: list[pd.DataFrame] = []

    for city_key, city_cfg in CITIES.items():
        bbox = city_cfg["bbox"]
        for category in OSM_POI_TAGS:
            print(f"  Fetching {category} in {city_key}...", end=" ", flush=True)
            try:
                raw = extract(bbox, category)
                df = transform(raw, city=city_key, category=category)
                print(f"{len(df)} POIs")
                frames.append(df)
            except Exception as exc:
                print(f"FAILED — {exc}")
            time.sleep(2)

    if not frames:
        print("No data collected — aborting.")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["osm_id"]).reset_index(drop=True)
    print(f"\nTotal: {len(combined):,} unique POIs across all cities and categories")

    loaded = load(combined)
    print(f"Loaded: {loaded:,} rows → barrioscout_raw.osm_pois")


if __name__ == "__main__":
    print("=== OSM POIs pipeline ===")
    main()
