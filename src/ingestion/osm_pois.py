"""
Ingestion module for OpenStreetMap POIs via Overpass API.

Source: https://overpass-api.de/
Schema target: barrioscout_raw.osm_pois
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import requests

from config.settings import OVERPASS_URL, OSM_POI_TAGS

logger = logging.getLogger(__name__)


def build_overpass_query(bbox: tuple[float, float, float, float], amenities: list[str]) -> str:
    """Build an Overpass QL query for the given bounding box and amenity list.

    Args:
        bbox: (south, west, north, east) in decimal degrees.
        amenities: List of OSM amenity tag values.

    Returns:
        Overpass QL query string.
    """
    south, west, north, east = bbox
    bbox_str = f"{south},{west},{north},{east}"
    union_parts = "\n  ".join(
        f'node["amenity"="{a}"]({bbox_str});' for a in amenities
    )
    return f"[out:json][timeout:25];\n(\n  {union_parts}\n);\nout body;"


def extract(bbox: tuple[float, float, float, float], category: str = "health") -> dict[str, Any]:
    """Fetch POIs from Overpass API for a category within a bounding box.

    Args:
        bbox: (south, west, north, east) in decimal degrees.
        category: One of the keys in OSM_POI_TAGS (e.g. 'health', 'education').

    Returns:
        Raw Overpass JSON response as a dict.
    """
    amenities = OSM_POI_TAGS.get(category, [])
    query = build_overpass_query(bbox, amenities)
    response = requests.post(OVERPASS_URL, data={"data": query}, timeout=60)
    response.raise_for_status()
    return response.json()


def transform(raw: dict[str, Any], city: str, category: str) -> pd.DataFrame:
    """Flatten Overpass JSON elements into a DataFrame.

    Args:
        raw: Raw JSON dict from extract().
        city: City identifier (e.g. 'granada').
        category: POI category label.

    Returns:
        DataFrame with one row per POI.
    """
    elements = raw.get("elements", [])
    records = []
    for el in elements:
        tags = el.get("tags", {})
        records.append(
            {
                "osm_id": el.get("id"),
                "city": city,
                "category": category,
                "amenity": tags.get("amenity"),
                "name": tags.get("name"),
                "lat": el.get("lat"),
                "lon": el.get("lon"),
            }
        )
    return pd.DataFrame(records)


def load(df: pd.DataFrame) -> None:
    """Load the transformed DataFrame into BigQuery raw layer.

    Args:
        df: Transformed DataFrame from transform().
    """
    from src.processing.bq_loader import load_dataframe
    load_dataframe(df, dataset="barrioscout_raw", table="osm_pois")
