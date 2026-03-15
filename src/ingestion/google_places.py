"""
Ingestion module for Google Places API.

Source: https://maps.googleapis.com/maps/api/place/nearbysearch/json
Requires: GOOGLE_PLACES_API_KEY in .env
Schema target: barrioscout_raw.google_places
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import requests

from config.settings import GOOGLE_PLACES_API_KEY, GOOGLE_PLACES_URL

logger = logging.getLogger(__name__)


def extract(
    lat: float,
    lon: float,
    place_type: str = "supermarket",
    radius_m: int = 1000,
) -> dict[str, Any]:
    """Fetch nearby places from Google Places API.

    Args:
        lat: Centre latitude.
        lon: Centre longitude.
        place_type: Google Places type string (e.g. 'supermarket', 'hospital').
        radius_m: Search radius in metres.

    Returns:
        Raw API response as a dict.

    Raises:
        EnvironmentError: If GOOGLE_PLACES_API_KEY is not set.
        requests.HTTPError: On API error.
    """
    if not GOOGLE_PLACES_API_KEY:
        raise EnvironmentError("GOOGLE_PLACES_API_KEY is not set in environment.")

    params = {
        "location": f"{lat},{lon}",
        "radius": radius_m,
        "type": place_type,
        "key": GOOGLE_PLACES_API_KEY,
    }
    response = requests.get(GOOGLE_PLACES_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def transform(raw: dict[str, Any], city: str) -> pd.DataFrame:
    """Flatten Google Places results into a DataFrame.

    Args:
        raw: Raw JSON dict from extract().
        city: City identifier.

    Returns:
        DataFrame with one row per place.
    """
    results = raw.get("results", [])
    records = []
    for place in results:
        location = place.get("geometry", {}).get("location", {})
        records.append(
            {
                "place_id": place.get("place_id"),
                "city": city,
                "name": place.get("name"),
                "type": ",".join(place.get("types", [])),
                "rating": place.get("rating"),
                "user_ratings_total": place.get("user_ratings_total"),
                "lat": location.get("lat"),
                "lon": location.get("lng"),
                "vicinity": place.get("vicinity"),
            }
        )
    return pd.DataFrame(records)


def load(df: pd.DataFrame) -> None:
    """Load the transformed DataFrame into BigQuery raw layer.

    Args:
        df: Transformed DataFrame from transform().
    """
    from src.processing.bq_loader import load_dataframe
    load_dataframe(df, dataset="barrioscout_raw", table="google_places")
