"""
Ingestion module for Catastro INSPIRE WFS — Building data.

Source: Dirección General del Catastro — wfsBU.aspx (Buildings)
Schema target: barrioscout_raw.catastro_buildings

Approach: bbox tiling (EPSG:4326 → 25830 via pyproj), 900m tiles (~0.81km²).
NOTE: Catastro WFS actual area limit is ~1km² (not the 4km² stated in docs). Tested: 1000m OK, 1100m fails.
Sleep 1s between requests as a courtesy to the public API.

XML structure (confirmed by probing):
  Encoding: ISO-8859-1
  Building: <bu-ext2d:Building>
  Fields:   bu-core2d:reference, bu-core2d:beginning, bu-ext2d:currentUse
  Centroid: midpoint of gml:boundedBy/gml:Envelope (EPSG:25830 → WGS84)
"""

from __future__ import annotations

import argparse
import time
from xml.etree import ElementTree as ET

import pandas as pd
import requests
from pyproj import Transformer

from config.settings import CITIES

_WFS_URL = "http://ovc.catastro.meh.es/INSPIRE/wfsBU.aspx"

_NS = {
    "gml":       "http://www.opengis.net/gml/3.2",
    "bu-ext2d":  "http://inspire.jrc.ec.europa.eu/schemas/bu-ext2d/2.0",
    "bu-core2d": "http://inspire.jrc.ec.europa.eu/schemas/bu-core2d/2.0",
    "base":      "urn:x-inspire:specification:gmlas:BaseTypes:3.2",
}

_TO_25830 = Transformer.from_crs("EPSG:4326", "EPSG:25830", always_xy=True)
_TO_WGS84 = Transformer.from_crs("EPSG:25830", "EPSG:4326", always_xy=True)


def _generate_tiles(bbox_4326: tuple, tile_size_m: int = 900) -> list[tuple]:
    """Convert an EPSG:4326 bounding box into a grid of EPSG:25830 tiles.

    Args:
        bbox_4326: (south, west, north, east) in decimal degrees.
        tile_size_m: Side length of each square tile in metres.

    Returns:
        List of (xmin, ymin, xmax, ymax) tuples in EPSG:25830.
    """
    south, west, north, east = bbox_4326
    x_min, y_min = _TO_25830.transform(west, south)
    x_max, y_max = _TO_25830.transform(east, north)

    tiles: list[tuple] = []
    x = x_min
    while x < x_max:
        y = y_min
        while y < y_max:
            tiles.append((x, y, min(x + tile_size_m, x_max), min(y + tile_size_m, y_max)))
            y += tile_size_m
        x += tile_size_m

    print(f"  Generated {len(tiles)} tiles ({tile_size_m}m × {tile_size_m}m)")
    return tiles


def _fetch_tile(bbox_25830: tuple, max_retries: int = 2, retry_wait: int = 5) -> str:
    """Fetch one WFS tile from the Catastro API, with retry on failure.

    Args:
        bbox_25830: (xmin, ymin, xmax, ymax) in EPSG:25830.
        max_retries: Maximum number of retry attempts after the first failure.
        retry_wait: Seconds to wait between retries.

    Returns:
        Raw XML string, or empty string after all attempts fail / ExceptionReport.
    """
    xmin, ymin, xmax, ymax = bbox_25830
    params = {
        "SERVICE":   "WFS",
        "REQUEST":   "GetFeature",
        "TYPENAMES": "BU:Building",
        "SRSName":   "EPSG::25830",
        "BBOX":      f"{xmin},{ymin},{xmax},{ymax}",
    }
    for attempt in range(1 + max_retries):
        try:
            r = requests.get(_WFS_URL, params=params, timeout=60)
            r.raise_for_status()
            if "ExceptionReport" in r.text:
                print(f"  WARNING: ExceptionReport for tile {xmin:.0f},{ymin:.0f}")
                return ""
            return r.text
        except Exception as exc:
            if attempt < max_retries:
                print(f"  WARNING: tile {xmin:.0f},{ymin:.0f} failed (attempt {attempt + 1}) — {exc}. Retrying in {retry_wait}s...")
                time.sleep(retry_wait)
            else:
                print(f"  WARNING: tile {xmin:.0f},{ymin:.0f} failed after {attempt + 1} attempts — {exc}")
    return ""


def extract(city_key: str = "granada") -> list[str]:
    """Download all building tiles for a city.

    Args:
        city_key: Key in CITIES config (e.g. "granada", "madrid").

    Returns:
        List of raw XML strings, one per tile (may include empty strings for failed tiles).
    """
    bbox = CITIES[city_key]["bbox"]  # (south, west, north, east) EPSG:4326
    tiles = _generate_tiles(bbox)
    xml_list: list[str] = []
    failed = 0
    for i, tile in enumerate(tiles, 1):
        xml = _fetch_tile(tile)
        if not xml:
            failed += 1
        count = xml.count("<bu-ext2d:Building ") if xml else 0
        print(f"  Tile {i}/{len(tiles)}: {count} buildings")
        xml_list.append(xml)
        time.sleep(1)
    print(f"  Tiles: {len(tiles)} total, {failed} failed, {len(tiles) - failed} succeeded")
    return xml_list


def transform(xml_list: list[str]) -> pd.DataFrame:
    """Parse raw XML tiles into a clean, deduplicated buildings DataFrame.

    Args:
        xml_list: List of XML strings from extract().

    Returns:
        DataFrame with columns: cadastral_ref, year_built, current_use, latitude, longitude.
    """
    records: list[dict] = []
    for xml in xml_list:
        if not xml:
            continue
        try:
            root = ET.fromstring(xml.encode("iso-8859-1"))
        except ET.ParseError as exc:
            print(f"  WARNING: XML parse error — {exc}")
            continue

        for building in root.findall(".//bu-ext2d:Building", _NS):
            rec = _parse_building(building)
            if rec:
                records.append(rec)

    if not records:
        return pd.DataFrame(
            columns=["cadastral_ref", "year_built", "current_use", "latitude", "longitude"]
        )

    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset=["cadastral_ref"])
    return df.reset_index(drop=True)


def _parse_building(building: ET.Element) -> dict | None:
    """Extract fields from a single <bu-ext2d:Building> element.

    Returns None if cadastral_ref is missing (required field).
    """
    # cadastral_ref (required)
    ref_el = building.find(".//bu-core2d:reference", _NS)
    cadastral_ref = ref_el.text.strip() if ref_el is not None and ref_el.text else None
    if not cadastral_ref:
        return None

    # year_built: first 4 chars of ISO date in <bu-core2d:beginning>
    year_built: int | None = None
    beg_el = building.find(".//bu-core2d:beginning", _NS)
    if beg_el is not None and beg_el.text:
        try:
            year_built = int(beg_el.text[:4])
        except ValueError:
            pass

    # current_use: e.g. "1_residential", "3_industrial"
    use_el = building.find("bu-ext2d:currentUse", _NS)
    current_use = use_el.text.strip() if use_el is not None and use_el.text else None

    # centroid: midpoint of gml:boundedBy/gml:Envelope (EPSG:25830) → WGS84
    latitude: float | None = None
    longitude: float | None = None
    envelope = building.find("gml:boundedBy/gml:Envelope", _NS)
    if envelope is not None:
        lower = envelope.find("gml:lowerCorner", _NS)
        upper = envelope.find("gml:upperCorner", _NS)
        if lower is not None and upper is not None and lower.text and upper.text:
            lx, ly = map(float, lower.text.split())
            ux, uy = map(float, upper.text.split())
            cx, cy = (lx + ux) / 2, (ly + uy) / 2
            longitude, latitude = _TO_WGS84.transform(cx, cy)

    return {
        "cadastral_ref": cadastral_ref,
        "year_built":    year_built,
        "current_use":   current_use,
        "latitude":      latitude,
        "longitude":     longitude,
    }


def load(df: pd.DataFrame) -> int:
    """Load the transformed DataFrame into BigQuery raw layer.

    Args:
        df: Transformed DataFrame from transform().

    Returns:
        Number of rows loaded.
    """
    from src.processing.bq_loader import load_to_bigquery

    return load_to_bigquery(df, "barrioscout_raw.catastro_buildings")


def main() -> None:
    parser = argparse.ArgumentParser(description="Catastro INSPIRE buildings pipeline")
    parser.add_argument(
        "--city",
        choices=list(CITIES.keys()),
        default=None,
        help="City to process (default: all cities)",
    )
    args = parser.parse_args()

    city_keys = [args.city] if args.city else list(CITIES.keys())

    for city_key in city_keys:
        print(f"\n=== Catastro pipeline — {city_key} ===")
        xml_list = extract(city_key)
        df = transform(xml_list)
        print(f"Transformed: {len(df):,} buildings")
        loaded = load(df)
        print(f"Loaded     : {loaded:,} rows → barrioscout_raw.catastro_buildings")


if __name__ == "__main__":
    main()
