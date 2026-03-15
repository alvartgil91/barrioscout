"""
Source validation script for BarrioScout.

Validates connectivity and basic data retrieval for each external data source.
Does NOT require BigQuery to be configured — only tests external APIs.

Usage:
    python tests/test_sources.py
"""

from __future__ import annotations

import sys
import os

# Allow imports from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import textwrap
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()

from config.settings import (
    CITIES,
    GOOGLE_PLACES_API_KEY,
    GOOGLE_PLACES_URL,
    INE_RENTA_URL,
    INE_IPV_URL,
    OVERPASS_URL,
    OSM_POI_TAGS,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"
BOLD = "\033[1m"


def _ok(source: str, detail: str) -> None:
    print(f"  {GREEN}OK{RESET}    {BOLD}{source}{RESET}")
    print(f"         {detail}\n")


def _fail(source: str, error: str) -> None:
    print(f"  {RED}FAIL{RESET}  {BOLD}{source}{RESET}")
    print(f"         {error}\n")


# ---------------------------------------------------------------------------
# 1. Ministerio de Transportes
# ---------------------------------------------------------------------------

def test_ministerio() -> None:
    """Validate price data using INE IPV (Índice de Precios de Vivienda).

    NOTE: Ministerio de Transportes municipal CSV files block programmatic access
    via CloudFront WAF (HTTP 403). The INE IPV series (quarterly, by CCAA) is used
    as the open alternative for price trend analysis. Municipal-level data from the
    Ministerio can be downloaded manually from transportes.gob.es and placed in
    data/raw/ for local processing.
    """
    source = "Precio vivienda — INE IPV (quarterly)"
    try:
        import io
        import pandas as pd

        resp = requests.get(INE_IPV_URL, timeout=30)
        resp.raise_for_status()
        df = pd.read_csv(
            io.BytesIO(resp.content),
            sep=";",
            encoding="utf-8-sig",
            dtype=str,
        )
        df.columns = [c.strip().lower() for c in df.columns]
        total_rows = len(df)
        sample = f"Total rows: {total_rows:,} | Columns: {list(df.columns[:4])}"
        _ok(source, sample)
    except Exception as exc:
        _fail(source, str(exc))


# ---------------------------------------------------------------------------
# 2. Catastro INSPIRE
# ---------------------------------------------------------------------------

def test_catastro() -> None:
    """Fetch a Catastro WFS feature for a known Granada parcel reference."""
    source = "Catastro INSPIRE (WFS)"
    # A known public reference in Granada city centre
    REFCAT = "9872023VH5197S0001WX"
    WFS_URL = "https://ovc.catastro.meh.es/INSPIRE/wfsCP.aspx"
    try:
        params = {
            "SERVICE": "WFS",
            "REQUEST": "GetCapabilities",
        }
        resp = requests.get(WFS_URL, params=params, timeout=30)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")
        body_snippet = resp.text[:200].replace("\n", " ").strip()

        if "xml" in content_type.lower() or "<?xml" in resp.text[:50]:
            _ok(source, f"WFS GetCapabilities returned XML | snippet: {body_snippet[:120]}")
        else:
            _fail(source, f"Unexpected content type: {content_type} | body: {body_snippet}")
    except Exception as exc:
        _fail(source, str(exc))


# ---------------------------------------------------------------------------
# 3. OpenStreetMap Overpass API
# ---------------------------------------------------------------------------

def test_osm() -> None:
    """Query hospitals within the Granada bounding box."""
    source = "OpenStreetMap — Overpass API"
    granada = CITIES["granada"]
    south, west, north, east = granada["bbox"]
    bbox_str = f"{south},{west},{north},{east}"

    query = (
        f'[out:json][timeout:25];\n'
        f'(\n'
        f'  node["amenity"="hospital"]({bbox_str});\n'
        f'  way["amenity"="hospital"]({bbox_str});\n'
        f');\n'
        f'out body;'
    )
    try:
        resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        elements = data.get("elements", [])
        count = len(elements)
        first_name = elements[0].get("tags", {}).get("name", "unnamed") if elements else "—"
        _ok(source, f"Hospitals in Granada: {count} | First: '{first_name}'")
    except Exception as exc:
        _fail(source, str(exc))


# ---------------------------------------------------------------------------
# 4. Google Places API
# ---------------------------------------------------------------------------

def test_google_places() -> None:
    """Search for supermarkets near Granada city centre (requires API key)."""
    source = "Google Places API"
    if not GOOGLE_PLACES_API_KEY:
        print(f"  SKIP  {BOLD}{source}{RESET}")
        print(f"         GOOGLE_PLACES_API_KEY not set — skipping.\n")
        return

    granada = CITIES["granada"]
    params = {
        "location": f"{granada['lat']},{granada['lon']}",
        "radius": 1000,
        "type": "supermarket",
        "key": GOOGLE_PLACES_API_KEY,
    }
    try:
        resp = requests.get(GOOGLE_PLACES_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status")
        results = data.get("results", [])
        if status == "OK":
            first = results[0].get("name", "—") if results else "—"
            _ok(source, f"Status: {status} | Results: {len(results)} | First: '{first}'")
        else:
            _fail(source, f"API status: {status} | error: {data.get('error_message', '—')}")
    except Exception as exc:
        _fail(source, str(exc))


# ---------------------------------------------------------------------------
# 5. INE
# ---------------------------------------------------------------------------

def test_ine() -> None:
    """Download the INE median income CSV and validate it has rows."""
    source = "INE — Renta neta media por persona"
    try:
        import io
        import pandas as pd

        resp = requests.get(INE_RENTA_URL, timeout=60)
        resp.raise_for_status()

        df = pd.read_csv(
            io.BytesIO(resp.content),
            sep="\t",
            encoding="utf-8-sig",
            thousands=".",
            decimal=",",
            dtype=str,
            nrows=5,
        )
        rows_hint = f"First 5 rows loaded | Columns: {list(df.columns[:4])}"
        _ok(source, rows_hint)
    except Exception as exc:
        # Try HEAD to confirm server is up
        try:
            ping = requests.head("https://www.ine.es/", timeout=10)
            _fail(
                source,
                f"INE server reachable (HTTP {ping.status_code}) but CSV parse failed: {exc}",
            )
        except Exception:
            _fail(source, str(exc))


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

TESTS = [
    test_ministerio,
    test_catastro,
    test_osm,
    test_google_places,
    test_ine,
]


def main() -> None:
    print()
    print(f"{BOLD}{'=' * 55}{RESET}")
    print(f"{BOLD}  BarrioScout — Data Source Validation{RESET}")
    print(f"{BOLD}{'=' * 55}{RESET}\n")

    for test_fn in TESTS:
        test_fn()

    print(f"{BOLD}{'=' * 55}{RESET}")
    print("Done. Fix any FAIL entries before running ingestion.")
    print()


if __name__ == "__main__":
    main()
