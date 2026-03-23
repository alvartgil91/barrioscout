"""
Fix 4 known bad geocodes in barrioscout_raw.idealista_listings.

Affected property_ids (all Madrid metro):
  110937108 — "Cerceda"              → geocoded to Galicia (lat 43.18)
  110984813 — "La Serna del Monte"   → outside Madrid bbox (lat 41.03)
  110275820 — "calle Alta Santo Domingo" → city == full address (no comma)
  101597453 — "embajador Ciudalcampo"    → city == full address (no comma)

Usage:
    cd /path/to/barrioscout
    python scripts/fix_4_bad_geocodes.py
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load .env from repo root before importing project modules
load_dotenv(Path(__file__).parent.parent / ".env")

# Add repo root to path so src/ and config/ are importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from google.cloud import bigquery

from src.ingestion.idealista_emails import geocode_address

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROJECT = "portfolio-alvartgil91"
TABLE = f"{PROJECT}.barrioscout_raw.idealista_listings"
ALERT_CITY = "Madrid"

TARGET_IDS = [
    "110937108",
    "110984813",
    "110275820",
    "101597453",
]

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    client = bigquery.Client(project=PROJECT)

    # 1. Fetch current state of the 4 listings
    ids_sql = ", ".join(f"'{pid}'" for pid in TARGET_IDS)
    query = f"""
        SELECT property_id, address, city, lat, lon, geocode_level
        FROM `{TABLE}`
        WHERE property_id IN ({ids_sql})
        QUALIFY ROW_NUMBER() OVER (PARTITION BY property_id ORDER BY _loaded_at DESC) = 1
    """
    rows = list(client.query(query).result())

    if not rows:
        print("No rows found — check property_ids or table name.")
        sys.exit(1)

    # 2. Re-geocode each and display old vs new
    print(f"\n{'='*72}")
    print(f"Re-geocoding {len(rows)} listings with alert_city={ALERT_CITY!r}")
    print(f"{'='*72}\n")

    results: list[dict] = []
    for row in rows:
        pid = row["property_id"]
        address = row["address"] or ""
        city = row["city"] or ""
        old_lat = row["lat"]
        old_lon = row["lon"]
        old_level = row["geocode_level"]

        print(f"property_id : {pid}")
        print(f"address     : {address!r}")
        print(f"city        : {city!r}")
        print(f"old coords  : lat={old_lat}, lon={old_lon}, level={old_level!r}")

        new_lat, new_lon, new_level = geocode_address(
            address, city, alert_city=ALERT_CITY
        )

        print(f"new coords  : lat={new_lat}, lon={new_lon}, level={new_level!r}")
        changed = (new_lat != old_lat) or (new_lon != old_lon)
        print(f"changed     : {'YES ✓' if changed else 'NO (same result)'}")
        print()

        results.append(
            {
                "property_id": pid,
                "new_lat": new_lat,
                "new_lon": new_lon,
                "new_level": new_level,
                "changed": changed,
            }
        )

    # 3. Confirmation prompt
    updatable = [r for r in results if r["new_lat"] is not None]
    if not updatable:
        print("No successful geocodes — nothing to update.")
        sys.exit(0)

    answer = input(f"¿Actualizar {len(updatable)} listing(s) en BigQuery? [y/n]: ").strip().lower()
    if answer != "y":
        print("Aborted — no changes written.")
        sys.exit(0)

    # 4. UPDATE each row
    print()
    for r in updatable:
        pid = r["property_id"]
        dml = f"""
            UPDATE `{TABLE}`
            SET
                lat           = {r['new_lat']},
                lon           = {r['new_lon']},
                geocode_level = '{r['new_level']}',
                alert_city    = '{ALERT_CITY}'
            WHERE property_id = '{pid}'
        """
        client.query(dml).result()
        print(f"  Updated property_id={pid} → lat={r['new_lat']:.6f}, lon={r['new_lon']:.6f}, level={r['new_level']!r}")

    print(f"\nDone. {len(updatable)} row(s) updated in {TABLE}.")


if __name__ == "__main__":
    main()
