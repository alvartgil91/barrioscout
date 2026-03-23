"""
One-time re-geocoding script: replaces Nominatim coordinates with Google Maps
Geocoding API coordinates for all rows in barrioscout_raw.idealista_listings.

Usage:
    cd /path/to/barrioscout
    python scripts/regeocode_with_google.py

Requirements:
    - GOOGLE_GEOCODING_API_KEY set in .env (or environment)
    - GCP credentials configured (ADC or GOOGLE_APPLICATION_CREDENTIALS)
"""

from __future__ import annotations

import datetime
import math
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import bigquery

# Load .env from project root (two levels up from scripts/)
load_dotenv(Path(__file__).parent.parent / ".env")

import os

GCP_PROJECT = "portfolio-alvartgil91"
TABLE_ID = f"{GCP_PROJECT}.barrioscout_raw.idealista_listings"
GOOGLE_GEOCODING_URL = "https://maps.googleapis.com/maps/api/geocode/json"
SLEEP_BETWEEN_REQUESTS = 0.05  # 20 QPS — well within Google free tier
MAX_RETRIES = 3


def _haversine_meters(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return distance in metres between two lat/lon points."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _google_geocode(address: str, city: str, api_key: str) -> dict:
    """Call Google Geocoding API. Returns a result dict with keys:
        lat_new, lon_new, geocode_level, geocode_query

    Never raises — errors are encoded in geocode_level.
    """
    query = f"{address}, {city}, Spain"
    params = {"address": query, "key": api_key}

    data: dict = {}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(GOOGLE_GEOCODING_URL, params=params, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            break
        except Exception as exc:
            time.sleep(SLEEP_BETWEEN_REQUESTS)
            if attempt == MAX_RETRIES:
                print(f"  [ERROR] HTTP failure after {MAX_RETRIES} retries for '{query}': {exc}")
                return {"lat_new": None, "lon_new": None, "geocode_level": "HTTP_ERROR", "geocode_query": query}
            wait = 2 ** attempt
            print(f"  [WARN] Attempt {attempt} failed ({exc}), retrying in {wait}s…")
            time.sleep(wait)

    status = data.get("status")
    results = data.get("results", [])

    if status == "ZERO_RESULTS" or not results:
        return {"lat_new": None, "lon_new": None, "geocode_level": "NO_RESULT", "geocode_query": query}

    if status != "OK":
        print(f"  [WARN] Unexpected status '{status}' for '{query}'")
        return {"lat_new": None, "lon_new": None, "geocode_level": f"STATUS_{status}", "geocode_query": query}

    location = results[0]["geometry"]["location"]
    location_type = results[0]["geometry"].get("location_type", "UNKNOWN")
    return {
        "lat_new": float(location["lat"]),
        "lon_new": float(location["lng"]),
        "geocode_level": location_type,
        "geocode_query": query,
    }


def _ensure_geocode_level_column(client: bigquery.Client) -> None:
    """Add geocode_level STRING column to idealista_listings if it doesn't exist."""
    sql = f"""
        ALTER TABLE `{TABLE_ID}`
        ADD COLUMN IF NOT EXISTS geocode_level STRING
    """
    client.query(sql).result()
    print("  [BQ] Ensured geocode_level column exists.")


def _read_listings(client: bigquery.Client) -> pd.DataFrame:
    """Read property_id, email_id, address, city, lat, lon from BQ."""
    sql = f"""
        SELECT property_id, email_id, address, city, lat, lon
        FROM `{TABLE_ID}`
    """
    return client.query(sql).to_dataframe()


def _write_temp_table(client: bigquery.Client, results_df: pd.DataFrame) -> str:
    """Write geocoding results to a temp BQ table. Returns the full table ref."""
    temp_table = f"{GCP_PROJECT}.barrioscout_raw._temp_regeocode"
    schema = [
        bigquery.SchemaField("property_id", "STRING"),
        bigquery.SchemaField("email_id", "STRING"),
        bigquery.SchemaField("lat_new", "FLOAT"),
        bigquery.SchemaField("lon_new", "FLOAT"),
        bigquery.SchemaField("geocode_level", "STRING"),
        bigquery.SchemaField("geocode_query", "STRING"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_dataframe(results_df, temp_table, job_config=job_config)
    job.result()
    print(f"  [BQ] Wrote {len(results_df)} rows to temp table {temp_table}")
    return temp_table


def _merge_into_main(client: bigquery.Client, temp_table: str) -> None:
    """MERGE new coordinates from temp table into the main listings table."""
    sql = f"""
        MERGE `{TABLE_ID}` AS target
        USING `{temp_table}` AS src
        ON target.property_id = src.property_id AND target.email_id = src.email_id
        WHEN MATCHED THEN UPDATE SET
            target.lat = src.lat_new,
            target.lon = src.lon_new,
            target.geocode_level = src.geocode_level
    """
    job = client.query(sql)
    job.result()
    print(f"  [BQ] MERGE complete.")


def _drop_temp_table(client: bigquery.Client, temp_table: str) -> None:
    client.query(f"DROP TABLE IF EXISTS `{temp_table}`").result()
    print(f"  [BQ] Dropped temp table {temp_table}")


def main() -> None:
    api_key = os.getenv("GOOGLE_GEOCODING_API_KEY", "")
    if not api_key:
        print("ERROR: GOOGLE_GEOCODING_API_KEY not set. Add it to .env and retry.")
        sys.exit(1)

    client = bigquery.Client(project=GCP_PROJECT)

    print("=== BarrioScout: Re-geocode with Google Maps API ===\n")

    # 1. Ensure column exists
    _ensure_geocode_level_column(client)

    # 2. Read all listings
    print("Reading listings from BigQuery…")
    df = _read_listings(client)
    total = len(df)
    print(f"  {total} listings to process\n")

    # 3. Geocode each listing
    results: list[dict] = []
    for i, row in enumerate(df.itertuples(index=False), start=1):
        address = row.address or ""
        city = row.city or ""

        if not address or not city:
            results.append({
                "property_id": row.property_id,
                "email_id": row.email_id,
                "lat_new": None,
                "lon_new": None,
                "geocode_level": "NO_ADDRESS",
                "geocode_query": "",
            })
        else:
            geo = _google_geocode(address, city, api_key)
            results.append({
                "property_id": row.property_id,
                "email_id": row.email_id,
                **geo,
            })

        if i % 100 == 0:
            print(f"  Progress: {i}/{total} ({i/total*100:.1f}%)")

    print(f"\nGeocoding complete: {total} listings processed.\n")

    # 4. Build results DataFrame
    results_df = pd.DataFrame(results)

    # 5. Save CSV backup
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = Path(__file__).parent / f"regeocode_results_{timestamp}.csv"
    results_df.to_csv(backup_path, index=False)
    print(f"Backup saved: {backup_path}\n")

    # 6. Summary stats
    level_counts = results_df["geocode_level"].value_counts()
    print("Geocode level distribution:")
    for level, count in level_counts.items():
        print(f"  {level}: {count}")

    # Count significant coordinate changes (>100m)
    merged = df.merge(results_df, on=["property_id", "email_id"])
    changed_significantly = 0
    null_to_coord = 0
    for _, r in merged.iterrows():
        old_lat, old_lon = r["lat"], r["lon"]
        new_lat, new_lon = r["lat_new"], r["lon_new"]
        if pd.isna(old_lat) and pd.notna(new_lat):
            null_to_coord += 1
        elif pd.notna(old_lat) and pd.notna(new_lat):
            dist = _haversine_meters(float(old_lat), float(old_lon), float(new_lat), float(new_lon))
            if dist > 100:
                changed_significantly += 1

    print(f"\nCoordinate changes:")
    print(f"  Moved >100m from original: {changed_significantly}")
    print(f"  NULL → valid coordinate:   {null_to_coord}")

    # 7. Write to BQ via temp table + MERGE
    print("\nWriting results to BigQuery…")
    temp_table = _write_temp_table(client, results_df)
    _merge_into_main(client, temp_table)
    _drop_temp_table(client, temp_table)

    print(f"\n=== Done. {total} rows updated. ===")


if __name__ == "__main__":
    main()
