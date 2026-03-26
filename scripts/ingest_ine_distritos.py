"""
Ingest INE census district polygons into barrioscout_raw.neighborhoods.

For 16 municipalities with >1 census district, replaces the single
municipality polygon with individual district polygons. Each district
becomes a separate row with zone_type='metro_neighborhood' (derived
downstream by stg_neighborhoods via code pattern 'metro_%_d%').

Source: scripts/ine_distritos/all_distritos.geojson (INE geoserver, WGS84)
Target: barrioscout_raw.neighborhoods
"""

import json
import re
from pathlib import Path

from google.cloud import bigquery
from shapely.geometry import shape, MultiPolygon

PROJECT = "portfolio-alvartgil91"
TABLE = f"{PROJECT}.barrioscout_raw.neighborhoods"

GEOJSON_PATH = Path(__file__).parent / "ine_distritos" / "all_distritos.geojson"

# Map INE NMUN ("Surname, Article" format) to raw city name
INE_NAME_TO_RAW_CITY: dict[str, str] = {
    "Escorial, El": "El Escorial",
    "Zubia, La": "La Zubia",
    "Gabias, Las": "Las Gabias",
    "Rozas de Madrid, Las": "Las Rozas de Madrid",
}


def _normalize_slug(name: str) -> str:
    """Generate a slug from a municipality name, matching existing metro_ codes."""
    s = name.lower()
    s = s.replace("á", "a").replace("é", "e").replace("í", "i")
    s = s.replace("ó", "o").replace("ú", "u")
    s = s.replace("ñ", "n").replace("ü", "u")
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s


def _geojson_to_wkt(geom_dict: dict) -> str:
    """Convert GeoJSON geometry dict to WKT string."""
    geom = shape(geom_dict)
    if not geom.is_valid:
        geom = geom.buffer(0)
    # Simplify MultiPolygon with 1 polygon to Polygon
    if isinstance(geom, MultiPolygon) and len(list(geom.geoms)) == 1:
        geom = list(geom.geoms)[0]
    return geom.wkt


def main() -> None:
    print("=== INE Census District Ingestion ===\n")

    # --- Load GeoJSON ---
    with open(GEOJSON_PATH, encoding="utf-8") as f:
        features = json.load(f)["features"]
    print(f"Loaded {len(features)} features from {GEOJSON_PATH.name}")

    # --- Group by municipality, filter to multi-district ---
    by_cumun: dict[str, list[dict]] = {}
    for feat in features:
        cumun = feat["properties"]["CUMUN"]
        by_cumun.setdefault(cumun, []).append(feat)

    multi = {k: sorted(v, key=lambda f: f["properties"]["CDIS"])
             for k, v in by_cumun.items() if len(v) > 1}

    total_districts = sum(len(v) for v in multi.values())
    print(f"{len(multi)} municipalities with >1 district ({total_districts} total)\n")

    # --- Build records and validate ---
    all_records: dict[str, list[dict]] = {}  # raw_city -> [records]

    print(f"{'Municipality':<30} {'Code':<15} {'Dists':>5} {'Total km2':>10} {'Avg km2':>8}")
    print("-" * 72)

    for cumun, feats in sorted(multi.items()):
        nmun_ine = feats[0]["properties"]["NMUN"]
        raw_city = INE_NAME_TO_RAW_CITY.get(nmun_ine, nmun_ine)
        slug = _normalize_slug(raw_city)
        old_code = f"metro_{slug}"

        total_area = sum(f["properties"].get("area_km2", 0) for f in feats)
        avg_area = total_area / len(feats)
        print(f"{raw_city:<30} {old_code:<15} {len(feats):>5} {total_area:>10.1f} {avg_area:>8.1f}")

        records = []
        for feat in feats:
            props = feat["properties"]
            cdis = props["CDIS"]
            wkt = _geojson_to_wkt(feat["geometry"])

            # Validate centroid in expected range
            geom = shape(feat["geometry"])
            lon, lat = geom.centroid.x, geom.centroid.y
            if not (-5.0 <= lon <= -3.0 and 37.0 <= lat <= 41.0):
                print(f"  WARNING: {raw_city} Distrito {cdis} centroid "
                      f"({lon:.4f}, {lat:.4f}) outside expected range!")

            records.append({
                "city": raw_city,
                "level": "neighborhood",
                "name": f"Distrito {cdis}",
                "code": f"metro_{slug}_d{cdis}",
                "district_name": raw_city,
                "geometry_wkt": wkt,
            })

        all_records[raw_city] = records

    # --- Ingest to BigQuery ---
    print("\n--- Ingesting to BigQuery ---")
    client = bigquery.Client(project=PROJECT)

    total_deleted = 0
    total_inserted = 0

    for city, records in sorted(all_records.items()):
        slug = _normalize_slug(city)
        old_code = f"metro_{slug}"

        # Delete the old single-municipality record
        delete_query = f"DELETE FROM `{TABLE}` WHERE code = @old_code"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("old_code", "STRING", old_code),
            ]
        )
        result = client.query(delete_query, job_config=job_config).result()
        deleted = result.num_dml_affected_rows or 0
        total_deleted += deleted

        # Insert new district records via streaming insert
        rows = [
            {
                "city": rec["city"],
                "level": rec["level"],
                "name": rec["name"],
                "code": rec["code"],
                "district_name": rec["district_name"],
                "geometry_wkt": rec["geometry_wkt"],
            }
            for rec in records
        ]

        errors = client.insert_rows_json(TABLE, rows)
        if errors:
            print(f"  ERROR {city}: {errors[:200]}")
        else:
            total_inserted += len(records)
            print(f"  {city:<30} deleted={deleted}, inserted={len(records)}")

    print(f"\nTotal: deleted {total_deleted}, inserted {total_inserted}")

    # --- Post-ingestion verification ---
    print("\n--- Verification ---")
    verify_query = f"""
        SELECT city, COUNT(*) as n, STRING_AGG(code, ', ' ORDER BY code) as codes
        FROM `{TABLE}`
        WHERE code LIKE 'metro\\_%\\_d%'
        GROUP BY city
        ORDER BY n DESC, city
    """
    rows = list(client.query(verify_query).result())
    print(f"{'City':<30} {'N':>3}  Codes")
    print("-" * 80)
    for row in rows:
        print(f"{row.city:<30} {row.n:>3}  {row.codes}")
    total_new = sum(row.n for row in rows)
    print(f"\nTotal new district records: {total_new}")

    # Count remaining single-municipality records
    remaining_query = f"""
        SELECT COUNT(*) as n
        FROM `{TABLE}`
        WHERE code LIKE 'metro\\_%' AND code NOT LIKE 'metro\\_%\\_d%'
    """
    remaining = list(client.query(remaining_query).result())[0].n
    print(f"Remaining single-municipality records: {remaining}")
    print(f"Expected: {51 - len(all_records)} (51 original - {len(all_records)} subdivided)")

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
