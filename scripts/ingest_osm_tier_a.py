"""
Ingest OSM polygon subdivisions for Tier A municipalities.

Tier A: municipalities with complete admin_level boundary polygons in OSM.
- Pozuelo de Alarcón: 20 admin10 polygons (100% coverage)
- Collado Villalba: 4 admin9 polygons (36.7% coverage) + "Resto" gap polygon

Source: scripts/osm_21_municipios/*.geojson (pre-downloaded from Overpass)
Target: barrioscout_raw.neighborhoods
"""

import json
import re
from pathlib import Path

from google.cloud import bigquery
from shapely.geometry import shape, MultiPolygon
from shapely.ops import unary_union
from shapely import wkt as shapely_wkt

PROJECT = "portfolio-alvartgil91"
TABLE = f"{PROJECT}.barrioscout_raw.neighborhoods"

OSM_DIR = Path(__file__).parent / "osm_21_municipios"
MUNI_POLYGONS = Path(__file__).parent / "municipal_polygons.json"


def _normalize_slug(name: str) -> str:
    """Generate a slug from a name."""
    s = name.lower()
    s = s.replace("á", "a").replace("é", "e").replace("í", "i")
    s = s.replace("ó", "o").replace("ú", "u")
    s = s.replace("ñ", "n").replace("ü", "u")
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s


def _shape_to_wkt(geom) -> str:
    """Convert shapely geometry to WKT, simplifying single-part MultiPolygons."""
    if not geom.is_valid:
        geom = geom.buffer(0)
    if isinstance(geom, MultiPolygon) and len(list(geom.geoms)) == 1:
        geom = list(geom.geoms)[0]
    return geom.wkt


def _load_municipal_polygon(city_name: str):
    """Load the original municipal polygon from municipal_polygons.json."""
    with open(MUNI_POLYGONS, encoding="utf-8") as f:
        munis = json.load(f)
    for m in munis:
        if m.get("city", "").lower() == city_name.lower():
            return shapely_wkt.loads(m["geometry_wkt"])
    return None


def process_pozuelo() -> list[dict]:
    """Process Pozuelo de Alarcón: 20 admin10 polygons, 100% coverage."""
    raw_city = "Pozuelo de Alarcón"
    muni_slug = _normalize_slug(raw_city)

    with open(OSM_DIR / "28115_Pozuelo_de_Alarcón.geojson", encoding="utf-8") as f:
        data = json.load(f)

    records = []
    for feat in data["features"]:
        name = feat["properties"]["name"]
        geom = shape(feat["geometry"])
        barrio_slug = _normalize_slug(name)
        wkt = _shape_to_wkt(geom)
        area_km2 = geom.area * 111.32**2

        records.append({
            "city": raw_city,
            "level": "neighborhood",
            "name": name,
            "code": f"metro_{muni_slug}_{barrio_slug}",
            "district_name": raw_city,
            "geometry_wkt": wkt,
            "_area_km2": area_km2,
        })

    # Verify coverage
    all_geoms = [shape(f["geometry"]) for f in data["features"]]
    all_geoms = [g.buffer(0) if not g.is_valid else g for g in all_geoms]
    union = unary_union(all_geoms)
    union_area = union.area * 111.32**2

    print(f"\nPozuelo de Alarcón: {len(records)} polygons, union area {union_area:.1f} km2")
    for r in records:
        print(f"  {r['name']:<50} {r['_area_km2']:>6.2f} km2  code={r['code']}")

    return records


def process_collado_villalba() -> list[dict]:
    """Process Collado Villalba: 4 admin9 + 'Resto' gap polygon."""
    raw_city = "Collado Villalba"
    muni_slug = _normalize_slug(raw_city)

    with open(OSM_DIR / "28047_Collado_Villalba.geojson", encoding="utf-8") as f:
        data = json.load(f)

    # Filter admin9 only
    admin9_feats = [f for f in data["features"]
                    if f["properties"].get("admin_level") == "9"]

    records = []
    admin9_geoms = []
    for feat in admin9_feats:
        name = feat["properties"]["name"]
        geom = shape(feat["geometry"])
        if not geom.is_valid:
            geom = geom.buffer(0)
        admin9_geoms.append(geom)
        barrio_slug = _normalize_slug(name)
        wkt = _shape_to_wkt(geom)
        area_km2 = geom.area * 111.32**2

        records.append({
            "city": raw_city,
            "level": "neighborhood",
            "name": name,
            "code": f"metro_{muni_slug}_{barrio_slug}",
            "district_name": raw_city,
            "geometry_wkt": wkt,
            "_area_km2": area_km2,
        })

    # Compute gap polygon
    admin9_union = unary_union(admin9_geoms)
    muni_geom = _load_municipal_polygon("Collado Villalba")
    if muni_geom is None:
        print("  WARNING: Municipal polygon not found for Collado Villalba!")
    else:
        if not muni_geom.is_valid:
            muni_geom = muni_geom.buffer(0)
        gap = muni_geom.difference(admin9_union)
        if not gap.is_valid:
            gap = gap.buffer(0)
        gap_area = gap.area * 111.32**2
        muni_area = muni_geom.area * 111.32**2

        if gap_area > 0.5:
            wkt = _shape_to_wkt(gap)
            records.append({
                "city": raw_city,
                "level": "neighborhood",
                "name": "Resto de Collado Villalba",
                "code": f"metro_{muni_slug}_resto",
                "district_name": raw_city,
                "geometry_wkt": wkt,
                "_area_km2": gap_area,
            })
            print(f"\n  Gap polygon: {gap_area:.1f} km2 ({gap_area/muni_area*100:.1f}% of municipality)")

    admin9_area = sum(g.area * 111.32**2 for g in admin9_geoms)
    total_area = sum(r["_area_km2"] for r in records)

    print(f"\nCollado Villalba: {len(records)} polygons (4 admin9 + resto), total {total_area:.1f} km2")
    for r in records:
        print(f"  {r['name']:<50} {r['_area_km2']:>6.2f} km2  code={r['code']}")

    return records


def ingest(records: list[dict], raw_city: str, old_codes: list[str]) -> None:
    """Delete old records and insert new ones into BigQuery."""
    client = bigquery.Client(project=PROJECT)

    # Delete old records
    total_deleted = 0
    for old_code in old_codes:
        delete_query = f"DELETE FROM `{TABLE}` WHERE code = @old_code"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("old_code", "STRING", old_code),
            ]
        )
        result = client.query(delete_query, job_config=job_config).result()
        deleted = result.num_dml_affected_rows or 0
        total_deleted += deleted

    # Insert new records via DML (avoids streaming buffer issues)
    values_clauses = []
    params = []
    for i, r in enumerate(records):
        p_city = f"city_{i}"
        p_name = f"name_{i}"
        p_code = f"code_{i}"
        p_dist = f"dist_{i}"
        p_wkt = f"wkt_{i}"
        values_clauses.append(
            f"(@{p_city}, 'neighborhood', @{p_name}, @{p_code}, @{p_dist}, @{p_wkt})"
        )
        params.extend([
            bigquery.ScalarQueryParameter(p_city, "STRING", r["city"]),
            bigquery.ScalarQueryParameter(p_name, "STRING", r["name"]),
            bigquery.ScalarQueryParameter(p_code, "STRING", r["code"]),
            bigquery.ScalarQueryParameter(p_dist, "STRING", r["district_name"]),
            bigquery.ScalarQueryParameter(p_wkt, "STRING", r["geometry_wkt"]),
        ])

    insert_query = (
        f"INSERT INTO `{TABLE}` (city, level, name, code, district_name, geometry_wkt) "
        f"VALUES {', '.join(values_clauses)}"
    )
    insert_config = bigquery.QueryJobConfig(query_parameters=params)
    client.query(insert_query, job_config=insert_config).result()
    print(f"  {raw_city}: deleted {total_deleted} old, inserted {len(records)} new")


def main() -> None:
    print("=== OSM Tier A Ingestion ===\n")

    # Process both municipalities
    pozuelo_records = process_pozuelo()
    collado_records = process_collado_villalba()

    # Ingest Pozuelo (replace 2 INE districts)
    print("\n--- Ingesting to BigQuery ---")
    ingest(
        pozuelo_records,
        "Pozuelo de Alarcón",
        ["metro_pozuelo_de_alarcon_d01", "metro_pozuelo_de_alarcon_d02"],
    )

    # Ingest Collado Villalba (replace 1 municipality record)
    ingest(
        collado_records,
        "Collado Villalba",
        ["metro_collado_villalba"],
    )

    # Verification
    print("\n--- Verification ---")
    client = bigquery.Client(project=PROJECT)

    for city_pattern in ["Pozuelo%", "Collado%"]:
        query = f"""
            SELECT city, code, name
            FROM `{TABLE}`
            WHERE city LIKE @pattern AND code LIKE 'metro_%'
            ORDER BY code
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("pattern", "STRING", city_pattern),
            ]
        )
        rows = list(client.query(query, job_config=job_config).result())
        print(f"\n{rows[0].city if rows else city_pattern}: {len(rows)} records")
        for row in rows:
            print(f"  {row.code:<55} {row.name}")

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
