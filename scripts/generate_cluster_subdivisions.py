"""
Subdivide 10 metro municipalities by clustering census sections (Tier C).

Uses KMeans on section centroids, then unions each cluster's geometries into
a single polygon. Names are assigned by geographic direction (Norte, Sur, etc.).

Source: scripts/ine_secciones_21/*.geojson (INE census sections, WGS84)
Target: barrioscout_raw.neighborhoods
"""

import json
import math
import re
from pathlib import Path
from typing import Any

import numpy as np
from google.cloud import bigquery
from shapely.geometry import MultiPolygon, Polygon, shape
from shapely.ops import unary_union
from sklearn.cluster import KMeans

PROJECT = "portfolio-alvartgil91"
TABLE = f"{PROJECT}.barrioscout_raw.neighborhoods"
SECTIONS_DIR = Path(__file__).resolve().parent / "ine_secciones_21"

TIER_C: dict[str, dict[str, Any]] = {
    "Alcobendas":              {"cod_ine": "28006", "n_clusters": 6, "raw_city": "Alcobendas"},
    "Rivas-Vaciamadrid":       {"cod_ine": "28123", "n_clusters": 7, "raw_city": "Rivas-Vaciamadrid"},
    "Arganda del Rey":         {"cod_ine": "28014", "n_clusters": 5, "raw_city": "Arganda del Rey"},
    "Valdemoro":               {"cod_ine": "28161", "n_clusters": 5, "raw_city": "Valdemoro"},
    "San Fernando de Henares": {"cod_ine": "28130", "n_clusters": 4, "raw_city": "San Fernando de Henares"},
    "Tres Cantos":             {"cod_ine": "28903", "n_clusters": 5, "raw_city": "Tres Cantos"},
    "Mejorada del Campo":      {"cod_ine": "28084", "n_clusters": 3, "raw_city": "Mejorada del Campo"},
    "Atarfe":                  {"cod_ine": "18022", "n_clusters": 4, "raw_city": "Atarfe"},
    "Las Gabias":              {"cod_ine": "18905", "n_clusters": 4, "raw_city": "Las Gabias"},
    "Peligros":                {"cod_ine": "18153", "n_clusters": 3, "raw_city": "Peligros"},
}

# File naming: cod_ine -> filename in ine_secciones_21/
FILE_MAP: dict[str, str] = {
    "28006": "28006_Alcobendas.geojson",
    "28123": "28123_Rivas-Vaciamadrid.geojson",
    "28014": "28014_Arganda_del_Rey.geojson",
    "28161": "28161_Valdemoro.geojson",
    "28130": "28130_San_Fernando_de_Henares.geojson",
    "28903": "28903_Tres_Cantos.geojson",
    "28084": "28084_Mejorada_del_Campo.geojson",
    "18022": "18022_Atarfe.geojson",
    "18905": "18905_Las_Gabias.geojson",
    "18153": "18153_Peligros.geojson",
}

# Direction names in Spanish
DIRECTIONS_8 = ["Este", "Noreste", "Norte", "Noroeste", "Oeste", "Suroeste", "Sur", "Sureste"]


def _normalize_slug(name: str) -> str:
    s = name.lower()
    for old, new in [("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),("ñ","n"),("ü","u")]:
        s = s.replace(old, new)
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")


def _area_km2(geom: Polygon | MultiPolygon) -> float:
    bounds = geom.bounds
    mid_lat = (bounds[1] + bounds[3]) / 2.0
    return geom.area * 111.0 * 111.0 * math.cos(math.radians(mid_lat))


def _validate(geom):
    if not geom.is_valid:
        geom = geom.buffer(0)
    return geom


def _load_sections(cod_ine: str) -> list[tuple[Polygon | MultiPolygon, dict]]:
    """Load census section geometries from GeoJSON. Returns [(geom, properties), ...]."""
    fname = FILE_MAP.get(cod_ine)
    if not fname:
        return []
    path = SECTIONS_DIR / fname
    if not path.exists():
        print(f"  WARNING: File not found: {path}")
        return []

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    sections = []
    for feat in data["features"]:
        if feat["properties"].get("TIPO") != "SECCIONADO":
            continue
        geom = shape(feat["geometry"])
        geom = _validate(geom)
        sections.append((geom, feat["properties"]))
    return sections


def _cluster_sections(
    sections: list[tuple[Polygon | MultiPolygon, dict]], n_clusters: int
) -> list[tuple[Polygon | MultiPolygon, int]]:
    """Cluster sections by centroid using KMeans. Returns [(geom, cluster_id), ...]."""
    # Adjust n_clusters if we have fewer sections
    n_clusters = min(n_clusters, len(sections))

    centroids = np.array([[s[0].centroid.x, s[0].centroid.y] for s in sections])

    # Scale longitude by cos(lat) for better distance metric
    mid_lat = np.mean(centroids[:, 1])
    scaled = centroids.copy()
    scaled[:, 0] *= math.cos(math.radians(mid_lat))

    km = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = km.fit_predict(scaled)

    return [(sections[i][0], int(labels[i])) for i in range(len(sections))]


def _build_cluster_polygons(
    clustered: list[tuple[Polygon | MultiPolygon, int]]
) -> dict[int, Polygon | MultiPolygon]:
    """Union sections by cluster, returning {cluster_id: polygon}."""
    by_cluster: dict[int, list] = {}
    for geom, cid in clustered:
        by_cluster.setdefault(cid, []).append(geom)

    result = {}
    for cid, geoms in by_cluster.items():
        union = _validate(unary_union(geoms))
        # If MultiPolygon, keep it (sections may form islands)
        result[cid] = union
    return result


def _assign_direction_names(
    cluster_polys: dict[int, Polygon | MultiPolygon],
    muni_centroid: tuple[float, float],
) -> dict[int, str]:
    """Assign direction names based on cluster centroid position relative to municipality center."""
    cx, cy = muni_centroid
    cos_lat = math.cos(math.radians(cy))

    # Calculate angle and distance for each cluster
    cluster_info: list[tuple[int, float, float]] = []  # (cid, angle_deg, distance)
    for cid, poly in cluster_polys.items():
        pc = poly.centroid
        dx = (pc.x - cx) * cos_lat
        dy = pc.y - cy
        angle = math.degrees(math.atan2(dy, dx))  # 0=E, 90=N, -90=S, 180=W
        dist = math.sqrt(dx**2 + dy**2)
        cluster_info.append((cid, angle, dist))

    # Determine "Centro" threshold: 25% of max distance
    max_dist = max(d for _, _, d in cluster_info) if cluster_info else 0
    centro_threshold = max_dist * 0.25

    names: dict[int, str] = {}
    used_names: dict[str, int] = {}  # name -> count

    # First pass: assign Centro for close-to-center clusters
    for cid, angle, dist in cluster_info:
        if dist < centro_threshold and "Centro" not in used_names:
            names[cid] = "Centro"
            used_names["Centro"] = 1

    # Second pass: assign direction names for remaining
    for cid, angle, dist in sorted(cluster_info, key=lambda x: -x[2]):
        if cid in names:
            continue

        # Map angle to 8-direction index
        # angle: 0=E, 90=N. Bin into 8 sectors of 45° each
        idx = round(angle / 45.0) % 8
        base_name = DIRECTIONS_8[idx]

        if base_name not in used_names:
            names[cid] = base_name
            used_names[base_name] = 1
        else:
            # Try adjacent directions
            found = False
            for offset in [1, -1, 2, -2]:
                alt_idx = (idx + offset) % 8
                alt_name = DIRECTIONS_8[alt_idx]
                if alt_name not in used_names:
                    names[cid] = alt_name
                    used_names[alt_name] = 1
                    found = True
                    break
            if not found:
                # Fallback: add numeric suffix
                count = used_names.get(base_name, 0) + 1
                names[cid] = f"{base_name} {count}"
                used_names[base_name] = count

    return names


def _merge_small_clusters(
    cluster_polys: dict[int, Polygon | MultiPolygon],
    names: dict[int, str],
    min_area: float = 0.5,
) -> tuple[dict[int, Polygon | MultiPolygon], dict[int, str]]:
    """Merge clusters smaller than min_area km² into nearest neighbor."""
    changed = True
    while changed:
        changed = False
        small = [(cid, p) for cid, p in cluster_polys.items() if _area_km2(p) < min_area]
        if not small or len(cluster_polys) <= 1:
            break
        for cid, poly in small:
            if len(cluster_polys) <= 1:
                break
            # Find nearest neighbor
            pc = poly.centroid
            best_cid = None
            best_dist = float("inf")
            for other_cid, other_poly in cluster_polys.items():
                if other_cid == cid:
                    continue
                d = pc.distance(other_poly.centroid)
                if d < best_dist:
                    best_dist = d
                    best_cid = other_cid
            if best_cid is not None:
                # Merge
                merged = _validate(unary_union([poly, cluster_polys[best_cid]]))
                cluster_polys[best_cid] = merged
                del cluster_polys[cid]
                del names[cid]
                changed = True
                break  # Restart loop after modification

    return cluster_polys, names


def process_municipality(
    muni_name: str, config: dict, client: bigquery.Client
) -> list[dict]:
    """Process a single municipality: load sections, cluster, name, build records."""
    cod_ine = config["cod_ine"]
    n_clusters = config["n_clusters"]
    raw_city = config["raw_city"]
    muni_slug = _normalize_slug(raw_city)

    print(f"\n{'='*60}")
    print(f"Processing: {muni_name} (cod_ine={cod_ine}, target={n_clusters} clusters)")
    print(f"{'='*60}")

    # Load sections
    sections = _load_sections(cod_ine)
    if not sections:
        print(f"  ERROR: No sections found, skipping")
        return []
    print(f"  Loaded {len(sections)} census sections")

    # Total area
    total_area = _area_km2(_validate(unary_union([s[0] for s in sections])))
    print(f"  Total area: {total_area:.1f} km²")

    # Cluster
    clustered = _cluster_sections(sections, n_clusters)
    cluster_polys = _build_cluster_polygons(clustered)
    print(f"  Initial clusters: {len(cluster_polys)}")

    # Municipal centroid (from union of all sections)
    all_union = _validate(unary_union([s[0] for s in sections]))
    muni_centroid = (all_union.centroid.x, all_union.centroid.y)

    # Name clusters
    names = _assign_direction_names(cluster_polys, muni_centroid)

    # Merge small clusters
    cluster_polys, names = _merge_small_clusters(cluster_polys, names)
    print(f"  Final clusters: {len(cluster_polys)}")

    # Print summary
    for cid in sorted(cluster_polys.keys()):
        area = _area_km2(cluster_polys[cid])
        n_secs = sum(1 for _, c in clustered if c == cid)
        gtype = cluster_polys[cid].geom_type
        print(f"    {names[cid]:<20} {area:>6.1f} km²  ({n_secs} sections, {gtype})")

    # Build records
    records = []
    for cid in sorted(cluster_polys.keys()):
        zone_name = names[cid]
        zone_slug = _normalize_slug(zone_name)
        geom = cluster_polys[cid]
        wkt = geom.wkt

        records.append({
            "city": raw_city,
            "level": "neighborhood",
            "name": zone_name,
            "code": f"metro_{muni_slug}_{zone_slug}",
            "district_name": raw_city,
            "geometry_wkt": wkt,
        })

    return records


def ingest_records(client: bigquery.Client, records: list[dict], raw_city: str) -> None:
    """Delete old rows and insert new records via DML."""
    muni_slug = _normalize_slug(raw_city)

    # Delete old records (both whole municipality and any previous subdivisions)
    delete_query = f"""
        DELETE FROM `{TABLE}`
        WHERE code = 'metro_{muni_slug}'
           OR code LIKE 'metro_{muni_slug}_%'
    """
    print(f"  Deleting old rows...")
    client.query(delete_query).result()

    if not records:
        return

    # DML INSERT
    values_clauses = []
    params = []
    for i, rec in enumerate(records):
        values_clauses.append(
            f"(@city_{i}, 'neighborhood', @name_{i}, @code_{i}, @dist_{i}, @wkt_{i})"
        )
        params.extend([
            bigquery.ScalarQueryParameter(f"city_{i}", "STRING", rec["city"]),
            bigquery.ScalarQueryParameter(f"name_{i}", "STRING", rec["name"]),
            bigquery.ScalarQueryParameter(f"code_{i}", "STRING", rec["code"]),
            bigquery.ScalarQueryParameter(f"dist_{i}", "STRING", rec["district_name"]),
            bigquery.ScalarQueryParameter(f"wkt_{i}", "STRING", rec["geometry_wkt"]),
        ])

    insert_query = (
        f"INSERT INTO `{TABLE}` (city, level, name, code, district_name, geometry_wkt) "
        f"VALUES {', '.join(values_clauses)}"
    )
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    client.query(insert_query, job_config=job_config).result()
    print(f"  Inserted {len(records)} rows")


def main() -> None:
    print("=== Census Section Clustering (Tier C) ===\n")
    client = bigquery.Client(project=PROJECT)

    total_zones = 0
    for muni_name, config in TIER_C.items():
        records = process_municipality(muni_name, config, client)
        if records:
            ingest_records(client, records, config["raw_city"])
            total_zones += len(records)

    print(f"\n{'='*60}")
    print(f"Done! Created {total_zones} zones across {len(TIER_C)} municipalities.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
