"""Generate Voronoi polygon subdivisions for 9 metro municipalities and ingest into BigQuery.

For each municipality, OSM place nodes (suburbs/neighbourhoods) are used as seed points.
Voronoi tessellation splits the municipal polygon into named zones. Optionally, an urban
clipping step (based on Catastro building density) separates built-up areas from rural
periphery.

Usage:
    python scripts/generate_voronoi_subdivisions.py
"""

import csv
import json
import re
import time
from pathlib import Path
from typing import Any

import numpy as np
import requests
from google.cloud import bigquery
from scipy.spatial import Voronoi
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiPolygon,
    Point,
    Polygon,
    box,
)
from shapely.ops import unary_union
from shapely import wkt as shapely_wkt

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROJECT = "portfolio-alvartgil91"
TABLE = f"{PROJECT}.barrioscout_raw.neighborhoods"

SCRIPTS_DIR = Path(__file__).resolve().parent
NODOS_CSV = SCRIPTS_DIR / "osm_21_municipios" / "nodos.csv"
POLYGONS_JSON = SCRIPTS_DIR / "municipal_polygons.json"

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_RETRIES = 3
OVERPASS_BACKOFF = [10, 30, 60]

TIER_B: dict[str, dict[str, Any]] = {
    "Parla": {"cod_ine": "28106", "urban_clip": False, "raw_city": "Parla"},
    "San Sebastián de los Reyes": {
        "cod_ine": "28134",
        "urban_clip": True,
        "raw_city": "San Sebastián de los Reyes",
    },
    "Boadilla del Monte": {
        "cod_ine": "28022",
        "urban_clip": True,
        "raw_city": "Boadilla del Monte",
    },
    "Las Rozas de Madrid": {
        "cod_ine": "28127",
        "urban_clip": True,
        "raw_city": "Las Rozas de Madrid",
    },
    "Pinto": {"cod_ine": "28113", "urban_clip": True, "raw_city": "Pinto"},
    "Majadahonda": {"cod_ine": "28080", "urban_clip": False, "raw_city": "Majadahonda"},
    "Navalcarnero": {
        "cod_ine": "28096",
        "urban_clip": True,
        "raw_city": "Navalcarnero",
    },
    "Colmenar Viejo": {
        "cod_ine": "28049",
        "urban_clip": True,
        "raw_city": "Colmenar Viejo",
    },
    "La Zubia": {"cod_ine": "18193", "urban_clip": False, "raw_city": "La Zubia"},
}

# Municipalities that need Overpass queries (not in nodos.csv)
OVERPASS_MUNICIPALITIES = {"28127", "28113", "28134"}

# Approximate conversion factor: 1 degree latitude ≈ 111 km
DEG_TO_KM = 111.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_slug(name: str) -> str:
    """Convert a place name to a URL/code-safe slug."""
    s = name.lower()
    for old, new in [
        ("á", "a"),
        ("é", "e"),
        ("í", "i"),
        ("ó", "o"),
        ("ú", "u"),
        ("ñ", "n"),
        ("ü", "u"),
    ]:
        s = s.replace(old, new)
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s


def _area_km2(geom: Polygon | MultiPolygon) -> float:
    """Rough area in km² using a simple lat/lon → km approximation."""
    bounds = geom.bounds  # (minx, miny, maxx, maxy)
    mid_lat = (bounds[1] + bounds[3]) / 2.0
    deg_lon_km = DEG_TO_KM * np.cos(np.radians(mid_lat))
    # Scale factor: area in degree² → km²
    return geom.area * DEG_TO_KM * deg_lon_km


def _ensure_polygon(geom: Any) -> Polygon | MultiPolygon | None:
    """Extract polygon(s) from a geometry, discarding lines/points."""
    if geom.is_empty:
        return None
    if isinstance(geom, (Polygon, MultiPolygon)):
        return geom
    if isinstance(geom, GeometryCollection):
        polys = [g for g in geom.geoms if isinstance(g, (Polygon, MultiPolygon))]
        if not polys:
            return None
        return unary_union(polys)
    return None


def _validate(geom: Polygon | MultiPolygon) -> Polygon | MultiPolygon:
    """Return a valid version of the geometry."""
    if not geom.is_valid:
        geom = geom.buffer(0)
    return geom


# ---------------------------------------------------------------------------
# Step 1: Load / fetch nodes
# ---------------------------------------------------------------------------


def _load_csv_nodes() -> dict[str, list[dict]]:
    """Read nodos.csv and group rows by cod_ine."""
    groups: dict[str, list[dict]] = {}
    with open(NODOS_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cod = row["cod_ine"].strip()
            groups.setdefault(cod, []).append(
                {
                    "name": row["name"].strip(),
                    "place": row["place"].strip(),
                    "lat": float(row["lat"]),
                    "lon": float(row["lon"]),
                }
            )
    return groups


def _overpass_query(name: str) -> list[dict]:
    """Query Overpass for suburb/neighbourhood nodes inside a named municipality."""
    query = (
        f'[out:json][timeout:30];\n'
        f'area["name"="{name}"]["admin_level"="8"]["boundary"="administrative"]->.searchArea;\n'
        f'(\n'
        f'  node["place"="suburb"](area.searchArea);\n'
        f'  node["place"="neighbourhood"](area.searchArea);\n'
        f');\n'
        f'out body;'
    )
    for attempt in range(OVERPASS_RETRIES):
        try:
            resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=60)
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = OVERPASS_BACKOFF[min(attempt, len(OVERPASS_BACKOFF) - 1)]
                print(f"  Overpass {resp.status_code}, retrying in {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            elements = resp.json().get("elements", [])
            nodes = []
            for el in elements:
                nodes.append(
                    {
                        "name": el.get("tags", {}).get("name", ""),
                        "place": el.get("tags", {}).get("place", ""),
                        "lat": el["lat"],
                        "lon": el["lon"],
                    }
                )
            return nodes
        except requests.RequestException as exc:
            wait = OVERPASS_BACKOFF[min(attempt, len(OVERPASS_BACKOFF) - 1)]
            print(f"  Overpass error: {exc}, retrying in {wait}s...")
            time.sleep(wait)
    print("  WARNING: Overpass query failed after all retries")
    return []


def _get_nodes(
    cod_ine: str,
    muni_name: str,
    muni_polygon: Polygon | MultiPolygon,
    csv_groups: dict[str, list[dict]],
) -> list[dict]:
    """Return deduplicated, filtered nodes for a municipality."""
    if cod_ine in OVERPASS_MUNICIPALITIES:
        print(f"  Querying Overpass for {muni_name}...")
        raw_nodes = _overpass_query(muni_name)
        time.sleep(2)
    else:
        raw_nodes = csv_groups.get(cod_ine, [])

    if not raw_nodes:
        print(f"  WARNING: No nodes found for {muni_name}")
        return []

    # Filter: must be inside municipal polygon
    nodes = [n for n in raw_nodes if muni_polygon.contains(Point(n["lon"], n["lat"]))]

    # Filter Pinto: discard Chilean false positives
    if cod_ine == "28113":
        nodes = [n for n in nodes if 39.0 <= n["lat"] <= 41.0]

    # Deduplicate by name: prefer suburb over neighbourhood, closest to centroid
    centroid = muni_polygon.centroid
    by_name: dict[str, list[dict]] = {}
    for n in nodes:
        if not n["name"]:
            continue
        by_name.setdefault(n["name"], []).append(n)

    deduped: list[dict] = []
    for name, candidates in by_name.items():
        if len(candidates) == 1:
            deduped.append(candidates[0])
            continue
        # Prefer suburb
        suburbs = [c for c in candidates if c["place"] == "suburb"]
        pool = suburbs if suburbs else candidates
        # Pick closest to centroid
        best = min(pool, key=lambda c: centroid.distance(Point(c["lon"], c["lat"])))
        deduped.append(best)

    return deduped


# ---------------------------------------------------------------------------
# Step 2: Load municipal polygons
# ---------------------------------------------------------------------------


def _load_municipal_polygons() -> dict[str, Polygon | MultiPolygon]:
    """Load municipal_polygons.json and return a dict keyed by city name (lowercase)."""
    with open(POLYGONS_JSON, encoding="utf-8") as f:
        data = json.load(f)
    result: dict[str, Polygon | MultiPolygon] = {}
    for rec in data:
        city = rec["city"]
        geom = shapely_wkt.loads(rec["geometry_wkt"])
        result[city.lower()] = _validate(geom)
    return result


def _find_polygon(
    muni_name: str, polys: dict[str, Polygon | MultiPolygon]
) -> Polygon | MultiPolygon | None:
    """Find a municipal polygon by case-insensitive name match."""
    key = muni_name.lower()
    if key in polys:
        return polys[key]
    # Substring match
    for k, v in polys.items():
        if key in k or k in key:
            return v
    return None


# ---------------------------------------------------------------------------
# Step 3: Voronoi generation
# ---------------------------------------------------------------------------


def _voronoi_polygons(
    nodes: list[dict], muni_polygon: Polygon | MultiPolygon
) -> list[tuple[dict, Polygon | MultiPolygon]]:
    """Generate Voronoi cells clipped to the municipal polygon.

    Returns a list of (node_dict, clipped_polygon) pairs.
    """
    n = len(nodes)
    if n == 0:
        return []

    if n == 1:
        return [(nodes[0], muni_polygon)]

    if n == 2:
        return _bisect_polygon(nodes, muni_polygon)

    points = np.array([[nd["lon"], nd["lat"]] for nd in nodes])
    vor = Voronoi(points)

    # Build a large bounding box (10x municipality)
    bx = muni_polygon.bounds
    dx = (bx[2] - bx[0]) * 5
    dy = (bx[3] - bx[1]) * 5
    big_box = box(bx[0] - dx, bx[1] - dy, bx[2] + dx, bx[3] + dy)

    results: list[tuple[dict, Polygon | MultiPolygon]] = []
    for idx, region_idx in enumerate(vor.point_region):
        region = vor.regions[region_idx]
        if not region or -1 in region:
            # Region extends to infinity — build from ridge clipping
            cell = _infinite_region(vor, idx, big_box)
        else:
            verts = [vor.vertices[i] for i in region]
            cell = Polygon(verts)

        cell = _validate(cell)
        clipped = _ensure_polygon(cell.intersection(muni_polygon))
        if clipped is None:
            continue
        clipped = _validate(clipped)
        results.append((nodes[idx], clipped))

    # Merge tiny cells (< 0.1 km²) into nearest neighbor
    results = _merge_tiny_cells(results, min_area_km2=0.1)

    return results


def _infinite_region(
    vor: Voronoi, point_idx: int, big_box: Polygon
) -> Polygon:
    """Reconstruct a Voronoi region that extends to infinity by clipping ridges."""
    region_idx = vor.point_region[point_idx]
    region = vor.regions[region_idx]

    if not region:
        return Polygon()

    # Collect finite vertices and compute far points for infinite ridges
    finite_verts = [vor.vertices[i] for i in region if i >= 0]
    if not finite_verts:
        return Polygon()

    # Gather all ridges for this point
    center = vor.points[point_idx]
    all_verts = list(finite_verts)

    for ridge_points, ridge_verts in zip(vor.ridge_points, vor.ridge_vertices):
        if point_idx not in ridge_points:
            continue
        if -1 not in ridge_verts:
            continue

        # One vertex is at infinity
        finite_v_idx = [v for v in ridge_verts if v >= 0]
        if not finite_v_idx:
            continue
        finite_v = vor.vertices[finite_v_idx[0]]

        # Direction: perpendicular to the line between the two points
        other_idx = ridge_points[0] if ridge_points[1] == point_idx else ridge_points[1]
        midpoint = 0.5 * (vor.points[point_idx] + vor.points[other_idx])
        tangent = vor.points[other_idx] - vor.points[point_idx]
        normal = np.array([-tangent[1], tangent[0]])
        # Point away from the midpoint toward the region's center
        if np.dot(normal, center - midpoint) > 0:
            direction = normal
        else:
            direction = -normal

        # Normalise and extend far
        direction = direction / np.linalg.norm(direction)
        far_point = finite_v + direction * 10.0  # 10 degrees is very far
        all_verts.append(far_point)

    if len(all_verts) < 3:
        return Polygon()

    # Order vertices by angle around the center
    cx, cy = center
    angles = [np.arctan2(v[1] - cy, v[0] - cx) if isinstance(v, np.ndarray) else np.arctan2(v[1] - cy, v[0] - cx) for v in all_verts]
    ordered = [v for _, v in sorted(zip(angles, all_verts))]
    poly = Polygon(ordered)
    poly = _validate(poly)
    return _validate(poly.intersection(big_box))


def _bisect_polygon(
    nodes: list[dict], muni_polygon: Polygon | MultiPolygon
) -> list[tuple[dict, Polygon | MultiPolygon]]:
    """Split a municipality in half using the perpendicular bisector of 2 nodes."""
    p1 = np.array([nodes[0]["lon"], nodes[0]["lat"]])
    p2 = np.array([nodes[1]["lon"], nodes[1]["lat"]])
    mid = (p1 + p2) / 2.0
    tangent = p2 - p1
    normal = np.array([-tangent[1], tangent[0]])
    normal = normal / np.linalg.norm(normal)

    # Create a very long bisector line
    far = 10.0
    line = LineString([mid - normal * far, mid + normal * far])

    # Split the polygon
    from shapely.ops import split

    parts = split(muni_polygon, line)
    if len(parts.geoms) < 2:
        # Fallback: assign whole polygon to first node
        return [(nodes[0], muni_polygon)]

    # Assign each part to the nearest node
    results = []
    used = set()
    for node in nodes:
        pt = Point(node["lon"], node["lat"])
        best_idx = min(
            (i for i in range(len(parts.geoms)) if i not in used),
            key=lambda i: pt.distance(parts.geoms[i]),
        )
        results.append((node, _validate(parts.geoms[best_idx])))
        used.add(best_idx)

    return results


def _merge_tiny_cells(
    cells: list[tuple[dict, Polygon | MultiPolygon]], min_area_km2: float
) -> list[tuple[dict, Polygon | MultiPolygon]]:
    """Merge cells smaller than min_area_km2 into their nearest neighbor."""
    if len(cells) <= 1:
        return cells

    merged = True
    while merged:
        merged = False
        new_cells = []
        skip = set()
        for i, (node_i, geom_i) in enumerate(cells):
            if i in skip:
                continue
            if _area_km2(geom_i) < min_area_km2 and len(cells) - len(skip) > 1:
                # Find nearest neighbor (by centroid distance)
                ci = geom_i.centroid
                best_j = None
                best_dist = float("inf")
                for j, (node_j, geom_j) in enumerate(cells):
                    if j == i or j in skip:
                        continue
                    d = ci.distance(geom_j.centroid)
                    if d < best_dist:
                        best_dist = d
                        best_j = j
                if best_j is not None:
                    # Merge into neighbor — the neighbor keeps its name
                    node_j, geom_j = cells[best_j]
                    merged_geom = _validate(unary_union([geom_i, geom_j]))
                    cells[best_j] = (node_j, merged_geom)
                    skip.add(i)
                    merged = True
                    continue
            new_cells.append((node_i, geom_i))
        # Rebuild list including updated neighbors
        final = []
        for i, cell in enumerate(cells):
            if i not in skip:
                final.append(cell)
        cells = final

    return cells


# ---------------------------------------------------------------------------
# Step 4: Urban clipping
# ---------------------------------------------------------------------------


def _get_catastro_buildings(
    client: bigquery.Client, muni_wkt: str
) -> list[tuple[float, float]]:
    """Query BigQuery for catastro building points within a municipality."""
    query = f"""
        SELECT longitude AS lon, latitude AS lat
        FROM `{PROJECT}.barrioscout_raw.catastro_buildings`
        WHERE ST_WITHIN(
            ST_GEOGPOINT(longitude, latitude),
            ST_GEOGFROMTEXT('{muni_wkt}')
        )
    """
    rows = client.query(query).result()
    return [(row.lon, row.lat) for row in rows]


def _urban_clip(
    voronoi_cells: list[tuple[dict, Polygon | MultiPolygon]],
    muni_polygon: Polygon | MultiPolygon,
    muni_name: str,
    client: bigquery.Client,
) -> list[tuple[str, Polygon | MultiPolygon]]:
    """Apply urban clipping if municipality area > 40 km² and enough buildings exist.

    Returns list of (zone_name, polygon) pairs.
    """
    muni_area = _area_km2(muni_polygon)
    if muni_area <= 40.0:
        print(f"  Area {muni_area:.1f} km² <= 40, skipping urban clip")
        return [(node["name"], geom) for node, geom in voronoi_cells]

    muni_wkt = muni_polygon.wkt
    print(f"  Querying catastro buildings for urban clip...")
    buildings = _get_catastro_buildings(client, muni_wkt)
    print(f"  Found {len(buildings)} catastro buildings")

    if len(buildings) < 50:
        print(f"  < 50 buildings, skipping urban clip")
        return [(node["name"], geom) for node, geom in voronoi_cells]

    # Build urban blob: 50m buffer around each building, then union
    # 50m ≈ 0.00045 degrees
    buffer_deg = 0.00045
    building_buffers = [Point(lon, lat).buffer(buffer_deg) for lon, lat in buildings]
    urban_blob = unary_union(building_buffers)

    # Morphological closing: buffer(-20m) then buffer(+20m) to fill gaps
    close_deg = 0.00018  # ~20m
    urban_blob = urban_blob.buffer(-close_deg).buffer(close_deg)
    urban_blob = _validate(urban_blob)

    if urban_blob.is_empty:
        print(f"  Urban blob is empty after closing, skipping clip")
        return [(node["name"], geom) for node, geom in voronoi_cells]

    results: list[tuple[str, Polygon | MultiPolygon]] = []
    remaining_periferia_parts = []

    for node, cell in voronoi_cells:
        urban_part = _ensure_polygon(cell.intersection(urban_blob))
        rural_part = _ensure_polygon(cell.difference(urban_blob))

        if urban_part is not None and not urban_part.is_empty:
            results.append((node["name"], _validate(urban_part)))

        if rural_part is not None and not rural_part.is_empty:
            remaining_periferia_parts.append(rural_part)

    # Combine all rural parts into one "Periferia"
    if remaining_periferia_parts:
        periferia = _validate(unary_union(remaining_periferia_parts))
        periferia_area = _area_km2(periferia)
        if periferia_area < 1.0 and results:
            # Merge into nearest Voronoi cell
            pc = periferia.centroid
            best_idx = min(
                range(len(results)),
                key=lambda i: pc.distance(results[i][1].centroid),
            )
            name, geom = results[best_idx]
            results[best_idx] = (name, _validate(unary_union([geom, periferia])))
            print(f"  Periferia {periferia_area:.2f} km² < 1, merged into {name}")
        else:
            results.append((f"Periferia de {muni_name}", periferia))
            print(
                f"  Periferia de {muni_name}: {periferia_area:.1f} km²"
            )

    print(f"  Urban clip applied: {len(results)} zones")
    return results


# ---------------------------------------------------------------------------
# Step 5: Build records
# ---------------------------------------------------------------------------


def _build_records(
    zones: list[tuple[str, Polygon | MultiPolygon]], raw_city: str
) -> list[dict]:
    """Build BigQuery-ready records from named zones."""
    muni_slug = _normalize_slug(raw_city)
    records = []
    for zone_name, geom in zones:
        zone_slug = _normalize_slug(zone_name)
        wkt = geom.wkt
        records.append(
            {
                "city": raw_city,
                "level": "neighborhood",
                "name": zone_name,
                "code": f"metro_{muni_slug}_{zone_slug}",
                "district_name": raw_city,
                "geometry_wkt": wkt,
            }
        )
    return records


# ---------------------------------------------------------------------------
# Step 6: Ingest to BigQuery
# ---------------------------------------------------------------------------


def _ingest_to_bq(
    client: bigquery.Client, records: list[dict], raw_city: str
) -> None:
    """Delete old rows and insert new subdivision records via DML."""
    muni_slug = _normalize_slug(raw_city)

    # Delete existing whole-municipality row and any previous subdivisions
    delete_query = f"""
        DELETE FROM `{TABLE}`
        WHERE code = 'metro_{muni_slug}'
           OR code LIKE 'metro_{muni_slug}_%'
    """
    print(f"  Deleting old rows (code = 'metro_{muni_slug}' or LIKE 'metro_{muni_slug}_%')...")
    client.query(delete_query).result()

    if not records:
        print("  No records to insert")
        return

    # Build parameterised INSERT
    values_clauses = []
    params = []
    for i, rec in enumerate(records):
        values_clauses.append(
            f"(@city_{i}, 'neighborhood', @name_{i}, @code_{i}, @dist_{i}, @wkt_{i})"
        )
        params.extend(
            [
                bigquery.ScalarQueryParameter(f"city_{i}", "STRING", rec["city"]),
                bigquery.ScalarQueryParameter(f"name_{i}", "STRING", rec["name"]),
                bigquery.ScalarQueryParameter(f"code_{i}", "STRING", rec["code"]),
                bigquery.ScalarQueryParameter(
                    f"dist_{i}", "STRING", rec["district_name"]
                ),
                bigquery.ScalarQueryParameter(
                    f"wkt_{i}", "STRING", rec["geometry_wkt"]
                ),
            ]
        )

    insert_query = (
        f"INSERT INTO `{TABLE}` "
        f"(city, level, name, code, district_name, geometry_wkt) "
        f"VALUES {', '.join(values_clauses)}"
    )
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    client.query(insert_query, job_config=job_config).result()
    print(f"  Inserted {len(records)} rows into {TABLE}")


# ---------------------------------------------------------------------------
# Step 7: Summary
# ---------------------------------------------------------------------------


def _print_summary(
    zones: list[tuple[str, Polygon | MultiPolygon]],
    muni_polygon: Polygon | MultiPolygon,
    muni_name: str,
    urban_clipped: bool,
) -> None:
    """Print a summary of the generated zones."""
    muni_area = _area_km2(muni_polygon)
    total_zone_area = sum(_area_km2(g) for _, g in zones)
    print(f"\n  Summary for {muni_name}:")
    print(f"    Zones created: {len(zones)}")
    print(f"    Urban clip applied: {urban_clipped}")
    print(f"    Municipality area: {muni_area:.2f} km²")
    print(f"    Total zone area:   {total_zone_area:.2f} km²")
    for name, geom in zones:
        print(f"      - {name}: {_area_km2(geom):.2f} km²")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    """Process all 9 Tier B municipalities."""
    print("Loading CSV nodes...")
    csv_groups = _load_csv_nodes()

    print("Loading municipal polygons...")
    poly_map = _load_municipal_polygons()

    client = bigquery.Client(project=PROJECT)

    for muni_name, config in TIER_B.items():
        cod_ine = config["cod_ine"]
        urban_clip_flag = config["urban_clip"]
        raw_city = config["raw_city"]

        print(f"\n{'='*60}")
        print(f"Processing: {muni_name} (cod_ine={cod_ine})")
        print(f"{'='*60}")

        # Step 2: Get municipal polygon
        muni_polygon = _find_polygon(muni_name, poly_map)
        if muni_polygon is None:
            print(f"  ERROR: No polygon found for {muni_name}, skipping")
            continue

        # Step 1: Get nodes
        nodes = _get_nodes(cod_ine, muni_name, muni_polygon, csv_groups)
        print(f"  Nodes found: {len(nodes)}")
        for nd in nodes:
            print(f"    - {nd['name']} ({nd['place']}) @ {nd['lat']:.4f}, {nd['lon']:.4f}")

        if not nodes:
            print(f"  WARNING: No nodes, using whole municipality as single zone")
            zones: list[tuple[str, Polygon | MultiPolygon]] = [
                (muni_name, muni_polygon)
            ]
            urban_clipped = False
        else:
            # Step 3: Voronoi
            voronoi_cells = _voronoi_polygons(nodes, muni_polygon)
            print(f"  Voronoi cells: {len(voronoi_cells)}")

            # Step 4: Urban clipping
            if urban_clip_flag:
                zones = _urban_clip(voronoi_cells, muni_polygon, muni_name, client)
                urban_clipped = _area_km2(muni_polygon) > 40.0
            else:
                zones = [(node["name"], geom) for node, geom in voronoi_cells]
                urban_clipped = False

        # Step 7: Summary
        _print_summary(zones, muni_polygon, muni_name, urban_clipped)

        # Step 5: Build records
        records = _build_records(zones, raw_city)

        # Step 6: Ingest
        _ingest_to_bq(client, records, raw_city)

    print(f"\n{'='*60}")
    print("Done! All municipalities processed.")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
