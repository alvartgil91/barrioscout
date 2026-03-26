"""
Fix Voronoi overlaps for 9 Tier B municipalities.

Replaces scipy.spatial.Voronoi with shapely.ops.voronoi_diagram which
guarantees disjoint cells. Each cell is intersected with the municipal
polygon for exact coverage with zero overlap.
"""

import csv
import json
import re
import time
from pathlib import Path
from typing import Any

import requests
from google.cloud import bigquery
from shapely.geometry import MultiPoint, Point, Polygon, MultiPolygon, GeometryCollection
from shapely.ops import voronoi_diagram, unary_union
from shapely import wkt as shapely_wkt

PROJECT = "portfolio-alvartgil91"
TABLE = f"{PROJECT}.barrioscout_raw.neighborhoods"

SCRIPTS_DIR = Path(__file__).resolve().parent
NODOS_CSV = SCRIPTS_DIR / "osm_21_municipios" / "nodos.csv"
POLYGONS_JSON = SCRIPTS_DIR / "municipal_polygons.json"

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

TIER_B: dict[str, dict[str, Any]] = {
    "Parla":                       {"cod_ine": "28106", "raw_city": "Parla"},
    "San Sebastián de los Reyes":  {"cod_ine": "28134", "raw_city": "San Sebastián de los Reyes"},
    "Boadilla del Monte":          {"cod_ine": "28022", "raw_city": "Boadilla del Monte"},
    "Las Rozas de Madrid":         {"cod_ine": "28127", "raw_city": "Las Rozas de Madrid"},
    "Pinto":                       {"cod_ine": "28113", "raw_city": "Pinto"},
    "Majadahonda":                 {"cod_ine": "28080", "raw_city": "Majadahonda"},
    "Navalcarnero":                {"cod_ine": "28096", "raw_city": "Navalcarnero"},
    "Colmenar Viejo":              {"cod_ine": "28049", "raw_city": "Colmenar Viejo"},
    "La Zubia":                    {"cod_ine": "18193", "raw_city": "La Zubia"},
}

OVERPASS_MUNICIPALITIES = {"28127", "28113", "28134"}


def _slug(name: str) -> str:
    s = name.lower()
    for old, new in [("á","a"),("é","e"),("í","i"),("ó","o"),("ú","u"),("ñ","n"),("ü","u")]:
        s = s.replace(old, new)
    return re.sub(r"[^a-z0-9]+", "_", s).strip("_")


def _valid(geom):
    if not geom.is_valid:
        geom = geom.buffer(0)
    return geom


def _extract_polys(geom) -> list[Polygon | MultiPolygon]:
    """Extract polygon parts from any geometry."""
    if geom.is_empty:
        return []
    if isinstance(geom, (Polygon, MultiPolygon)):
        return [geom]
    if isinstance(geom, GeometryCollection):
        return [g for g in geom.geoms if isinstance(g, (Polygon, MultiPolygon)) and not g.is_empty]
    return []


def _load_csv_nodes() -> dict[str, list[dict]]:
    groups: dict[str, list[dict]] = {}
    with open(NODOS_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cod = row["cod_ine"].strip()
            groups.setdefault(cod, []).append({
                "name": row["name"].strip(),
                "place": row["place"].strip(),
                "lat": float(row["lat"]),
                "lon": float(row["lon"]),
            })
    return groups


def _overpass_nodes(name: str) -> list[dict]:
    query = (
        f'[out:json][timeout:30];\n'
        f'area["name"="{name}"]["admin_level"="8"]["boundary"="administrative"]->.s;\n'
        f'(node["place"="suburb"](area.s);node["place"="neighbourhood"](area.s););\n'
        f'out body;'
    )
    for attempt in range(3):
        try:
            resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=60)
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = [10, 30, 60][min(attempt, 2)]
                print(f"  Overpass {resp.status_code}, retrying in {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return [
                {"name": el.get("tags", {}).get("name", ""),
                 "place": el.get("tags", {}).get("place", ""),
                 "lat": el["lat"], "lon": el["lon"]}
                for el in resp.json().get("elements", [])
            ]
        except Exception as exc:
            print(f"  Overpass error: {exc}")
            time.sleep(10)
    return []


def _get_nodes(cod_ine: str, muni_name: str, muni_poly, csv_groups: dict) -> list[dict]:
    if cod_ine in OVERPASS_MUNICIPALITIES:
        print(f"  Querying Overpass for {muni_name}...")
        raw = _overpass_nodes(muni_name)
        time.sleep(2)
    else:
        raw = csv_groups.get(cod_ine, [])

    # Filter inside polygon
    nodes = [n for n in raw if n["name"] and muni_poly.contains(Point(n["lon"], n["lat"]))]
    # Filter Pinto false positives
    if cod_ine == "28113":
        nodes = [n for n in nodes if 39.0 <= n["lat"] <= 41.0]

    # Deduplicate by name: prefer suburb, closest to centroid
    centroid = muni_poly.centroid
    by_name: dict[str, list[dict]] = {}
    for n in nodes:
        by_name.setdefault(n["name"], []).append(n)

    deduped = []
    for name, cands in by_name.items():
        suburbs = [c for c in cands if c["place"] == "suburb"]
        pool = suburbs if suburbs else cands
        best = min(pool, key=lambda c: centroid.distance(Point(c["lon"], c["lat"])))
        deduped.append(best)
    return deduped


def _load_municipal_polygons() -> dict[str, Polygon | MultiPolygon]:
    with open(POLYGONS_JSON, encoding="utf-8") as f:
        data = json.load(f)
    return {rec["city"].lower(): _valid(shapely_wkt.loads(rec["geometry_wkt"])) for rec in data}


def _find_polygon(name: str, polys: dict):
    key = name.lower()
    if key in polys:
        return polys[key]
    for k, v in polys.items():
        if key in k or k in key:
            return v
    return None


def _voronoi_zones(
    nodes: list[dict], muni_poly: Polygon | MultiPolygon
) -> list[tuple[dict, Polygon | MultiPolygon]]:
    """Generate non-overlapping Voronoi zones clipped to municipal polygon."""
    n = len(nodes)
    if n == 0:
        return []
    if n == 1:
        return [(nodes[0], muni_poly)]

    points = MultiPoint([(nd["lon"], nd["lat"]) for nd in nodes])

    # voronoi_diagram: returns GeometryCollection of polygons
    # envelope clips the infinite regions
    regions = voronoi_diagram(points, envelope=muni_poly)

    # Match each Voronoi cell to its seed node
    results: list[tuple[dict, Polygon | MultiPolygon]] = []
    used_nodes: set[int] = set()

    for region_geom in regions.geoms:
        # Clip to municipal polygon
        clipped = region_geom.intersection(muni_poly)
        polys = _extract_polys(clipped)
        if not polys:
            continue
        clipped_poly = _valid(unary_union(polys))
        if clipped_poly.is_empty:
            continue

        # Find which node this cell contains
        best_node_idx = None
        best_dist = float("inf")
        for i, nd in enumerate(nodes):
            if i in used_nodes:
                continue
            pt = Point(nd["lon"], nd["lat"])
            if clipped_poly.contains(pt):
                best_node_idx = i
                break
            d = clipped_poly.distance(pt)
            if d < best_dist:
                best_dist = d
                best_node_idx = i

        if best_node_idx is not None:
            used_nodes.add(best_node_idx)
            results.append((nodes[best_node_idx], clipped_poly))

    # If some nodes weren't matched (extra cells), merge orphan cells into nearest
    unmatched_cells = []
    for region_geom in regions.geoms:
        clipped = region_geom.intersection(muni_poly)
        polys = _extract_polys(clipped)
        if not polys:
            continue
        clipped_poly = _valid(unary_union(polys))
        if clipped_poly.is_empty:
            continue
        # Check if this cell is already in results
        already = False
        for _, rp in results:
            if rp.equals(clipped_poly) or clipped_poly.intersection(rp).area / clipped_poly.area > 0.9:
                already = True
                break
        if not already:
            unmatched_cells.append(clipped_poly)

    # Merge unmatched cells into nearest result
    for orphan in unmatched_cells:
        if not results:
            break
        oc = orphan.centroid
        best_idx = min(range(len(results)), key=lambda i: oc.distance(results[i][1].centroid))
        node, geom = results[best_idx]
        results[best_idx] = (node, _valid(unary_union([geom, orphan])))

    # Merge tiny cells (<0.1 km² ≈ 0.000008 deg²)
    min_area_deg2 = 0.1 / (111.0 * 85.0)  # rough deg² for 0.1 km² at 40°N
    changed = True
    while changed and len(results) > 1:
        changed = False
        for i, (node_i, geom_i) in enumerate(results):
            if geom_i.area < min_area_deg2:
                ci = geom_i.centroid
                best_j = min(
                    (j for j in range(len(results)) if j != i),
                    key=lambda j: ci.distance(results[j][1].centroid)
                )
                node_j, geom_j = results[best_j]
                results[best_j] = (node_j, _valid(unary_union([geom_i, geom_j])))
                results.pop(i)
                changed = True
                break

    return results


def _area_km2(geom) -> float:
    import math
    bounds = geom.bounds
    mid_lat = (bounds[1] + bounds[3]) / 2.0
    return geom.area * 111.0 * 111.0 * math.cos(math.radians(mid_lat))


def main() -> None:
    print("=== Fix Voronoi Overlaps (Tier B) ===\n")

    csv_groups = _load_csv_nodes()
    poly_map = _load_municipal_polygons()
    client = bigquery.Client(project=PROJECT)

    for muni_name, config in TIER_B.items():
        cod_ine = config["cod_ine"]
        raw_city = config["raw_city"]
        muni_slug = _slug(raw_city)

        print(f"\n{'='*60}")
        print(f"{muni_name} (cod_ine={cod_ine})")
        print(f"{'='*60}")

        muni_poly = _find_polygon(muni_name, poly_map)
        if muni_poly is None:
            print("  ERROR: No polygon found, skipping")
            continue

        nodes = _get_nodes(cod_ine, muni_name, muni_poly, csv_groups)
        print(f"  {len(nodes)} nodes")

        if not nodes:
            print("  WARNING: No nodes, skipping")
            continue

        zones = _voronoi_zones(nodes, muni_poly)
        print(f"  {len(zones)} zones generated")

        # Verify no overlap
        for i in range(len(zones)):
            for j in range(i + 1, len(zones)):
                overlap = zones[i][1].intersection(zones[j][1]).area
                if overlap > 1e-10:
                    print(f"  WARNING: overlap {zones[i][0]['name']} <-> {zones[j][0]['name']}: "
                          f"{overlap * 111**2:.6f} km²")

        # Print summary
        muni_area = _area_km2(muni_poly)
        total_zone_area = sum(_area_km2(g) for _, g in zones)
        print(f"  Municipality: {muni_area:.1f} km², zones total: {total_zone_area:.1f} km²")
        for nd, geom in zones:
            print(f"    {nd['name']:<40} {_area_km2(geom):>6.1f} km²")

        # Build records
        records = []
        for nd, geom in zones:
            zone_slug = _slug(nd["name"])
            records.append({
                "city": raw_city,
                "level": "neighborhood",
                "name": nd["name"],
                "code": f"metro_{muni_slug}_{zone_slug}",
                "district_name": raw_city,
                "geometry_wkt": geom.wkt,
            })

        # Delete old + insert new
        delete_query = f"""
            DELETE FROM `{TABLE}`
            WHERE code LIKE 'metro_{muni_slug}_%'
               OR code = 'metro_{muni_slug}'
        """
        client.query(delete_query).result()

        values_clauses = []
        params = []
        for i, rec in enumerate(records):
            values_clauses.append(
                f"(@c{i}, 'neighborhood', @n{i}, @k{i}, @d{i}, @w{i})"
            )
            params.extend([
                bigquery.ScalarQueryParameter(f"c{i}", "STRING", rec["city"]),
                bigquery.ScalarQueryParameter(f"n{i}", "STRING", rec["name"]),
                bigquery.ScalarQueryParameter(f"k{i}", "STRING", rec["code"]),
                bigquery.ScalarQueryParameter(f"d{i}", "STRING", rec["district_name"]),
                bigquery.ScalarQueryParameter(f"w{i}", "STRING", rec["geometry_wkt"]),
            ])

        insert_query = (
            f"INSERT INTO `{TABLE}` (city, level, name, code, district_name, geometry_wkt) "
            f"VALUES {', '.join(values_clauses)}"
        )
        client.query(insert_query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
        print(f"  Ingested {len(records)} records")

    print(f"\n{'='*60}")
    print("Done!")


if __name__ == "__main__":
    main()
