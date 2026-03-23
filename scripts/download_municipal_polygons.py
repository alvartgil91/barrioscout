"""
Download municipal boundary polygons from OpenStreetMap Overpass API.
Generates WKT geometries ready for insertion into barrioscout_raw.neighborhoods.
"""

import json
import time
import csv
import re
import sys
from pathlib import Path

import requests
from shapely.geometry import Polygon, MultiPolygon
from shapely.ops import unary_union, linemerge

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
RATE_LIMIT_SECONDS = 5
MAX_RETRIES = 4
RETRY_BACKOFF = [10, 30, 60, 120]  # seconds to wait between retries

MUNICIPALITIES: dict[str, list[str]] = {
    "Granada": [
        "Las Gabias", "Churriana de la Vega", "Atarfe", "Armilla",
        "Ogíjares", "Otura", "La Zubia", "Peligros", "Monachil",
        "Alhendín", "Vegas del Genil",
    ],
    "Madrid": [
        "Alcalá de Henares", "Torrejón de Ardoz", "Getafe", "Las Rozas de Madrid",
        "Pozuelo de Alarcón", "Alcorcón", "San Sebastián de los Reyes", "Móstoles",
        "Majadahonda", "Alcobendas", "Leganés", "Rivas-Vaciamadrid",
        "Coslada", "Fuenlabrada", "Boadilla del Monte", "Tres Cantos",
        "Villaviciosa de Odón", "Valdemoro", "Colmenar Viejo", "Parla",
        "Arganda del Rey", "Torrelodones", "Villanueva de la Cañada",
        "Villanueva del Pardillo", "Galapagar", "Mejorada del Campo",
        "San Fernando de Henares", "Algete", "Cerceda",
        "Navalcarnero", "Pinto", "Ciempozuelos",
        "El Escorial", "Collado Villalba",
    ],
}


def _overpass_query(name: str) -> dict | None:
    """Query Overpass for admin_level=8 relation by exact name within Spain, with retry."""
    query = (
        '[out:json];'
        'area["name"="España"]["admin_level"="2"]->.spain;'
        f'relation["name"="{name}"]["admin_level"="8"](area.spain);'
        'out geom;'
    )
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=90)
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                print(f"  [HTTP {resp.status_code}] waiting {wait}s before retry {attempt + 1}/{MAX_RETRIES}...", end=" ", flush=True)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            wait = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
            print(f"  [timeout] waiting {wait}s before retry {attempt + 1}/{MAX_RETRIES}...", end=" ", flush=True)
            time.sleep(wait)
        except Exception as exc:
            print(f"  [HTTP error] {exc}")
            return None
    print(f"  [FAILED after {MAX_RETRIES} retries]")
    return None


def _overpass_query_partial(name: str) -> dict | None:
    """Fallback: search by name~regex within Spain."""
    # Strip common suffixes to broaden the search
    short = re.sub(r"\s+(de|del|de la|de los|de las)\s+.*", "", name, flags=re.I)
    if short == name:
        return None
    print(f"  [fallback] trying partial name '{short}'")
    query = (
        '[out:json];'
        'area["name"="España"]["admin_level"="2"]->.spain;'
        f'relation["name"~"{short}",i]["admin_level"="8"](area.spain);'
        'out geom;'
    )
    try:
        resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        # Pick the element whose name matches most closely
        elements = data.get("elements", [])
        for el in elements:
            if el.get("tags", {}).get("name", "").lower() == name.lower():
                data["elements"] = [el]
                return data
        # Return first result if any
        return data if elements else None
    except Exception as exc:
        print(f"  [HTTP error fallback] {exc}")
        return None


def _ways_to_coords(members: list[dict]) -> tuple[list[list], list[list]]:
    """
    Extract outer and inner rings from OSM relation members.
    Each way has a 'geometry' list of {lat, lon} dicts.
    Returns (outer_ways, inner_ways) as lists of coordinate lists [(lon, lat), ...].
    """
    outer_ways: list[list[tuple[float, float]]] = []
    inner_ways: list[list[tuple[float, float]]] = []

    for member in members:
        if member.get("type") != "way":
            continue
        geom = member.get("geometry", [])
        if not geom:
            continue
        coords = [(pt["lon"], pt["lat"]) for pt in geom]
        role = member.get("role", "outer")
        if role == "inner":
            inner_ways.append(coords)
        else:
            outer_ways.append(coords)

    return outer_ways, inner_ways


def _merge_ways(ways: list[list[tuple[float, float]]]) -> list[list[tuple[float, float]]]:
    """
    Merge fragmented ways into continuous rings using shapely linemerge.
    Returns a list of closed coordinate rings.
    """
    from shapely.geometry import LineString, MultiLineString

    if not ways:
        return []

    lines = [LineString(w) for w in ways if len(w) >= 2]
    if not lines:
        return []

    merged = linemerge(lines)

    # Normalise to list of LineStrings
    if merged.geom_type == "LineString":
        candidates = [merged]
    elif merged.geom_type == "MultiLineString":
        candidates = list(merged.geoms)
    else:
        candidates = []

    rings = []
    for line in candidates:
        coords = list(line.coords)
        if len(coords) < 3:
            continue
        # Close the ring if not already closed
        if coords[0] != coords[-1]:
            coords.append(coords[0])
        rings.append(coords)

    return rings


def _build_geometry(members: list[dict]) -> MultiPolygon | Polygon | None:
    """Build a Shapely geometry from OSM relation members."""
    outer_ways, inner_ways = _ways_to_coords(members)

    outer_rings = _merge_ways(outer_ways)
    inner_rings = _merge_ways(inner_ways)

    if not outer_rings:
        return None

    # Build individual polygons from outer rings (one per closed ring)
    polygons = []
    for ring in outer_rings:
        # Find holes whose centroid is inside this outer ring
        outer_poly = Polygon(ring)
        holes = [h for h in inner_rings if outer_poly.contains(Polygon(h).centroid)]
        poly = Polygon(ring, holes)
        if not poly.is_valid:
            poly = poly.buffer(0)
        if poly.is_valid and not poly.is_empty:
            polygons.append(poly)

    if not polygons:
        return None

    if len(polygons) == 1:
        return polygons[0]

    combined = unary_union(polygons)
    if not combined.is_valid:
        combined = combined.buffer(0)
    return combined


def _normalize_code(name: str) -> str:
    """Generate a slug-style code from municipality name."""
    s = name.lower()
    s = s.replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")
    s = s.replace("ñ", "n").replace("ü", "u")
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return f"metro_{s}"


def download_municipality(name: str, metro_area: str) -> dict | None:
    """Download and process a single municipality. Returns a record dict or None."""
    print(f"  Querying '{name}'...", end=" ", flush=True)
    data = _overpass_query(name)

    if data is None:
        print("FAILED (HTTP error)")
        return None

    elements = data.get("elements", [])

    if not elements:
        print("not found — trying fallback...", end=" ", flush=True)
        data = _overpass_query_partial(name)
        if data:
            elements = data.get("elements", [])

    if not elements:
        print("NOT FOUND")
        return None

    # Use first matching element
    element = elements[0]
    osm_name = element.get("tags", {}).get("name", name)
    members = element.get("members", [])

    geom = _build_geometry(members)
    if geom is None:
        print("GEOMETRY FAILED")
        return None

    wkt = geom.wkt
    n_vertices = sum(len(p.exterior.coords) for p in (
        geom.geoms if geom.geom_type == "MultiPolygon" else [geom]
    ))
    area_km2 = geom.area * (111.32 ** 2)  # rough degrees² → km² at mid-latitudes

    print(f"OK ({geom.geom_type}, {n_vertices} vertices, {area_km2:.1f} km²)")

    return {
        "city": osm_name,
        "level": "neighborhood",
        "name": osm_name,
        "code": _normalize_code(osm_name),
        "district_name": osm_name,
        "geometry_wkt": wkt,
        "metro_area": metro_area,
        "geom_type": geom.geom_type,
        "n_vertices": n_vertices,
        "area_km2": round(area_km2, 2),
    }


def main() -> None:
    out_json = Path(__file__).parent / "municipal_polygons.json"

    # Resume: load already-downloaded results to avoid re-fetching
    if out_json.exists():
        with open(out_json, encoding="utf-8") as f:
            results: list[dict] = json.load(f)
        already_downloaded = {r["city"] for r in results}
        print(f"Resuming: {len(already_downloaded)} already downloaded: {', '.join(sorted(already_downloaded))}")
    else:
        results = []
        already_downloaded: set[str] = set()

    failed: list[tuple[str, str]] = []

    for metro_area, municipalities in MUNICIPALITIES.items():
        print(f"\n=== {metro_area} metro ===")
        for name in municipalities:
            # Skip if already downloaded (match by original name or OSM name)
            if name in already_downloaded or any(
                r.get("city", "").lower() == name.lower() for r in results
            ):
                print(f"  Skipping '{name}' (already downloaded)")
                continue
            record = download_municipality(name, metro_area)
            if record:
                results.append(record)
                already_downloaded.add(record["city"])
                # Save incrementally after each success
                with open(out_json, "w", encoding="utf-8") as f:
                    json.dump(results, f, ensure_ascii=False, indent=2)
            else:
                failed.append((metro_area, name))
            time.sleep(RATE_LIMIT_SECONDS)

    # Save full JSON (final write)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\nSaved {len(results)} records → {out_json}")

    # Save CSV with BQ-ready columns only (no geom_type / n_vertices / area_km2)
    bq_fields = ["city", "level", "name", "code", "district_name", "geometry_wkt", "metro_area"]
    out_csv = Path(__file__).parent / "municipal_polygons.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=bq_fields)
        writer.writeheader()
        for r in results:
            writer.writerow({k: r[k] for k in bq_fields})
    print(f"Saved CSV → {out_csv}")

    # Summary table
    print("\n=== SUMMARY ===")
    print(f"{'Municipality':<35} {'Metro':<10} {'Type':<15} {'Vertices':>8} {'Area km²':>10}")
    print("-" * 82)
    for r in results:
        print(f"{r['name']:<35} {r['metro_area']:<10} {r['geom_type']:<15} {r['n_vertices']:>8} {r['area_km2']:>10.1f}")

    if failed:
        print(f"\n=== FAILED ({len(failed)}) ===")
        for metro, name in failed:
            print(f"  [{metro}] {name}")
    else:
        print("\nAll municipalities downloaded successfully.")

    # Sanity check: Getafe WKT prefix
    getafe = next((r for r in results if "Getafe" in r["name"]), None)
    if getafe:
        print(f"\nGetafe WKT (first 200 chars):\n{getafe['geometry_wkt'][:200]}")


if __name__ == "__main__":
    main()
