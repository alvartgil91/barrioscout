"""
Validation test for the neighbourhoods and districts pipeline.

Downloads live data from Madrid TopoJSON and Granada WFS, runs extract + transform,
and validates expected counts and geometry validity.

Does NOT load to BigQuery.

Usage:
    python tests/test_neighborhoods_pipeline.py
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shapely import wkt

from src.ingestion.neighborhoods import extract, transform

BOLD = "\033[1m"
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"


def test_madrid_extract() -> None:
    """Validate Madrid returns ~131 barrios + ~21 distritos."""
    print(f"\n{BOLD}Test: Madrid extract{RESET}")
    raw = extract("madrid")

    barrios = [r for r in raw if r["level"] == "neighborhood"]
    distritos = [r for r in raw if r["level"] == "district"]

    print(f"  Barrios: {len(barrios)} (expected ~131)")
    print(f"  Distritos: {len(distritos)} (expected ~21)")

    assert len(barrios) >= 125, f"Too few barrios: {len(barrios)}"
    assert len(distritos) >= 20, f"Too few distritos: {len(distritos)}"

    # All barrios should have a district_name
    missing_district = [r for r in barrios if not r["district_name"]]
    assert not missing_district, f"{len(missing_district)} barrios without district_name"

    # All distritos should have district_name = None
    assert all(r["district_name"] is None for r in distritos)

    print(f"  {GREEN}PASS{RESET}")


def test_granada_extract() -> None:
    """Validate Granada returns ~37 barrios + ~8 distritos (dissolved)."""
    print(f"\n{BOLD}Test: Granada extract{RESET}")
    raw = extract("granada")

    barrios = [r for r in raw if r["level"] == "neighborhood"]
    distritos = [r for r in raw if r["level"] == "district"]

    print(f"  Barrios: {len(barrios)} (expected ~37)")
    print(f"  Distritos: {len(distritos)} (expected ~8)")

    assert len(barrios) >= 30, f"Too few barrios: {len(barrios)}"
    assert len(distritos) >= 7, f"Too few distritos: {len(distritos)}"

    # Check that coords are WGS84 (lon ~ -3.6, lat ~ 37.2), not UTM metres
    sample = barrios[0]["geometry"]
    centroid = sample.centroid
    assert -4.5 < centroid.x < -3.0, f"Longitude looks wrong: {centroid.x}"
    assert 36.5 < centroid.y < 38.0, f"Latitude looks wrong: {centroid.y}"

    print(f"  CRS check: centroid ({centroid.x:.4f}, {centroid.y:.4f}) — WGS84 OK")
    print(f"  {GREEN}PASS{RESET}")


def test_transform_wkt_validity() -> None:
    """Validate that transform produces valid WKT for all features."""
    print(f"\n{BOLD}Test: transform WKT validity{RESET}")
    raw = extract()
    df = transform(raw)

    invalid = 0
    for _, row in df.iterrows():
        try:
            geom = wkt.loads(row["geometry_wkt"])
            if not geom.is_valid:
                invalid += 1
        except Exception as exc:
            print(f"  FAIL: {row['city']} {row['name']} — {exc}")
            invalid += 1

    print(f"  Total rows: {len(df)}")
    print(f"  Invalid WKT: {invalid}")
    assert invalid == 0, f"{invalid} invalid geometries found"

    # Check schema columns
    expected_cols = {"city", "level", "name", "code", "district_name", "geometry_wkt"}
    assert expected_cols.issubset(set(df.columns)), f"Missing columns: {expected_cols - set(df.columns)}"

    print(f"  {GREEN}PASS{RESET}")


if __name__ == "__main__":
    passed = 0
    failed = 0

    for test_fn in [test_madrid_extract, test_granada_extract, test_transform_wkt_validity]:
        try:
            test_fn()
            passed += 1
        except (AssertionError, Exception) as exc:
            print(f"  {RED}FAIL: {exc}{RESET}")
            failed += 1

    print(f"\n{BOLD}Results: {passed} passed, {failed} failed{RESET}")
    sys.exit(1 if failed else 0)
