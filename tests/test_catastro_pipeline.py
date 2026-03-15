"""
Validation test for the Catastro INSPIRE buildings pipeline.

Tests _fetch_tile() + transform() using a single hard-coded tile in central Granada.
Does NOT run the full city extract (that takes ~12 tiles + sleep) and does NOT load to BigQuery.

Tile used: 447000,4110000 → 447900,4110900 in EPSG:25830 (900m × 900m, central Granada, confirmed working).

Usage:
    python tests/test_catastro_pipeline.py
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ingestion.catastro import _fetch_tile, transform

BOLD = "\033[1m"
GREEN = "\033[92m"
RESET = "\033[0m"

# Single tile in central Granada (confirmed returning buildings in probe)
_TEST_TILE = (447000.0, 4110000.0, 447900.0, 4110900.0)  # 900m × 900m


def test_catastro_pipeline() -> None:
    print(f"\n{BOLD}=== Catastro pipeline — single tile test (central Granada) ==={RESET}\n")

    # --- fetch single tile ---
    print(f"{BOLD}_fetch_tile(){RESET}")
    xml = _fetch_tile(_TEST_TILE)
    raw_count = xml.count("<bu-ext2d:Building ") if xml else 0
    print(f"  Response  : {len(xml):,} chars")
    print(f"  Buildings : {raw_count} found in raw XML\n")

    assert xml, "_fetch_tile() returned empty string — API may be down"
    assert raw_count > 0, "No buildings found in tile — check tile coordinates or API"

    # --- transform ---
    print(f"{BOLD}transform(){RESET}")
    df = transform([xml])
    print(f"  Shape     : {df.shape}")
    print(f"  Columns   : {list(df.columns)}\n")

    print(f"{BOLD}Sample (3 rows):{RESET}")
    print(df.head(3).to_string(index=False))
    print()

    # Statistics
    year_valid = df["year_built"].dropna()
    uses = sorted(df["current_use"].dropna().unique())
    print(f"{BOLD}Statistics:{RESET}")
    print(f"  year_built range : {int(year_valid.min())} – {int(year_valid.max())}" if not year_valid.empty else "  year_built: all null")
    print(f"  current_use      : {uses}\n")

    # Assertions
    assert not df.empty, "transform() returned empty DataFrame"
    expected_cols = {"cadastral_ref", "year_built", "current_use", "latitude", "longitude"}
    assert expected_cols.issubset(set(df.columns)), (
        f"Missing columns: {expected_cols - set(df.columns)}"
    )
    assert df["cadastral_ref"].notna().all(), "Null values in cadastral_ref"
    assert df["cadastral_ref"].is_unique, "Duplicate cadastral_ref — dedup failed"
    assert df["latitude"].between(36, 39).all(), "Latitude out of Granada range"
    assert df["longitude"].between(-5, -2).all(), "Longitude out of Granada range"

    print(f"{GREEN}OK — all assertions passed{RESET}\n")


if __name__ == "__main__":
    test_catastro_pipeline()
