"""
Validation test for the INE Renta ingestion pipeline.

Tests extract() and transform() without requiring BigQuery credentials.
Prints shape, columns, and 3 sample rows for visual inspection.

NOTE: INE table 30896 covers Catalonia municipalities. The 'city' column will
be None for all rows in this dataset (Granada/Madrid data requires a national
table). Update INE_RENTA_URL in config/settings.py for national coverage.

Usage:
    python tests/test_ine_pipeline.py
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ingestion.ine import extract, transform

BOLD = "\033[1m"
GREEN = "\033[92m"
RESET = "\033[0m"


def test_ine_pipeline() -> None:
    print(f"\n{BOLD}=== INE Renta pipeline — extract + transform ==={RESET}\n")

    # --- extract ---
    print(f"{BOLD}extract(){RESET}")
    raw = extract()
    print(f"  Shape   : {raw.shape}")
    print(f"  Columns : {list(raw.columns)}\n")

    assert not raw.empty, "extract() returned empty DataFrame"
    assert raw.shape[1] == 6, f"Expected 6 columns, got {raw.shape[1]}"

    # --- transform ---
    print(f"{BOLD}transform(){RESET}")
    clean = transform(raw)
    print(f"  Shape   : {clean.shape}")
    print(f"  Columns : {list(clean.columns)}")
    cities = clean["city"].dropna().unique().tolist()
    print(f"  Cities  : {sorted(cities) if cities else 'none in this dataset (see note above)'}\n")

    print(f"{BOLD}Sample (3 rows):{RESET}")
    print(clean.head(3).to_string(index=False))
    print()

    # Structural assertions (data-source agnostic)
    assert not clean.empty, "transform() returned empty DataFrame"
    expected_cols = {"municipio_codigo", "municipio_nombre", "año", "renta_neta_media", "city"}
    assert expected_cols.issubset(set(clean.columns)), (
        f"Missing columns: {expected_cols - set(clean.columns)}"
    )
    assert clean["renta_neta_media"].notna().all(), "Null values found in renta_neta_media"
    assert clean["municipio_codigo"].str.match(r"^\d{5}$").all(), "Unexpected municipio_codigo format"
    assert clean["año"].notna().all(), "Null values found in año"

    # City column must only contain expected labels or None
    valid_cities = {"Granada", "Madrid", None}
    actual_cities = set(clean["city"].unique())
    assert actual_cities.issubset(valid_cities | {float("nan")}), (
        f"Unexpected city values: {actual_cities - valid_cities}"
    )

    print(f"{GREEN}OK — all assertions passed{RESET}\n")


if __name__ == "__main__":
    test_ine_pipeline()
