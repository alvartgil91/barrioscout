"""
Validation test for the INE Renta ingestion pipeline.

Tests extract() and transform() without requiring BigQuery credentials.
Prints shape, columns, and 3 sample rows for visual inspection.

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
    print(f"  Cities  : {sorted(cities)}\n")

    print(f"{BOLD}Sample (3 rows):{RESET}")
    print(clean.head(3).to_string(index=False))
    print()

    # Structural assertions
    assert not clean.empty, "transform() returned empty DataFrame"
    expected_cols = {"municipality_code", "municipality_name", "year", "net_avg_income", "city"}
    assert expected_cols.issubset(set(clean.columns)), (
        f"Missing columns: {expected_cols - set(clean.columns)}"
    )
    assert clean["net_avg_income"].notna().all(), "Null values found in net_avg_income"
    assert clean["municipality_code"].str.match(r"^\d{5}$").all(), "Unexpected municipality_code format"
    assert clean["year"].notna().all(), "Null values found in year"

    # Both target cities must be present
    assert "Granada" in cities, "No Granada municipalities found"
    assert "Madrid" in cities, "No Madrid municipalities found"

    print(f"{GREEN}OK — all assertions passed{RESET}\n")


if __name__ == "__main__":
    test_ine_pipeline()
