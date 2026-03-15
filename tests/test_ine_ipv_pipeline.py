"""
Validation test for the INE IPV ingestion pipeline.

Tests extract() and transform() without requiring BigQuery credentials.
Prints shape, columns, and 3 sample rows for visual inspection.

Usage:
    python tests/test_ine_ipv_pipeline.py
"""

from __future__ import annotations

import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.ingestion.ine_ipv import extract, transform

BOLD = "\033[1m"
GREEN = "\033[92m"
RESET = "\033[0m"


def test_ine_ipv_pipeline() -> None:
    print(f"\n{BOLD}=== INE IPV pipeline — extract + transform ==={RESET}\n")

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
    ccaa = sorted(clean["autonomous_community"].unique())
    index_types = sorted(clean["index_type"].unique())
    print(f"  CCAA    : {ccaa}")
    print(f"  Types   : {index_types}\n")

    print(f"{BOLD}Sample (3 rows):{RESET}")
    print(clean.head(3).to_string(index=False))
    print()

    # Structural assertions
    assert not clean.empty, "transform() returned empty DataFrame"
    expected_cols = {"autonomous_community", "index_type", "period", "value"}
    assert expected_cols.issubset(set(clean.columns)), (
        f"Missing columns: {expected_cols - set(clean.columns)}"
    )
    assert clean["value"].notna().all(), "Null values found in value"

    # Both target CCAA must be present (INE prefixes names with a 2-digit code)
    assert "01 Andalucía" in ccaa, "Andalucía not found in output"
    assert "13 Madrid, Comunidad de" in ccaa, "Madrid not found in output"

    # Period format must match YYYYTn
    bad_periods = clean[~clean["period"].str.match(r"^\d{4}T\d$")]
    assert bad_periods.empty, f"Unexpected period format: {bad_periods['period'].unique()[:5]}"

    print(f"{GREEN}OK — all assertions passed{RESET}\n")


if __name__ == "__main__":
    test_ine_ipv_pipeline()
