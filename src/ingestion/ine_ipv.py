"""
Ingestion module for INE IPV (Índice de Precios de Vivienda).

Source: INE table 25171 — quarterly house price index by autonomous community
URL: configured as INE_IPV_URL in config/settings.py
Schema target: barrioscout_raw.ine_ipv
"""

from __future__ import annotations

import io

import pandas as pd
import requests

from config.settings import INE_IPV_URL

# Target autonomous communities (CCAA) to keep.
# INE prefixes names with a 2-digit code (e.g. "01 Andalucía", "13 Madrid, Comunidad de").
_CCAA_FILTER: set[str] = {"01 Andalucía", "13 Madrid, Comunidad de"}

# Housing type to keep (excludes "Vivienda nueva" and "Vivienda segunda mano")
_HOUSING_TYPE: str = "General"


def extract(url: str = INE_IPV_URL) -> pd.DataFrame:
    """Download the INE IPV CSV and return it as a raw DataFrame.

    Args:
        url: URL to the INE IPV CSV file.

    Returns:
        Raw DataFrame with original column names and string dtypes.
    """
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return pd.read_csv(
        io.BytesIO(response.content),
        sep=";",
        encoding="utf-8-sig",
        dtype=str,
    )


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and filter the raw INE IPV DataFrame.

    Steps:
    - Filter to target CCAA: Andalucía and Comunidad de Madrid
    - Filter to general housing type (excludes new/second-hand breakdown)
    - Rename columns to English
    - Cast value to float (Spanish format: comma as decimal separator)
    - Drop rows with null value
    - Period kept as string ("YYYYTn" format, e.g. "2024T1")

    Args:
        df: Raw DataFrame from extract().

    Returns:
        Cleaned DataFrame ready for loading.
    """
    df = df.copy()

    # Identify columns by position (INE CSV always has this order)
    ccaa_col    = df.columns[1]  # "Comunidades y Ciudades Autónomas"
    housing_col = df.columns[2]  # "General, vivienda nueva y de segunda mano"
    index_col   = df.columns[3]  # "Índices y tasas"
    period_col  = df.columns[4]  # "Periodo"
    total_col   = df.columns[5]  # "Total"

    # Filter to target autonomous communities
    df = df[df[ccaa_col].isin(_CCAA_FILTER)]

    # Filter to general indicator only (not broken down by housing type)
    df = df[df[housing_col] == _HOUSING_TYPE]

    # Rename to English output columns
    df = df.rename(columns={
        ccaa_col:   "autonomous_community",
        index_col:  "index_type",
        period_col: "period",
        total_col:  "value",
    })

    # Cast value: INE uses comma as decimal separator ("186,750" → 186.75)
    # Remove thousands dots first (if present), then swap decimal comma
    df["value"] = (
        df["value"]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
    )

    # Drop rows with null value
    df = df.dropna(subset=["value"])

    return df[["autonomous_community", "index_type", "period", "value"]].reset_index(drop=True)


def load(df: pd.DataFrame) -> int:
    """Load the transformed DataFrame into BigQuery raw layer.

    Args:
        df: Transformed DataFrame from transform().

    Returns:
        Number of rows loaded.
    """
    from src.processing.bq_loader import load_to_bigquery

    return load_to_bigquery(df, "barrioscout_raw.ine_ipv")


def main() -> None:
    print("=== INE IPV pipeline ===")

    raw = extract()
    print(f"Extracted  : {len(raw):,} rows")

    clean = transform(raw)
    ccaa = sorted(clean["autonomous_community"].unique())
    print(f"Transformed: {len(clean):,} rows | CCAA: {ccaa}")

    loaded = load(clean)
    print(f"Loaded     : {loaded:,} rows → barrioscout_raw.ine_ipv")


if __name__ == "__main__":
    main()
