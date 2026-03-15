"""
Ingestion module for INE (Instituto Nacional de Estadística) data.

Source: Atlas de distribución de renta de los hogares
URL: https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/30896.csv
Schema target: barrioscout_raw.ine_renta

NOTE: Table 30896 covers Catalonia municipalities (province "08"). If national
coverage is needed, update INE_RENTA_URL in config/settings.py to a national
table ID. The city column will be None for municipalities outside Granada (18)
and Madrid (28) — filtering to target cities happens in the clean layer.
"""

from __future__ import annotations

import io
import re

import pandas as pd
import requests

from config.settings import INE_RENTA_URL

# Only keep this income indicator from the multiple available
_INDICATOR = "Renta neta media por persona"


def extract(url: str = INE_RENTA_URL) -> pd.DataFrame:
    """Download the INE renta CSV and return it as a raw DataFrame.

    Args:
        url: URL to the INE CSV file (defaults to renta neta media por persona).

    Returns:
        Raw DataFrame with original column names and string dtypes.
    """
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    df = pd.read_csv(
        io.BytesIO(response.content),
        sep=";",
        encoding="utf-8-sig",
        dtype=str,
    )
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalise the raw INE renta DataFrame.

    Steps:
    - Keep only municipality-level rows (drops district and section rows)
    - Keep only 'Renta neta media por persona' indicator
    - Split municipality field into code and name
    - Cast types: municipio_codigo str, año int, renta_neta_media float
    - Drop rows with nulls in key fields
    - Add 'city' column: 'Granada' for codes starting with '18',
      'Madrid' for '28', None otherwise

    Args:
        df: Raw DataFrame from extract().

    Returns:
        Cleaned DataFrame ready for loading.
    """
    df = df.copy()

    # Identify columns by position (INE CSV always has this order)
    municipio_col = df.columns[0]   # "Municipios"
    distritos_col = df.columns[1]   # "Distritos"
    secciones_col = df.columns[2]   # "Secciones"
    indicador_col = df.columns[3]   # "Indicadores de renta media y mediana"
    periodo_col = df.columns[4]     # "Periodo"
    total_col = df.columns[5]       # "Total"

    # Keep municipality-level rows only (district and section columns are NaN)
    df = df[df[distritos_col].isna() & df[secciones_col].isna()]

    # Keep the single income indicator we care about
    df = df[df[indicador_col] == _INDICATOR]

    # Split "18087 Granada, ciudad" → code="18087", name="Granada, ciudad"
    split = df[municipio_col].str.extract(r"^(\d{5})\s+(.+)$", expand=True)
    df = df.copy()
    df["municipio_codigo"] = split[0].str.strip()
    df["municipio_nombre"] = split[1].str.strip()

    # año: period is already a 4-digit year string
    df["año"] = pd.to_numeric(df[periodo_col], errors="coerce").astype("Int64")

    # renta_neta_media: INE uses "." as thousands separator (no decimal comma)
    # "16.682" → remove dots → 16682.0
    df["renta_neta_media"] = (
        df[total_col]
        .str.replace(".", "", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
    )

    # Drop rows missing key fields
    df = df.dropna(subset=["municipio_codigo", "año", "renta_neta_media"])

    # Derive city label from province prefix (first 2 digits of the 5-digit code)
    df["city"] = df["municipio_codigo"].apply(_city_from_code)

    return df[["municipio_codigo", "municipio_nombre", "año", "renta_neta_media", "city"]].reset_index(drop=True)


def _city_from_code(code: str) -> str | None:
    if code.startswith("18"):
        return "Granada"
    if code.startswith("28"):
        return "Madrid"
    return None


def load(df: pd.DataFrame) -> int:
    """Load the transformed DataFrame into BigQuery raw layer.

    Args:
        df: Transformed DataFrame from transform().

    Returns:
        Number of rows loaded.
    """
    from src.processing.bq_loader import load_to_bigquery

    return load_to_bigquery(df, "barrioscout_raw.ine_renta")


def main() -> None:
    print("=== INE Renta pipeline ===")

    raw = extract()
    print(f"Extracted  : {len(raw):,} rows")

    clean = transform(raw)
    cities = clean["city"].dropna().unique().tolist()
    print(f"Transformed: {len(clean):,} rows | cities in data: {sorted(cities) or 'none (check INE_RENTA_URL)'}")

    loaded = load(clean)
    print(f"Loaded     : {loaded:,} rows → barrioscout_raw.ine_renta")


if __name__ == "__main__":
    main()
