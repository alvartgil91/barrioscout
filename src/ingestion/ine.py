"""
Ingestion module for INE (Instituto Nacional de Estadística) data.

Source: Atlas de distribución de renta de los hogares (operación 353)
Schema target: barrioscout_raw.ine_renta

INE publishes one table per province. Tables for target cities:
  Granada (province 18) → table 31025
  Madrid  (province 28) → table 31097
"""

from __future__ import annotations

import io

import pandas as pd
import requests

from config.settings import INE_RENTA_BASE_URL, INE_RENTA_TABLE_IDS

# Only keep this income indicator from the multiple available
_INDICATOR = "Renta neta media por persona"


def extract(
    table_ids: dict[str, int] = INE_RENTA_TABLE_IDS,
    base_url: str = INE_RENTA_BASE_URL,
) -> pd.DataFrame:
    """Download INE renta CSVs for all configured provinces and concatenate.

    Args:
        table_ids: Mapping of city name → INE table ID.
        base_url: URL template with {table_id} placeholder.

    Returns:
        Concatenated raw DataFrame with original column names and string dtypes.
    """
    frames = []
    for city, table_id in table_ids.items():
        url = base_url.format(table_id=table_id)
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        df = pd.read_csv(
            io.BytesIO(response.content),
            sep=";",
            encoding="utf-8-sig",
            dtype=str,
        )
        print(f"  Downloading {city} (table {table_id})... {len(df):,} rows")
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalise the raw INE renta DataFrame.

    Steps:
    - Keep only municipality-level rows (drops district and section rows)
    - Keep only 'Renta neta media por persona' indicator
    - Split municipality field into code and name
    - Cast types: municipality_code str, year int, net_avg_income float
    - Drop rows with nulls in key fields
    - Add 'city' column: 'Granada' for codes starting with '18',
      'Madrid' for '28', None otherwise (all municipalities kept in raw layer)

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
    df["municipality_code"] = split[0].str.strip()
    df["municipality_name"] = split[1].str.strip()

    # year: period is already a 4-digit year string
    df["year"] = pd.to_numeric(df[periodo_col], errors="coerce").astype("Int64")

    # net_avg_income: INE uses "." as thousands separator (no decimal comma)
    # "16.682" → remove dots → 16682.0
    df["net_avg_income"] = (
        df[total_col]
        .str.replace(".", "", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
    )

    # Drop rows missing key fields
    df = df.dropna(subset=["municipality_code", "year", "net_avg_income"])

    # Derive city label from province prefix (first 2 digits of the 5-digit code)
    # All municipalities kept — filtering to Granada/Madrid happens in clean layer
    df["city"] = df["municipality_code"].apply(_city_from_code)

    return df[["municipality_code", "municipality_name", "year", "net_avg_income", "city"]].reset_index(drop=True)


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
    print(f"Transformed: {len(clean):,} rows | cities: {sorted(cities)}")

    loaded = load(clean)
    print(f"Loaded     : {loaded:,} rows → barrioscout_raw.ine_renta")


if __name__ == "__main__":
    main()
