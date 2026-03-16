"""
Ingestion module for Ministerio de Transportes — property transactions by municipality.

Source: Boletín estadístico online (apps.fomento.gob.es/BoletinOnline2/)
        Local file: data/raw/ministerio_transacciones_municipio.xls
Schema target: barrioscout_raw.ministerio_transacciones

NOTE: transportes.gob.es blocks programmatic downloads via CloudFront WAF.
      The XLS is downloaded manually and read from disk — no HTTP calls here.

File structure (confirmed by probing):
  1 sheet ("Total "), ~8,200 rows × 91 columns, wide format.
  Row 10:   year headers ("Año 2004" … "Año 2025"), one per 4-col group, merged.
  Row 12:   quarter labels ("1º", "2º", "3º", "4º"), repeating.
  Rows 13+: region headers (ALL CAPS), province headers (Capitalised, no data),
            municipality rows (Capitalised, with numeric data) interleaved.
  Data cols: 3–90 (88 cols = 22 years × 4 quarters).
  Col 1:    municipality / province / region name.
"""

from __future__ import annotations

import pandas as pd

from config.settings import MINISTERIO_TRANSACCIONES_XLS

_CITIES = {"Granada", "Madrid"}


def extract() -> pd.DataFrame:
    """Read the local XLS file and return the raw sheet as a DataFrame.

    Returns:
        Raw DataFrame with no header (header=None), preserving all rows.
    """
    print(f"  Reading {MINISTERIO_TRANSACCIONES_XLS} ...", end=" ", flush=True)
    raw = pd.read_excel(MINISTERIO_TRANSACCIONES_XLS, sheet_name=0, header=None)
    print(f"{raw.shape[0]} rows × {raw.shape[1]} cols")
    return raw


def transform(raw: pd.DataFrame) -> pd.DataFrame:
    """Unpivot the wide format into long rows for Granada and Madrid only.

    Steps:
      1. Build quarter column labels from year (row 10) and quarter (row 12) headers.
      2. Slice municipality data rows (row 15 onward).
      3. Keep only rows with at least one numeric value (drops region/province headers).
      4. Filter to target cities.
      5. Melt wide → long and parse year/quarter from column name.

    Args:
        raw: Raw DataFrame from extract().

    Returns:
        DataFrame with columns: municipality, year, quarter, transactions.
    """
    # --- Build column labels from header rows (10 = years, 12 = quarters) ---
    year_row = raw.iloc[10]
    qtr_row  = raw.iloc[12]

    current_year = ""
    col_labels: dict[int, str] = {}  # col_index → "YYYY_Qn"
    for col in range(3, raw.shape[1]):
        yr_cell = year_row.iloc[col]
        if pd.notna(yr_cell) and "Año" in str(yr_cell):
            current_year = str(yr_cell).strip().replace("Año ", "")
        qtr_cell = qtr_row.iloc[col]
        if pd.notna(qtr_cell) and current_year:
            qtr_num = str(qtr_cell).strip().replace("º", "").replace(" (*)", "").strip()
            col_labels[col] = f"{current_year}_Q{qtr_num}"

    # --- Slice data rows (row 15 onward); col 1 = name, col_labels cols = data ---
    data_cols = list(col_labels.keys())
    df = raw.iloc[15:, [1] + data_cols].copy()
    df.columns = ["municipality"] + [col_labels[c] for c in data_cols]
    df = df.reset_index(drop=True)

    # --- Drop rows with no numeric data (region/province headers) ---
    numeric_cols = [col_labels[c] for c in data_cols]
    has_data = df[numeric_cols].apply(pd.to_numeric, errors="coerce").notna().any(axis=1)
    df = df[has_data].copy()

    # --- Filter to target cities ---
    df["municipality"] = df["municipality"].astype(str).str.strip()
    df = df[df["municipality"].isin(_CITIES)].copy()

    # --- Melt wide → long ---
    df_long = df.melt(id_vars="municipality", var_name="period", value_name="transactions")

    # --- Parse year and quarter from "YYYY_Qn" ---
    df_long["year"]    = df_long["period"].str[:4].astype(int)
    df_long["quarter"] = df_long["period"].str[-1].astype(int)
    df_long = df_long.drop(columns="period")

    # --- Cast transactions to numeric; drop missing ---
    df_long["transactions"] = pd.to_numeric(df_long["transactions"], errors="coerce")
    df_long = df_long.dropna(subset=["transactions"])
    df_long["transactions"] = df_long["transactions"].astype(int)

    # --- Final column order ---
    df_long = df_long[["municipality", "year", "quarter", "transactions"]]
    df_long = df_long.sort_values(["municipality", "year", "quarter"]).reset_index(drop=True)

    return df_long


def load(df: pd.DataFrame) -> int:
    """Load the transformed DataFrame into BigQuery raw layer.

    Args:
        df: Transformed DataFrame from transform().

    Returns:
        Number of rows loaded.
    """
    from src.processing.bq_loader import load_to_bigquery

    return load_to_bigquery(df, "barrioscout_raw.ministerio_transacciones")


def main() -> None:
    print("=== Ministerio transacciones pipeline ===")

    raw = extract()
    df  = transform(raw)

    print(f"  Rows      : {len(df):,}")
    print(f"  Cities    : {sorted(df['municipality'].unique())}")
    print(f"  Year range: {df['year'].min()} – {df['year'].max()}")

    # Show last 4 quarters per city
    for city in sorted(df["municipality"].unique()):
        last = df[df["municipality"] == city].tail(4)
        print(f"\n  {city} — last 4 quarters:")
        for _, row in last.iterrows():
            print(f"    {int(row.year)}-Q{int(row.quarter)}: {int(row.transactions):,} transactions")

    loaded = load(df)
    print(f"\n  Loaded: {loaded:,} rows → barrioscout_raw.ministerio_transacciones")


if __name__ == "__main__":
    main()
