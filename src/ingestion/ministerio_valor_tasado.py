"""
Ingestion module for Ministerio de Transportes — appraised housing value by municipality.

Source: Boletín estadístico online (apps.fomento.gob.es/BoletinOnline2/)
        Local file: data/raw/ministerio_valor_tasado_municipio.xls
Schema target: barrioscout_raw.ministerio_valor_tasado

NOTE: transportes.gob.es blocks programmatic downloads via CloudFront WAF.
      The XLS is downloaded manually and read from disk — no HTTP calls here.

File structure (confirmed by probing):
  84 sheets, one per quarter: "T1A2005" → "T4A2025".
  Each sheet: rows 0-18 = metadata; data from row 19.
  Col 1: Province (merged cells — first row of each provincial block only).
  Col 2: Municipality name.
  Col 3: Appraised value €/m² (total); "n.r" = not reported → NaN.
  Col 5: Number of appraisals; "n.r" → NaN.
  Only municipalities with >25,000 inhabitants are included (~283-304 per sheet).
"""

from __future__ import annotations

import pandas as pd

from config.settings import MINISTERIO_VALOR_TASADO_XLS

_CITIES = {"Granada", "Madrid"}


def extract() -> dict[str, pd.DataFrame]:
    """Read all sheets from the local XLS and return raw DataFrames.

    Returns:
        Dict mapping sheet name (e.g. "T1A2005") to raw DataFrame (header=None).
    """
    print(f"  Reading {MINISTERIO_VALOR_TASADO_XLS} ...", end=" ", flush=True)
    xl = pd.ExcelFile(MINISTERIO_VALOR_TASADO_XLS)
    sheets: dict[str, pd.DataFrame] = {}
    for name in xl.sheet_names:
        sheets[name] = pd.read_excel(MINISTERIO_VALOR_TASADO_XLS,
                                     sheet_name=name, header=None)
    print(f"{len(sheets)} sheets")
    return sheets


def _parse_sheet_name(name: str) -> tuple[int, int]:
    """Parse sheet name "TnAyyyy" into (quarter, year).

    Args:
        name: Sheet name such as "T1A2005" or "T3A2025 " (may have trailing spaces).

    Returns:
        (quarter, year) as ints, e.g. (1, 2005).
    """
    name = name.strip()
    quarter = int(name[1])
    year    = int(name[3:7])
    return quarter, year


def transform(sheets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Parse all sheets and return a long DataFrame for Granada and Madrid.

    For each sheet:
      - Forward-fill province column (merged cells).
      - Replace "n.r" with NaN.
      - Filter to target municipalities.
      - Append year and quarter columns.

    Args:
        sheets: Dict from extract().

    Returns:
        DataFrame with columns: province, municipality, year, quarter,
        appraised_value_eur_m2, num_appraisals.
    """
    frames: list[pd.DataFrame] = []

    for sheet_name, raw in sheets.items():
        quarter, year = _parse_sheet_name(sheet_name)

        # Slice data rows (row 19 onward); keep cols 1, 2, 3, 5
        data = raw.iloc[19:, [1, 2, 3, 5]].copy()
        data.columns = ["province", "municipality", "appraised_value_eur_m2", "num_appraisals"]
        data = data.reset_index(drop=True)

        # Forward-fill province (merged cells leave NaN in continuation rows)
        data["province"] = data["province"].ffill()

        # Drop rows with no municipality name
        data = data[data["municipality"].notna()].copy()
        data["municipality"] = data["municipality"].astype(str).str.strip()

        # Replace "n.r" with NaN
        data["appraised_value_eur_m2"] = data["appraised_value_eur_m2"].replace("n.r", pd.NA)
        data["num_appraisals"]         = data["num_appraisals"].replace("n.r", pd.NA)

        # Convert to float
        data["appraised_value_eur_m2"] = pd.to_numeric(
            data["appraised_value_eur_m2"], errors="coerce"
        )
        data["num_appraisals"] = pd.to_numeric(data["num_appraisals"], errors="coerce")

        # Filter to target cities
        data = data[data["municipality"].isin(_CITIES)].copy()
        if data.empty:
            continue

        # Drop rows where both value columns are NaN
        data = data.dropna(subset=["appraised_value_eur_m2", "num_appraisals"], how="all")

        data["year"]    = year
        data["quarter"] = quarter

        frames.append(data)

    if not frames:
        return pd.DataFrame(columns=[
            "province", "municipality", "year", "quarter",
            "appraised_value_eur_m2", "num_appraisals",
        ])

    combined = pd.concat(frames, ignore_index=True)
    combined["province"] = combined["province"].astype(str).str.strip()

    combined = combined[[
        "province", "municipality", "year", "quarter",
        "appraised_value_eur_m2", "num_appraisals",
    ]]
    combined = combined.sort_values(
        ["municipality", "year", "quarter"]
    ).reset_index(drop=True)

    return combined


def load(df: pd.DataFrame) -> int:
    """Load the transformed DataFrame into BigQuery raw layer.

    Args:
        df: Transformed DataFrame from transform().

    Returns:
        Number of rows loaded.
    """
    from src.processing.bq_loader import load_to_bigquery

    return load_to_bigquery(df, "barrioscout_raw.ministerio_valor_tasado")


def main() -> None:
    print("=== Ministerio valor tasado pipeline ===")

    sheets = extract()
    df     = transform(sheets)

    print(f"  Rows      : {len(df):,}")
    print(f"  Cities    : {sorted(df['municipality'].unique())}")
    print(f"  Year range: {df['year'].min()} – {df['year'].max()}")

    # Show last 4 quarters per city
    for city in sorted(df["municipality"].unique()):
        last = df[df["municipality"] == city].tail(4)
        print(f"\n  {city} — last 4 quarters:")
        for _, row in last.iterrows():
            val = f"{row.appraised_value_eur_m2:.1f}" if pd.notna(row.appraised_value_eur_m2) else "n.r"
            tas = f"{row.num_appraisals:.0f}"          if pd.notna(row.num_appraisals)         else "n.r"
            print(f"    {int(row.year)}-Q{int(row.quarter)}: €/m²={val}  appraisals={tas}")

    loaded = load(df)
    print(f"\n  Loaded: {loaded:,} rows → barrioscout_raw.ministerio_valor_tasado")


if __name__ == "__main__":
    main()
