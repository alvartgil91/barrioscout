"""
Ingestion module for INE (Instituto Nacional de Estadística) data.

Source: Atlas de distribución de renta de los hogares
URL: https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/30896.csv
Schema target: barrioscout_raw.ine_renta
"""

from __future__ import annotations

import io
import logging

import pandas as pd
import requests

from config.settings import INE_RENTA_URL

logger = logging.getLogger(__name__)


def extract(url: str = INE_RENTA_URL) -> bytes:
    """Download the INE CSV file.

    Args:
        url: URL to the INE CSV file (defaults to renta media per persona).

    Returns:
        Raw CSV content as bytes.
    """
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.content


def transform(raw: bytes) -> pd.DataFrame:
    """Parse and normalise the INE CSV into a clean DataFrame.

    Args:
        raw: Raw CSV bytes from extract().

    Returns:
        DataFrame with standardised column names.
    """
    df = pd.read_csv(
        io.BytesIO(raw),
        sep="\t",
        encoding="utf-8-sig",
        thousands=".",
        decimal=",",
        dtype=str,
    )
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    # Drop empty rows
    df = df.dropna(how="all")
    return df


def load(df: pd.DataFrame) -> None:
    """Load the transformed DataFrame into BigQuery raw layer.

    Args:
        df: Transformed DataFrame from transform().
    """
    from src.processing.bq_loader import load_dataframe
    load_dataframe(df, dataset="barrioscout_raw", table="ine_renta")
