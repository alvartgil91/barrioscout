"""
Ingestion module for Ministerio de Transportes real estate transaction data.

Source: quarterly CSV files published at mitma.gob.es
Schema target: barrioscout_raw.ministerio_transactions
"""

from __future__ import annotations

import io
import logging

import pandas as pd
import requests

from config.settings import MINISTERIO_BASE_URL

logger = logging.getLogger(__name__)


def extract(url: str) -> bytes:
    """Download the raw CSV bytes from the given URL.

    Args:
        url: Direct URL to the quarterly CSV file.

    Returns:
        Raw CSV content as bytes.
    """
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.content


def transform(raw: bytes) -> pd.DataFrame:
    """Parse and normalise the raw CSV into a clean DataFrame.

    Args:
        raw: Raw CSV bytes from extract().

    Returns:
        DataFrame with standardised column names and types.
    """
    df = pd.read_csv(io.BytesIO(raw), sep=";", encoding="latin-1", dtype=str)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def load(df: pd.DataFrame) -> None:
    """Load the transformed DataFrame into BigQuery raw layer.

    Args:
        df: Transformed DataFrame from transform().
    """
    from src.processing.bq_loader import load_dataframe
    load_dataframe(df, dataset="barrioscout_raw", table="ministerio_transactions")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("Ministerio ingestion not yet fully wired — run test_sources.py to validate.")
