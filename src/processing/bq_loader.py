"""
Generic BigQuery loader used by all ingestion modules.

Provides a single load_dataframe() function that handles auth,
schema inference, and append writes to the raw layer.
"""

from __future__ import annotations

import logging

import pandas as pd
from google.cloud import bigquery

from config.settings import GCP_PROJECT_ID

logger = logging.getLogger(__name__)


def load_dataframe(
    df: pd.DataFrame,
    dataset: str,
    table: str,
    project: str = GCP_PROJECT_ID,
    write_disposition: str = "WRITE_APPEND",
) -> None:
    """Load a DataFrame into a BigQuery table.

    Creates the table if it does not exist (schema inferred from DataFrame).
    Defaults to append mode — raw layer is never overwritten.

    Args:
        df: DataFrame to load.
        dataset: BigQuery dataset name (e.g. 'barrioscout_raw').
        table: Target table name.
        project: GCP project ID.
        write_disposition: 'WRITE_APPEND' (default) or 'WRITE_TRUNCATE'.
    """
    if df.empty:
        logger.warning("Empty DataFrame — skipping load to %s.%s", dataset, table)
        return

    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for completion

    logger.info(
        "Loaded %d rows into %s (disposition=%s)",
        len(df),
        table_ref,
        write_disposition,
    )
