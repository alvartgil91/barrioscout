"""
Generic BigQuery loader used by all ingestion modules.

Provides load_to_bigquery() that handles auth, schema inference,
_loaded_at injection, and append writes to the raw layer.
"""

from __future__ import annotations

import datetime

import pandas as pd
from google.cloud import bigquery

from config.settings import GCP_PROJECT_ID


def load_to_bigquery(
    df: pd.DataFrame,
    table_id: str,
    write_disposition: str = "WRITE_APPEND",
) -> int:
    """Load a DataFrame into a BigQuery table.

    Automatically injects a _loaded_at column (UTC timestamp) before loading.
    Creates the table if it does not exist (schema inferred from DataFrame).
    Defaults to append mode — raw layer is never overwritten.

    Args:
        df: DataFrame to load.
        table_id: Target table in 'dataset.table_name' format.
        write_disposition: 'WRITE_APPEND' (default) or 'WRITE_TRUNCATE'.

    Returns:
        Number of rows loaded.

    Raises:
        ValueError: If df is empty.
        RuntimeError: If the BigQuery job fails.
    """
    if df.empty:
        raise ValueError(f"Empty DataFrame — nothing to load into {table_id}")

    df = df.copy()
    df["_loaded_at"] = datetime.datetime.now(datetime.timezone.utc)

    full_table_ref = f"{GCP_PROJECT_ID}.{table_id}"
    print(f"Loading {len(df)} rows into {full_table_ref} ({write_disposition})...")

    client = bigquery.Client(project=GCP_PROJECT_ID)
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True,
    )

    try:
        job = client.load_table_from_dataframe(df, full_table_ref, job_config=job_config)
        job.result()
    except Exception as exc:
        raise RuntimeError(f"BigQuery load failed for {full_table_ref}: {exc}") from exc

    print(f"OK — {len(df)} rows loaded into {full_table_ref}")
    return len(df)
