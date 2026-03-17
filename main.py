"""Cloud Function entry point for Idealista email ingestion pipeline.

Reads Gmail OAuth credentials from Secret Manager, runs the existing
extract -> transform -> load -> post_process pipeline, and returns a
JSON summary. Designed for Cloud Functions 2nd gen with HTTP trigger.

The local pipeline (python -m src.ingestion.idealista_emails) is unaffected.
"""

from __future__ import annotations

import json
import logging

import functions_framework
from google.cloud import secretmanager
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

from config.settings import GCP_PROJECT_ID, GMAIL_SCOPES
from src.ingestion.idealista_emails import extract, transform, load, post_process

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Secret Manager helpers
# ---------------------------------------------------------------------------

_SM_CLIENT: secretmanager.SecretManagerServiceClient | None = None

# Max emails per Cloud Function invocation to stay within timeout
_CF_MAX_EMAILS = 50


def _get_sm_client() -> secretmanager.SecretManagerServiceClient:
    """Lazy-initialise the Secret Manager client (reused across warm starts)."""
    global _SM_CLIENT
    if _SM_CLIENT is None:
        _SM_CLIENT = secretmanager.SecretManagerServiceClient()
    return _SM_CLIENT


def _read_secret(secret_id: str) -> str:
    """Read the latest version of a secret from Secret Manager."""
    client = _get_sm_client()
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


def _write_secret_version(secret_id: str, data: str) -> None:
    """Add a new version to an existing secret in Secret Manager."""
    client = _get_sm_client()
    parent = client.secret_path(GCP_PROJECT_ID, secret_id)
    client.add_secret_version(
        request={"parent": parent, "payload": {"data": data.encode("utf-8")}}
    )


# ---------------------------------------------------------------------------
# OAuth2 credentials from Secret Manager
# ---------------------------------------------------------------------------


def _build_credentials() -> Credentials:
    """Build Gmail OAuth2 credentials from Secret Manager secrets.

    Reads the token JSON, refreshes if expired, and writes the updated
    token back to Secret Manager so the next invocation gets a valid token.

    Raises:
        RuntimeError: If the token cannot be refreshed (e.g. refresh_token
                      revoked). Must be regenerated locally and re-uploaded.
    """
    token_json = json.loads(_read_secret("gmail-oauth-token"))
    creds = Credentials.from_authorized_user_info(token_json, GMAIL_SCOPES)

    if creds.valid:
        return creds

    if creds.expired and creds.refresh_token:
        logger.info("OAuth token expired — refreshing")
        creds.refresh(Request())
        # Persist the refreshed token back to Secret Manager
        _write_secret_version("gmail-oauth-token", creds.to_json())
        logger.info("Refreshed token written to Secret Manager")
        return creds

    raise RuntimeError(
        "Gmail OAuth token is invalid and cannot be refreshed. "
        "Regenerate locally with `python -m src.ingestion.idealista_emails` "
        "and upload the new token to Secret Manager."
    )


# ---------------------------------------------------------------------------
# Cloud Function entry point
# ---------------------------------------------------------------------------


@functions_framework.http
def idealista_ingest(request):
    """HTTP Cloud Function: run Idealista email ingestion pipeline.

    Args:
        request: Flask request object (unused, required by framework).

    Returns:
        Tuple of (JSON response body, HTTP status code).
    """
    try:
        creds = _build_credentials()

        listings = extract(max_emails=_CF_MAX_EMAILS, creds=creds)
        if not listings:
            logger.info("No new listings found")
            return json.dumps({"status": "ok", "rows_loaded": 0, "message": "No new emails"}), 200

        logger.info("Extracted %d raw listings", len(listings))

        df = transform(listings)
        rows_loaded = load(df)

        email_ids = list({row["email_id"] for row in listings})
        post_process(email_ids, creds=creds)

        logger.info("Pipeline complete: %d rows loaded, %d emails processed", rows_loaded, len(email_ids))
        return json.dumps({
            "status": "ok",
            "rows_loaded": rows_loaded,
            "emails_processed": len(email_ids),
        }), 200

    except Exception as exc:
        logger.exception("Pipeline failed")
        return json.dumps({"status": "error", "message": str(exc)}), 500
