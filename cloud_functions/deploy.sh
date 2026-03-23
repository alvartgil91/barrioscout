#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# deploy.sh — Deploy Idealista ingestion Cloud Function + Cloud Scheduler
#
# Deploys from the repo root (--source=.) so the Cloud Function imports
# src/ and config/ directly without copying files.
#
# Usage:
#   cd barrioscout/
#   bash cloud_functions/deploy.sh
# ---------------------------------------------------------------------------
set -euo pipefail

PROJECT="portfolio-alvartgil91"
REGION="europe-west1"
SA_NAME="barrioscout-cf"
SA_EMAIL="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
FUNCTION_NAME="idealista-ingest"
SCHEDULER_JOB="idealista-ingest-schedule"

# Read GOOGLE_GEOCODING_API_KEY from .env (repo root)
REPO_ROOT_EARLY="$(cd "$(dirname "$0")/.." && pwd)"
GOOGLE_GEOCODING_API_KEY=$(grep -E '^GOOGLE_GEOCODING_API_KEY=' "${REPO_ROOT_EARLY}/.env" | cut -d'=' -f2- | sed 's/[[:space:]"'"'"']//g')
if [[ -z "$GOOGLE_GEOCODING_API_KEY" ]]; then
    echo "ERROR: GOOGLE_GEOCODING_API_KEY not found in .env — aborting deploy."
    exit 1
fi

# ---------------------------------------------------------------------------
# 1. Create service account (if it doesn't exist)
# ---------------------------------------------------------------------------
echo "=== Step 1: Service account ==="
if gcloud iam service-accounts describe "$SA_EMAIL" --project="$PROJECT" &>/dev/null; then
    echo "Service account $SA_EMAIL already exists — skipping creation"
else
    echo "Creating service account $SA_NAME..."
    gcloud iam service-accounts create "$SA_NAME" \
        --project="$PROJECT" \
        --display-name="BarrioScout Cloud Functions"
fi

# ---------------------------------------------------------------------------
# 2. Assign IAM roles
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 2: IAM roles ==="
ROLES=(
    "roles/secretmanager.secretAccessor"     # Read OAuth token + credentials
    "roles/secretmanager.secretVersionAdder"  # Write refreshed OAuth token
    "roles/bigquery.dataEditor"               # Insert rows into BQ tables
    "roles/bigquery.jobUser"                  # Execute BQ load jobs
)

for ROLE in "${ROLES[@]}"; do
    echo "Ensuring $ROLE..."
    gcloud projects add-iam-policy-binding "$PROJECT" \
        --member="serviceAccount:${SA_EMAIL}" \
        --role="$ROLE" \
        --condition=None \
        --quiet
done

# ---------------------------------------------------------------------------
# 3. Prepare requirements.txt for Cloud Functions
#    Cloud Functions expects requirements.txt in the source root.
#    We keep cf_requirements.txt as the source of truth to avoid
#    overwriting the project's main requirements.txt.
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 3: Preparing requirements.txt ==="
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ORIGINAL_REQ="${REPO_ROOT}/requirements.txt"
CF_REQ="${REPO_ROOT}/cf_requirements.txt"
BACKUP_REQ="${REPO_ROOT}/requirements.txt.bak"

# Back up original requirements.txt
cp "$ORIGINAL_REQ" "$BACKUP_REQ"
# Replace with Cloud Function deps
cp "$CF_REQ" "$ORIGINAL_REQ"
echo "Swapped requirements.txt with cf_requirements.txt (original backed up)"

# Restore on exit (even if deploy fails)
trap 'echo ""; echo "=== Cleanup: restoring original requirements.txt ==="; mv "$BACKUP_REQ" "$ORIGINAL_REQ"; echo "Done."' EXIT

# ---------------------------------------------------------------------------
# 4. Deploy Cloud Function (2nd gen)
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 4: Deploying Cloud Function ==="
gcloud functions deploy "$FUNCTION_NAME" \
    --gen2 \
    --runtime=python312 \
    --region="$REGION" \
    --source="$REPO_ROOT" \
    --entry-point=idealista_ingest \
    --trigger-http \
    --no-allow-unauthenticated \
    --memory=512MB \
    --timeout=540s \
    --max-instances=1 \
    --min-instances=0 \
    --service-account="$SA_EMAIL" \
    --set-env-vars="GCP_PROJECT_ID=${PROJECT},GOOGLE_GEOCODING_API_KEY=${GOOGLE_GEOCODING_API_KEY}" \
    --project="$PROJECT"

FUNCTION_URL=$(gcloud functions describe "$FUNCTION_NAME" \
    --gen2 \
    --region="$REGION" \
    --project="$PROJECT" \
    --format="value(serviceConfig.uri)")
echo "Function deployed at: $FUNCTION_URL"

# ---------------------------------------------------------------------------
# 5. Create Cloud Scheduler job (every 6 hours)
# ---------------------------------------------------------------------------
echo ""
echo "=== Step 5: Cloud Scheduler ==="
if gcloud scheduler jobs describe "$SCHEDULER_JOB" --location="$REGION" --project="$PROJECT" &>/dev/null; then
    echo "Scheduler job $SCHEDULER_JOB already exists — updating..."
    gcloud scheduler jobs update http "$SCHEDULER_JOB" \
        --location="$REGION" \
        --schedule="0 */6 * * *" \
        --uri="$FUNCTION_URL" \
        --http-method=POST \
        --oidc-service-account-email="$SA_EMAIL" \
        --oidc-token-audience="$FUNCTION_URL" \
        --project="$PROJECT"
else
    echo "Creating scheduler job $SCHEDULER_JOB..."
    gcloud scheduler jobs create http "$SCHEDULER_JOB" \
        --location="$REGION" \
        --schedule="0 */6 * * *" \
        --uri="$FUNCTION_URL" \
        --http-method=POST \
        --oidc-service-account-email="$SA_EMAIL" \
        --oidc-token-audience="$FUNCTION_URL" \
        --project="$PROJECT"
fi

echo ""
echo "=== Deploy complete ==="
echo "Function: $FUNCTION_URL"
echo "Schedule: every 6 hours (0 */6 * * *)"
echo "Service account: $SA_EMAIL"
