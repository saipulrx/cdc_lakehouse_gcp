#!/usr/bin/env bash
set -euo pipefail

# Load environment variables
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

echo "WARNING: This will delete the Datastream stream, connection profiles, Cloud SQL instance,"
echo "BigQuery datasets (${BQ_BRONZE_DATASET}, ${BQ_SILVER_DATASET}), and GCS bucket (gs://${GCS_BUCKET})."
read -r -p "Are you sure you want to proceed? (y/N): " CONFIRM
if [[ "${CONFIRM}" != "y" && "${CONFIRM}" != "Y" ]]; then
  echo "Cleanup cancelled."
  exit 0
fi

echo ">>> [1/5] Deleting Datastream Stream (${DS_STREAM_ID})..."
gcloud datastream streams delete "${DS_STREAM_ID}" \
  --location="${REGION}" --project="${PROJECT_ID}" --quiet || true

echo "    Waiting 15 seconds for stream deletion..."
sleep 15

echo ">>> [2/5] Deleting Datastream Connection Profiles..."
gcloud datastream connection-profiles delete "${DS_SOURCE_PROFILE}" \
  --location="${REGION}" --project="${PROJECT_ID}" --quiet || true
gcloud datastream connection-profiles delete "${DS_DEST_PROFILE}" \
  --location="${REGION}" --project="${PROJECT_ID}" --quiet || true

echo ">>> [3/5] Deleting Cloud SQL Instance (${CLOUDSQL_INSTANCE})..."
gcloud sql instances delete "${CLOUDSQL_INSTANCE}" \
  --project="${PROJECT_ID}" --quiet || true

echo ">>> [4/5] Deleting BigQuery Datasets & Connection..."
bq rm -r -f -d "${PROJECT_ID}:${BQ_BRONZE_DATASET}" || true
bq rm -r -f -d "${PROJECT_ID}:${BQ_SILVER_DATASET}" || true
bq rm --connection --project_id="${PROJECT_ID}" --location="${REGION}" "${BQ_CONNECTION_ID}" || true

echo ">>> [5/5] Deleting GCS Bucket (gs://${GCS_BUCKET})..."
gcloud storage rm --recursive "gs://${GCS_BUCKET}" --project="${PROJECT_ID}" || true

echo "=================================================================="
echo "Cleanup Complete!"
echo "=================================================================="
