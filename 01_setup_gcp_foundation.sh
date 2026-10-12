#!/usr/bin/env bash
set -euo pipefail

# Load environment variables
source "$(dirname "$0")/env.sh"

if [[ -z "${PROJECT_ID}" || "${PROJECT_ID}" == "(unset)" ]]; then
  echo "ERROR: PROJECT_ID is not set. Run 'export PROJECT_ID=your-project-id' first."
  exit 1
fi

echo ">>> [1/6] Enabling required Google Cloud APIs..."
gcloud services enable \
  sqladmin.googleapis.com \
  datastream.googleapis.com \
  bigquery.googleapis.com \
  bigqueryconnection.googleapis.com \
  biglake.googleapis.com \
  storage.googleapis.com \
  --project="${PROJECT_ID}"

echo ">>> [2/6] Creating Cloud Storage Bucket for Apache Iceberg (gs://${GCS_BUCKET})..."
if ! gcloud storage buckets describe "gs://${GCS_BUCKET}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud storage buckets create "gs://${GCS_BUCKET}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --uniform-bucket-level-access
else
  echo "    Bucket gs://${GCS_BUCKET} already exists."
fi

echo ">>> [3/6] Creating BigQuery Cloud Resource Connection (${BQ_CONNECTION_ID})..."
if ! bq show --connection "${PROJECT_ID}.${REGION}.${BQ_CONNECTION_ID}" >/dev/null 2>&1; then
  bq mk --connection \
    --connection_type=CLOUD_RESOURCE \
    --project_id="${PROJECT_ID}" \
    --location="${REGION}" \
    "${BQ_CONNECTION_ID}"
else
  echo "    Connection ${BQ_CONNECTION_ID} already exists."
fi

# Retrieve the Service Account ID from the BigQuery Connection
BQ_CONN_SA=$(bq show --format=json --connection "${PROJECT_ID}.${REGION}.${BQ_CONNECTION_ID}" \
  | python3 -c "import sys, json; print(json.load(sys.stdin)['cloudResource']['serviceAccountId'])")

echo "    BigQuery Connection Service Account: ${BQ_CONN_SA}"
echo "    Granting roles/storage.admin on gs://${GCS_BUCKET} to ${BQ_CONN_SA}..."
gcloud storage buckets add-iam-policy-binding "gs://${GCS_BUCKET}" \
  --member="serviceAccount:${BQ_CONN_SA}" \
  --role="roles/storage.admin" \
  --project="${PROJECT_ID}" >/dev/null

echo ">>> [4/6] Creating BigQuery Bronze & Silver Datasets in ${REGION}..."
for DATASET in "${BQ_BRONZE_DATASET}" "${BQ_SILVER_DATASET}"; do
  if ! bq show --dataset "${PROJECT_ID}:${DATASET}" >/dev/null 2>&1; then
    bq --location="${REGION}" mk --dataset \
      --description="Open Lakehouse Dataset (${DATASET}) for Apache Iceberg" \
      "${PROJECT_ID}:${DATASET}"
  else
    echo "    Dataset ${DATASET} already exists."
  fi
done

echo ">>> [5/6] Fetching Datastream Static IPs for region ${REGION}..."
DS_IPS=$(gcloud datastream locations fetch-static-ips "${REGION}" \
  --project="${PROJECT_ID}" \
  --format="value(staticIps)" | tr ';' ',' | tr '\n' ',' | sed 's/,$//')
echo "    Datastream Regional IPs to allowlist: ${DS_IPS}"

echo ">>> [6/6] Creating Cloud SQL for PostgreSQL Instance (${CLOUDSQL_INSTANCE})..."
echo "    (Note: Cloud SQL enabled with cloudsql.logical_decoding=on and Datastream IPs allowlisted)"
if ! gcloud sql instances describe "${CLOUDSQL_INSTANCE}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud sql instances create "${CLOUDSQL_INSTANCE}" \
    --project="${PROJECT_ID}" \
    --database-version=POSTGRES_15 \
    --edition=ENTERPRISE \
    --tier=db-custom-2-7680 \
    --region="${REGION}" \
    --root-password="${PG_PASSWORD}" \
    --database-flags=cloudsql.logical_decoding=on,max_replication_slots=10,max_wal_senders=10 \
    --authorized-networks="${DS_IPS}" \
    --storage-type=SSD \
    --storage-size=20GB
else
  echo "    Instance ${CLOUDSQL_INSTANCE} already exists. Ensuring flags and authorized networks..."
  gcloud sql instances patch "${CLOUDSQL_INSTANCE}" \
    --project="${PROJECT_ID}" \
    --database-flags=cloudsql.logical_decoding=on,max_replication_slots=10,max_wal_senders=10 \
    --authorized-networks="${DS_IPS}" \
    --quiet
fi

echo ">>> Creating database '${PG_DATABASE}' if not exists..."
if ! gcloud sql databases describe "${PG_DATABASE}" --instance="${CLOUDSQL_INSTANCE}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud sql databases create "${PG_DATABASE}" \
    --instance="${CLOUDSQL_INSTANCE}" \
    --project="${PROJECT_ID}"
fi

PG_PUBLIC_IP=$(gcloud sql instances describe "${CLOUDSQL_INSTANCE}" \
  --project="${PROJECT_ID}" \
  --format="value(ipAddresses[0].ipAddress)")

echo "=================================================================="
echo "Foundation Setup Complete!"
echo "  Cloud SQL Public IP        : ${PG_PUBLIC_IP}"
echo "  BQ Connection SA           : ${BQ_CONN_SA}"
echo "  GCS Iceberg Bucket         : gs://${GCS_BUCKET}"
echo ""
echo "Next Step:"
echo "  Run sql/02_postgres_init_cdc.sql against your Cloud SQL instance:"
echo "  gcloud sql connect ${CLOUDSQL_INSTANCE} --user=${PG_USER} --database=${PG_DATABASE} --project=${PROJECT_ID} < sql/02_postgres_init_cdc.sql"
echo "=================================================================="
