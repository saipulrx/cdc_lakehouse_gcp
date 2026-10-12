#!/usr/bin/env bash
set -euo pipefail

# Load environment variables
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

mkdir -p "${SCRIPT_DIR}/configs"

echo ">>> [1/5] Retrieving Cloud SQL Public IP..."
PG_PUBLIC_IP=$(gcloud sql instances describe "${CLOUDSQL_INSTANCE}" \
  --project="${PROJECT_ID}" \
  --format="value(ipAddresses[0].ipAddress)")

if [[ -z "${PG_PUBLIC_IP}" ]]; then
  echo "ERROR: Could not find Public IP for Cloud SQL instance ${CLOUDSQL_INSTANCE}."
  exit 1
fi
echo "    Cloud SQL Public IP: ${PG_PUBLIC_IP}"

echo ">>> [2/5] Creating Datastream PostgreSQL Source Connection Profile (${DS_SOURCE_PROFILE})..."
if ! gcloud datastream connection-profiles describe "${DS_SOURCE_PROFILE}" \
  --location="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud datastream connection-profiles create "${DS_SOURCE_PROFILE}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --display-name="${DS_SOURCE_PROFILE}" \
    --type=POSTGRESQL \
    --postgresql-hostname="${PG_PUBLIC_IP}" \
    --postgresql-port=5432 \
    --postgresql-username="${PG_USER}" \
    --postgresql-password="${PG_PASSWORD}" \
    --postgresql-database="${PG_DATABASE}" \
    --static-ip-connectivity
else
  echo "    Source connection profile ${DS_SOURCE_PROFILE} already exists."
fi

echo ">>> [3/5] Creating Datastream BigQuery Destination Connection Profile (${DS_DEST_PROFILE})..."
if ! gcloud datastream connection-profiles describe "${DS_DEST_PROFILE}" \
  --location="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud datastream connection-profiles create "${DS_DEST_PROFILE}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --display-name="${DS_DEST_PROFILE}" \
    --type=BIGQUERY \
    --static-ip-connectivity
else
  echo "    Destination connection profile ${DS_DEST_PROFILE} already exists."
fi

echo ">>> [4/5] Generating Source & BigQuery Iceberg (BLMT) Destination JSON Configs..."

cat > "${SCRIPT_DIR}/configs/postgresql_source_config.json" <<EOF
{
  "replicationSlot": "${PG_REPLICATION_SLOT}",
  "publication": "${PG_PUBLICATION}",
  "includeObjects": {
    "postgresqlSchemas": [
      {
        "schema": "public",
        "postgresqlTables": [
          {
            "table": "orders"
          }
        ]
      }
    ]
  }
}
EOF

cat > "${SCRIPT_DIR}/configs/bigquery_iceberg_destination_config.json" <<EOF
{
  "singleTargetDataset": {
    "datasetId": "${PROJECT_ID}:${BQ_BRONZE_DATASET}"
  },
  "appendOnly": {},
  "blmtConfig": {
    "bucket": "${GCS_BUCKET}",
    "rootPath": "/bronze",
    "connectionName": "${PROJECT_ID}.${REGION}.${BQ_CONNECTION_ID}",
    "fileFormat": "PARQUET",
    "tableFormat": "ICEBERG"
  }
}
EOF

echo "    Generated ${SCRIPT_DIR}/configs/postgresql_source_config.json"
echo "    Generated ${SCRIPT_DIR}/configs/bigquery_iceberg_destination_config.json"

echo ">>> [5/5] Creating and Starting Datastream CDC Stream (${DS_STREAM_ID})..."
if ! gcloud datastream streams describe "${DS_STREAM_ID}" \
  --location="${REGION}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  gcloud datastream streams create "${DS_STREAM_ID}" \
    --location="${REGION}" \
    --project="${PROJECT_ID}" \
    --display-name="${DS_STREAM_ID}" \
    --source="${DS_SOURCE_PROFILE}" \
    --postgresql-source-config="${SCRIPT_DIR}/configs/postgresql_source_config.json" \
    --destination="${DS_DEST_PROFILE}" \
    --bigquery-destination-config="${SCRIPT_DIR}/configs/bigquery_iceberg_destination_config.json" \
    --backfill-all

  echo "    Waiting for stream creation operation to complete..."
  for i in {1..24}; do
    STREAM_STATE=$(gcloud datastream streams describe "${DS_STREAM_ID}" \
      --location="${REGION}" --project="${PROJECT_ID}" \
      --format="value(state)" 2>/dev/null || true)
    if [[ "${STREAM_STATE}" == "NOT_STARTED" || "${STREAM_STATE}" == "RUNNING" ]]; then
      echo "    Stream is ready (state=${STREAM_STATE})."
      break
    fi
    echo "    Still creating stream... waiting 10s (${i}/24)"
    sleep 10
  done
fi

echo "    Starting stream ${DS_STREAM_ID} (setting state=RUNNING)..."
gcloud datastream streams update "${DS_STREAM_ID}" \
  --location="${REGION}" \
  --project="${PROJECT_ID}" \
  --state=RUNNING \
  --update-mask=state

echo "=================================================================="
echo "Datastream Iceberg CDC Pipeline is now RUNNING!"
echo "Check stream status with:"
echo "  gcloud datastream streams describe ${DS_STREAM_ID} --location=${REGION} --project=${PROJECT_ID}"
echo "=================================================================="
