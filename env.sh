#!/usr/bin/env bash
# ==============================================================================
# Environment Configuration for:
# Real-Time CDC from Cloud SQL (PostgreSQL) to BigQuery Apache Iceberg Tables
# ==============================================================================

# 1. Core GCP Project & Region Settings
export PROJECT_ID="${PROJECT_ID:-$(gcloud config get-value project 2>/dev/null)}"
export REGION="${REGION:-us-central1}"

# 2. Google Cloud Storage & BigQuery Lakehouse Settings
export GCS_BUCKET="${PROJECT_ID}-iceberg-lakehouse"
export BQ_CONNECTION_ID="iceberg-gcs-connection"
export BQ_BRONZE_DATASET="lakehouse_bronze"
export BQ_SILVER_DATASET="lakehouse_silver"

# 3. Cloud SQL for PostgreSQL Settings
export CLOUDSQL_INSTANCE="pg-cdc-source"
export PG_DATABASE="ecommerce"
export PG_USER="postgres"
export PG_PASSWORD="${PG_PASSWORD:-ChangeMe_Lakehouse2026!}"
export PG_PUBLICATION="datastream_iceberg_pub"
export PG_REPLICATION_SLOT="datastream_iceberg_slot"

# 4. Google Cloud Datastream Settings
export DS_SOURCE_PROFILE="pg-source-profile"
export DS_DEST_PROFILE="bq-iceberg-dest-profile"
export DS_STREAM_ID="pg-to-iceberg-cdc-stream"

echo "=================================================="
echo "Loaded Configuration:"
echo "  PROJECT_ID         : ${PROJECT_ID}"
echo "  REGION             : ${REGION}"
echo "  GCS_BUCKET         : gs://${GCS_BUCKET}"
echo "  BQ_CONNECTION_ID   : ${BQ_CONNECTION_ID}"
echo "  BQ_BRONZE_DATASET  : ${BQ_BRONZE_DATASET}"
echo "  BQ_SILVER_DATASET  : ${BQ_SILVER_DATASET}"
echo "  CLOUDSQL_INSTANCE  : ${CLOUDSQL_INSTANCE}"
echo "  DS_STREAM_ID       : ${DS_STREAM_ID}"
echo "=================================================="
