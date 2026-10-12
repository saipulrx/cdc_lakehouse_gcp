#!/usr/bin/env bash
set -euo pipefail

# Load environment variables
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "${SCRIPT_DIR}/env.sh"

echo "=================================================================="
echo ">>> [1/4] Inspecting Bronze Iceberg CDC Change Log in BigQuery..."
echo "=================================================================="
bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" "
SELECT
  order_id,
  customer_id,
  order_status,
  total_amount,
  datastream_metadata.change_type,
  TIMESTAMP_MICROS(datastream_metadata.source_timestamp) AS source_ts,
  datastream_metadata.change_sequence_number AS csn
FROM \`${PROJECT_ID}.${BQ_BRONZE_DATASET}.public_orders\`
ORDER BY order_id, datastream_metadata.source_timestamp DESC, csn DESC;
"

echo "=================================================================="
echo ">>> [2/4] Creating & Querying Real-Time Deduplicated View (Pattern A)..."
echo "=================================================================="
bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" "
CREATE OR REPLACE VIEW \`${PROJECT_ID}.${BQ_SILVER_DATASET}.v_orders_current\` AS
SELECT
  order_id,
  customer_id,
  product_category,
  order_status,
  total_amount,
  shipping_city,
  updated_at,
  TIMESTAMP_MICROS(datastream_metadata.source_timestamp) AS last_cdc_timestamp,
  datastream_metadata.change_type AS last_change_type
FROM \`${PROJECT_ID}.${BQ_BRONZE_DATASET}.public_orders\`
WHERE datastream_metadata.change_type != 'UPDATE-DELETE'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY order_id
  ORDER BY
    datastream_metadata.source_timestamp DESC,
    datastream_metadata.change_sequence_number DESC
) = 1
AND datastream_metadata.change_type != 'DELETE';

SELECT * FROM \`${PROJECT_ID}.${BQ_SILVER_DATASET}.v_orders_current\` ORDER BY order_id;
"

echo "=================================================================="
echo ">>> [3/4] Creating & Merging into Materialized Silver Iceberg Table (Pattern B)..."
echo "=================================================================="
bq query --use_legacy_sql=false --project_id="${PROJECT_ID}" "
CREATE TABLE IF NOT EXISTS \`${PROJECT_ID}.${BQ_SILVER_DATASET}.orders\` (
  order_id STRING,
  customer_id STRING,
  product_category STRING,
  order_status STRING,
  total_amount NUMERIC,
  shipping_city STRING,
  updated_at TIMESTAMP,
  last_cdc_timestamp TIMESTAMP
)
CLUSTER BY order_id
WITH CONNECTION \`${PROJECT_ID}.${REGION}.${BQ_CONNECTION_ID}\`
OPTIONS (
  file_format = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri = 'gs://${GCS_BUCKET}/silver/orders'
);

MERGE \`${PROJECT_ID}.${BQ_SILVER_DATASET}.orders\` AS T
USING (
  SELECT
    order_id,
    customer_id,
    product_category,
    order_status,
    total_amount,
    shipping_city,
    updated_at,
    TIMESTAMP_MICROS(datastream_metadata.source_timestamp) AS source_ts,
    datastream_metadata.change_type AS change_type
  FROM \`${PROJECT_ID}.${BQ_BRONZE_DATASET}.public_orders\`
  WHERE datastream_metadata.change_type != 'UPDATE-DELETE'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY
      datastream_metadata.source_timestamp DESC,
      datastream_metadata.change_sequence_number DESC
  ) = 1
) AS S
ON T.order_id = S.order_id
WHEN MATCHED AND S.change_type = 'DELETE' THEN
  DELETE
WHEN MATCHED THEN
  UPDATE SET
    customer_id        = S.customer_id,
    product_category   = S.product_category,
    order_status       = S.order_status,
    total_amount       = S.total_amount,
    shipping_city      = S.shipping_city,
    updated_at         = S.updated_at,
    last_cdc_timestamp = S.source_ts
WHEN NOT MATCHED AND S.change_type != 'DELETE' THEN
  INSERT (
    order_id, customer_id, product_category, order_status,
    total_amount, shipping_city, updated_at, last_cdc_timestamp
  )
  VALUES (
    S.order_id, S.customer_id, S.product_category, S.order_status,
    S.total_amount, S.shipping_city, S.updated_at, S.source_ts
  );

EXPORT TABLE METADATA FROM \`${PROJECT_ID}.${BQ_SILVER_DATASET}.orders\`;

SELECT * FROM \`${PROJECT_ID}.${BQ_SILVER_DATASET}.orders\` ORDER BY order_id;
"

echo "=================================================================="
echo ">>> [4/4] Verifying Open Parquet & Apache Iceberg Metadata in GCS..."
echo "=================================================================="
gcloud storage ls --recursive "gs://${GCS_BUCKET}/"
