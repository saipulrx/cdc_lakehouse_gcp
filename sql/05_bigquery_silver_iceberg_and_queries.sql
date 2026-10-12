-- ==============================================================================
-- Step 5: BigQuery Apache Iceberg Lakehouse Queries (Bronze -> Silver)
-- Replace ${PROJECT_ID}, ${REGION}, and ${GCS_BUCKET} if running manually in Console,
-- or execute directly via `./06_verify_gcs_and_bigquery.sh` which substitutes them.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. Inspect the Bronze Managed Iceberg Table (Append-Only CDC Change Log)
-- Notice how every INSERT, UPDATE-INSERT, and DELETE event is preserved with
-- `datastream_metadata` (uuid, source_timestamp, change_sequence_number, change_type).
-- ------------------------------------------------------------------------------
SELECT
  order_id,
  customer_id,
  product_category,
  order_status,
  total_amount,
  shipping_city,
  updated_at,
  datastream_metadata.change_type,
  TIMESTAMP_MICROS(datastream_metadata.source_timestamp) AS source_ts,
  datastream_metadata.change_sequence_number AS csn
FROM `${PROJECT_ID}.lakehouse_bronze.public_orders`
ORDER BY order_id, datastream_metadata.source_timestamp DESC, csn DESC;


-- ------------------------------------------------------------------------------
-- 2. Pattern A: Zero-Latency Deduplicated View (`v_orders_current`)
-- Dynamically resolves the latest row state per `order_id` and filters out deletes
-- (`change_type != 'DELETE'`) without duplicating storage.
-- ------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `${PROJECT_ID}.lakehouse_silver.v_orders_current` AS
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
FROM `${PROJECT_ID}.lakehouse_bronze.public_orders`
WHERE datastream_metadata.change_type != 'UPDATE-DELETE'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY order_id
  ORDER BY
    datastream_metadata.source_timestamp DESC,
    datastream_metadata.change_sequence_number DESC
) = 1
AND datastream_metadata.change_type != 'DELETE';

-- Query the live deduplicated view (should match PostgreSQL's 6 active rows!)
SELECT *
FROM `${PROJECT_ID}.lakehouse_silver.v_orders_current`
ORDER BY order_id;


-- ------------------------------------------------------------------------------
-- 3. Pattern B: Materialized Silver Apache Iceberg Table (`lakehouse_silver.orders`)
-- Stored as open Parquet + Iceberg v2 metadata in your GCS bucket.
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.lakehouse_silver.orders` (
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
WITH CONNECTION `${PROJECT_ID}.${REGION}.iceberg-gcs-connection`
OPTIONS (
  file_format = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri = 'gs://${GCS_BUCKET}/silver/orders'
);


-- ------------------------------------------------------------------------------
-- 4. Atomic ACID `MERGE INTO` from Bronze Iceberg Log -> Silver Iceberg Table
-- Applies INSERTs, UPDATEs, and physical DELETEs onto the Silver Iceberg table.
-- ------------------------------------------------------------------------------
MERGE `${PROJECT_ID}.lakehouse_silver.orders` AS T
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
  FROM `${PROJECT_ID}.lakehouse_bronze.public_orders`
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

-- Verify the Materialized Silver Apache Iceberg Table
SELECT *
FROM `${PROJECT_ID}.lakehouse_silver.orders`
ORDER BY order_id;


-- ------------------------------------------------------------------------------
-- 5. Export Iceberg Table Metadata Snapshot (for Apache Spark / PyIceberg readers)
-- and Test BigQuery Time Travel
-- ------------------------------------------------------------------------------
EXPORT TABLE METADATA FROM `${PROJECT_ID}.lakehouse_silver.orders`;

-- Time Travel query (inspect Silver table state 5 minutes ago)
-- SELECT *
-- FROM `${PROJECT_ID}.lakehouse_silver.orders`
-- FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 MINUTE);
