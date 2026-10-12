# Real-Time CDC to the Open Lakehouse: Streaming Cloud SQL (PostgreSQL) Changes into Apache Iceberg on GCP

End-to-end hands-on code repository for streaming real-time Change Data Capture (CDC) events from **Cloud SQL for PostgreSQL** into **BigQuery Managed Tables for Apache Iceberg** stored on **Google Cloud Storage (GCS)** using **Google Cloud Datastream**.

![Architecture Diagram](./cdc_postgresql_iceberg_architecture.jpg)

---

## Architecture Highlights

1. **Operational Source (OLTP):** Cloud SQL for PostgreSQL 15 configured with `cloudsql.logical_decoding=on` and `pgoutput` logical replication slot.
2. **Serverless CDC Ingestion:** Google Cloud Datastream captures initial backfill + real-time `INSERT`, `UPDATE`, and `DELETE` events from PostgreSQL WAL.
3. **Bronze Open Lakehouse Layer (`lakehouse_bronze.public_orders`):** Datastream writes directly to a **BigLake Managed Table for Apache Iceberg** (`fileFormat: PARQUET`, `tableFormat: ICEBERG`) in `APPEND_ONLY` mode inside your own GCS bucket.
4. **Silver Current-State Layer (`lakehouse_silver`):**
   - **Pattern A (Zero-Latency Virtual View):** `lakehouse_silver.v_orders_current` deduplicates CDC events on the fly using `QUALIFY ROW_NUMBER() OVER (...) = 1` and filters out deleted rows (`is_deleted = FALSE`).
   - **Pattern B (Materialized Silver Apache Iceberg Table):** `lakehouse_silver.orders` applies atomic ACID `MERGE INTO` (Upsert + Delete) into a second governed Apache Iceberg table on GCS, complete with `EXPORT TABLE METADATA` for multi-engine access (Apache Spark, PyIceberg, Trino) and Time Travel (`FOR SYSTEM_TIME AS OF`).

---

## Repository Structure

```text
.
├── env.sh                                         # Central environment variables (Project, Region, Names)
├── 01_setup_gcp_foundation.sh                     # Enables APIs, creates GCS bucket, BQ Connection, Datasets & Cloud SQL
├── sql/
│   ├── 02_postgres_init_cdc.sql                   # Creates orders table, publication, replication slot & seed data
│   ├── 04_postgres_simulate_cdc.sql               # Simulates real-time INSERT, UPDATE, and DELETE transactions
│   └── 05_bigquery_silver_iceberg_and_queries.sql # BigQuery SQL for Bronze inspection, Deduplicated View & Silver Iceberg MERGE
├── 03_setup_datastream_iceberg.sh                 # Creates Datastream profiles, BLMT Iceberg configs & starts stream
├── 06_verify_gcs_and_bigquery.sh                  # Runs verification queries & inspects Parquet/Iceberg files in GCS
├── 99_cleanup.sh                                  # Tears down all resources to prevent ongoing billing
└── medium_cdc_iceberg_outline.md                  # Full Medium article outline & Mermaid diagrams
```

---

## Step-by-Step Execution Guide

### Step 0: Configure Environment Variables
Edit or source `env.sh` with your GCP Project ID:

```bash
export PROJECT_ID="your-gcp-project-id"
export REGION="us-central1"
export PG_PASSWORD="YourStrongPassword2026!"
source ./env.sh
```

### Step 1: Provision GCP Foundation (GCS, BigQuery Connection, Cloud SQL)
```bash
chmod +x *.sh
./01_setup_gcp_foundation.sh
```

### Step 2: Initialize PostgreSQL Table, Publication & Logical Replication Slot
Connect to Cloud SQL and execute `sql/02_postgres_init_cdc.sql`:
```bash
PGPASSWORD="${PG_PASSWORD}" gcloud sql connect "${CLOUDSQL_INSTANCE}" \
  --user="${PG_USER}" \
  --database="${PG_DATABASE}" \
  --project="${PROJECT_ID}" < sql/02_postgres_init_cdc.sql
```

### Step 3: Create and Start the Datastream-to-Iceberg Stream
```bash
./03_setup_datastream_iceberg.sh
```
Wait ~2–3 minutes for the initial backfill of the 5 seed rows to land in BigQuery (`lakehouse_bronze.public_orders`). You can check the stream status with:
```bash
gcloud datastream streams describe "${DS_STREAM_ID}" \
  --location="${REGION}" \
  --project="${PROJECT_ID}" \
  --format="value(state)"
```

### Step 4: Simulate Live OLTP Changes (`INSERT`, `UPDATE`, `DELETE`)
Once the initial backfill is visible in BigQuery, simulate real-time transactions in PostgreSQL:
```bash
PGPASSWORD="${PG_PASSWORD}" gcloud sql connect "${CLOUDSQL_INSTANCE}" \
  --user="${PG_USER}" \
  --database="${PG_DATABASE}" \
  --project="${PROJECT_ID}" < sql/04_postgres_simulate_cdc.sql
```

### Step 5: Verify Bronze CDC Log, Silver Iceberg `MERGE`, and GCS Files
Wait ~1–2 minutes for Datastream to flush the CDC events to BigQuery Iceberg, then run:
```bash
./06_verify_gcs_and_bigquery.sh
```
This script will:
1. Query the Bronze Iceberg table (`lakehouse_bronze.public_orders`) showing all `INSERT`, `UPDATE-INSERT`, and `DELETE` events in `datastream_metadata`.
2. Create and query the real-time deduplicated view (`lakehouse_silver.v_orders_current`).
3. Create the Materialized Silver Apache Iceberg table (`lakehouse_silver.orders`), run the atomic `MERGE INTO` statement, and export the Iceberg snapshot metadata.
4. List the physical `.parquet` and `metadata/` files stored in `gs://${GCS_BUCKET}/`.

### Step 6: Clean Up Resources
When you have finished taking screenshots and running benchmarks for your article, tear down all resources:
```bash
./99_cleanup.sh
```
