-- ==============================================================================
-- Step 2: Initialize Cloud SQL (PostgreSQL) Table, Publication & Replication Slot
-- Database: ecommerce
-- ==============================================================================

-- 1. Create the operational `orders` table
CREATE TABLE IF NOT EXISTS public.orders (
    order_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    product_category VARCHAR(50) NOT NULL,
    order_status VARCHAR(30) NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL,
    shipping_city VARCHAR(50) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 2. Configure REPLICA IDENTITY so UPDATE and DELETE capture primary keys
ALTER TABLE public.orders REPLICA IDENTITY DEFAULT;

-- 3. Insert initial seed data (to demonstrate Datastream initial backfill into Iceberg)
INSERT INTO public.orders (order_id, customer_id, product_category, order_status, total_amount, shipping_city, updated_at)
VALUES
    ('ORD-1001', 'CUST-01', 'Electronics', 'PENDING',    450.00, 'Jakarta',   NOW() - INTERVAL '2 hours'),
    ('ORD-1002', 'CUST-02', 'Home Office', 'PROCESSING', 180.50, 'Surabaya',  NOW() - INTERVAL '90 minutes'),
    ('ORD-1003', 'CUST-03', 'Books',       'PENDING',     35.00, 'Bandung',   NOW() - INTERVAL '60 minutes'),
    ('ORD-1004', 'CUST-04', 'Gaming',      'SHIPPED',    890.00, 'Singapore', NOW() - INTERVAL '30 minutes'),
    ('ORD-1005', 'CUST-05', 'Audio',       'DELIVERED',  220.00, 'Jakarta',   NOW() - INTERVAL '15 minutes')
ON CONFLICT (order_id) DO NOTHING;

-- 4. Grant replication privileges to the postgres user
ALTER USER postgres WITH REPLICATION;

-- 5. Create Publication for Datastream
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'datastream_iceberg_pub') THEN
        CREATE PUBLICATION datastream_iceberg_pub FOR TABLE public.orders;
    END IF;
END
$$;

-- 6. Create Logical Replication Slot using `pgoutput` plugin
SELECT pg_create_logical_replication_slot('datastream_iceberg_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'datastream_iceberg_slot'
);

-- 7. Verify Publication, Replication Slot, and Initial Rows
SELECT pubname, puballtables FROM pg_publication WHERE pubname = 'datastream_iceberg_pub';
SELECT slot_name, plugin, slot_type, active FROM pg_replication_slots WHERE slot_name = 'datastream_iceberg_slot';
SELECT * FROM public.orders ORDER BY order_id;
