-- ==============================================================================
-- Step 4: Simulate Real-Time OLTP Changes (INSERT, UPDATE, DELETE) in PostgreSQL
-- Run this AFTER the Datastream stream is RUNNING and initial backfill is complete.
-- ==============================================================================

-- 1. INSERT two new incoming orders
INSERT INTO public.orders (order_id, customer_id, product_category, order_status, total_amount, shipping_city, updated_at)
VALUES
    ('ORD-1006', 'CUST-06', 'Smartphones', 'PENDING', 699.00, 'Jakarta',  NOW()),
    ('ORD-1007', 'CUST-07', 'Wearables',   'PAID',    149.90, 'Medan',    NOW());

-- 2. UPDATE existing orders (status progression & price adjustment)
UPDATE public.orders
SET order_status = 'SHIPPED',
    updated_at   = NOW()
WHERE order_id = 'ORD-1001';

UPDATE public.orders
SET order_status = 'DELIVERED',
    total_amount = 195.00,
    updated_at   = NOW()
WHERE order_id = 'ORD-1002';

-- 3. DELETE a cancelled order to test CDC delete propagation
DELETE FROM public.orders
WHERE order_id = 'ORD-1003';

-- 4. Verify current OLTP state in PostgreSQL (should have 6 active orders: 1001, 1002, 1004, 1005, 1006, 1007)
SELECT * FROM public.orders ORDER BY order_id;
