-- Transforms raw data from staging_orders into
-- the warehouse star schema tables.

-- Run after every successful staging load.

-- Only insert customers we haven't seen before
-- (based on first appearance in staging_orders).
INSERT INTO dim_customer (customer_id, country, first_seen_date)
SELECT DISTINCT
    s.customer_id,
    s.country,
    CAST(s.order_timestamp AS DATE) AS first_seen_date
FROM staging_orders s
WHERE NOT EXISTS (
    SELECT 1 FROM dim_customer d
    WHERE d.customer_id = s.customer_id
);

-- Only insert products we haven't seen before.
-- avg_price is calculated from staging data.
INSERT INTO dim_product (product_id, avg_price)
SELECT
    s.product_id,
    AVG(s.price) AS avg_price
FROM staging_orders s
WHERE NOT EXISTS (
    SELECT 1 FROM dim_product p
    WHERE p.product_id = s.product_id
)
GROUP BY s.product_id;

-- Insert date records for any new dates seen in staging.
-- EXTRACT pulls out day_of_week, month, quarter, year.
INSERT INTO dim_date (date_key, day_of_week, month, quarter, year, is_weekend)
SELECT DISTINCT
    CAST(order_timestamp AS DATE)                            AS date_key,
    EXTRACT(DOW  FROM CAST(order_timestamp AS DATE))         AS day_of_week,
    EXTRACT(MONTH FROM CAST(order_timestamp AS DATE))        AS month,
    EXTRACT(QUARTER FROM CAST(order_timestamp AS DATE))      AS quarter,
    EXTRACT(YEAR FROM CAST(order_timestamp AS DATE))         AS year,
    CASE
        WHEN EXTRACT(DOW FROM CAST(order_timestamp AS DATE)) IN (0, 6)
        THEN TRUE
        ELSE FALSE
    END                                                      AS is_weekend
FROM staging_orders
WHERE CAST(order_timestamp AS DATE) NOT IN (
    SELECT date_key FROM dim_date
);

--   total_amount = price * quantity
-- Only insert orders not already in fact_orders
INSERT INTO fact_orders
    (order_id, customer_id, product_id, order_date,
     quantity, price, total_amount, payment_method)
SELECT
    s.order_id,
    s.customer_id,
    s.product_id,
    CAST(s.order_timestamp AS DATE)        AS order_date,
    s.quantity,
    s.price,
    s.price * s.quantity                   AS total_amount,
    s.payment_method
FROM staging_orders s
WHERE NOT EXISTS (
    SELECT 1 FROM fact_orders f
    WHERE f.order_id = s.order_id
);
