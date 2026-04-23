-- Computes daily sales summary metrics.
-- Results are stored in the daily_sales_metrics table and used by the dashboard.
TRUNCATE TABLE daily_sales_metrics;

INSERT INTO daily_sales_metrics (metric_date, total_orders, revenue, avg_order_value)
SELECT
    order_date                    AS metric_date,
    COUNT(order_id)               AS total_orders,
    SUM(total_amount)             AS revenue,
    AVG(total_amount)             AS avg_order_value
FROM fact_orders
GROUP BY order_date
ORDER BY order_date;

-- Customer-level analytics:
SELECT
    customer_id,
    COUNT(order_id)       AS total_orders,
    SUM(total_amount)     AS lifetime_value,
    AVG(total_amount)     AS avg_order_value,
    MIN(order_date)       AS first_order_date,
    MAX(order_date)       AS last_order_date,
    CASE
        WHEN COUNT(order_id) > 1 THEN 'repeat'
        ELSE 'one-time'
    END                   AS customer_type
FROM fact_orders
GROUP BY customer_id
ORDER BY lifetime_value DESC;

-- Returns the top 20 products by total revenue.
SELECT
    product_id,
    COUNT(order_id)       AS total_orders,
    SUM(total_amount)     AS total_revenue,
    AVG(total_amount)     AS avg_order_value,
    SUM(quantity)         AS units_sold
FROM fact_orders
GROUP BY product_id
ORDER BY total_revenue DESC
LIMIT 20;
