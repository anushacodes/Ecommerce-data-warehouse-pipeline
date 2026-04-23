-- 1. check_duplicate_orders.sql
SELECT
    order_id,
    COUNT(*) AS occurrences
FROM fact_orders
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;

-- 2. check_null_orders.sql
SELECT COUNT(*) AS null_order_id_count
FROM fact_orders
WHERE order_id IS NULL;
