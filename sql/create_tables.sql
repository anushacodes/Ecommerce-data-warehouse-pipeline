-- Creates the staging layer in Redshift.
DROP TABLE IF EXISTS staging_orders;

CREATE TABLE staging_orders (
    order_id        VARCHAR(64),
    customer_id     VARCHAR(32),
    product_id      VARCHAR(32),
    order_timestamp VARCHAR(32),
    price           DECIMAL(10, 2),
    quantity        INTEGER,
    payment_method  VARCHAR(32),
    country         VARCHAR(8)
);

DROP TABLE IF EXISTS pipeline_runs;

CREATE TABLE pipeline_runs (
    run_id          VARCHAR(64)   PRIMARY KEY,
    dag_name        VARCHAR(128),
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    rows_processed  INTEGER,
    status          VARCHAR(16)
);

-- Creates the star schema for the data warehouse.

DROP TABLE IF EXISTS dim_customer;
CREATE TABLE dim_customer (
    customer_id     VARCHAR(32)   PRIMARY KEY,
    country         VARCHAR(8),
    first_seen_date DATE
);

DROP TABLE IF EXISTS dim_product;
CREATE TABLE dim_product (
    product_id      VARCHAR(32)   PRIMARY KEY,
    avg_price       DECIMAL(10, 2)
);

DROP TABLE IF EXISTS dim_date;
CREATE TABLE dim_date (
    date_key        DATE          PRIMARY KEY,
    day_of_week     INTEGER,
    month           INTEGER,
    quarter         INTEGER,
    year            INTEGER,
    is_weekend      BOOLEAN
);

DROP TABLE IF EXISTS fact_orders;
CREATE TABLE fact_orders (
    order_id        VARCHAR(64)    PRIMARY KEY,
    customer_id     VARCHAR(32)    REFERENCES dim_customer(customer_id),
    product_id      VARCHAR(32)    REFERENCES dim_product(product_id),
    order_date      DATE           REFERENCES dim_date(date_key),
    quantity        INTEGER,
    price           DECIMAL(10, 2),
    total_amount    DECIMAL(12, 2),
    payment_method  VARCHAR(32)
);

DROP TABLE IF EXISTS daily_sales_metrics;
CREATE TABLE daily_sales_metrics (
    metric_date     DATE           PRIMARY KEY,
    total_orders    INTEGER,
    revenue         DECIMAL(14, 2),
    avg_order_value DECIMAL(10, 2)
);
