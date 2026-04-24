# Data Dictionary

This document describes every field in the pipeline's tables.

---

## staging_orders

Raw orders loaded directly from S3 Parquet files.

| Column | Type | Description |
|---|---|---|
| `order_id` | VARCHAR(64) | Unique identifier for the order (UUID) |
| `customer_id` | VARCHAR(32) | Customer identifier (e.g. `cust_00042`) |
| `product_id` | VARCHAR(32) | Product identifier (e.g. `prod_0123`) |
| `order_timestamp` | VARCHAR(32) | ISO timestamp string e.g. `2026-03-01T14:23:11` |
| `price` | DECIMAL(10,2) | Unit price of the product |
| `quantity` | INTEGER | Number of units ordered |
| `payment_method` | VARCHAR(32) | Payment type: `credit_card`, `paypal`, etc. |
| `country` | VARCHAR(8) | 2-letter country code e.g. `US`, `UK` |

---

## fact_orders

Cleaned and enriched order events in the warehouse.

| Column | Type | Description |
|---|---|---|
| `order_id` | VARCHAR(64) | Primary key (unique per order) |
| `customer_id` | VARCHAR(32) | FK → `dim_customer.customer_id` |
| `product_id` | VARCHAR(32) | FK → `dim_product.product_id` |
| `order_date` | DATE | FK → `dim_date.date_key` |
| `quantity` | INTEGER | Units ordered |
| `price` | DECIMAL(10,2) | Unit price |
| `total_amount` | DECIMAL(12,2) | `price × quantity` (computed during transform) |
| `payment_method` | VARCHAR(32) | Payment method used |

---

## dim_customer

One row per unique customer.

| Column | Type | Description |
|---|---|---|
| `customer_id` | VARCHAR(32) | Primary key |
| `country` | VARCHAR(8) | Country of customer's first order |
| `first_seen_date` | DATE | Date of customer's first order |

---

## dim_product

One row per unique product.

| Column | Type | Description |
|---|---|---|
| `product_id` | VARCHAR(32) | Primary key |
| `avg_price` | DECIMAL(10,2) | Average price seen across all orders |

---

## dim_date

Calendar dimension for easy time-based filtering.

| Column | Type | Description |
|---|---|---|
| `date_key` | DATE | Primary key (the calendar date) |
| `day_of_week` | INTEGER | 0=Monday … 6=Sunday |
| `month` | INTEGER | 1–12 |
| `quarter` | INTEGER | 1–4 |
| `year` | INTEGER | e.g. 2026 |
| `is_weekend` | BOOLEAN | True if Saturday or Sunday |

---

## daily_sales_metrics

Pre-aggregated analytics table for fast dashboard queries.

| Column | Type | Description |
|---|---|---|
| `metric_date` | DATE | The calendar date |
| `total_orders` | INTEGER | Number of orders on that date |
| `revenue` | DECIMAL(14,2) | Sum of `total_amount` |
| `avg_order_value` | DECIMAL(10,2) | Average `total_amount` |

---

## pipeline_runs

Audit log of every pipeline execution.

| Column | Type | Description |
|---|---|---|
| `run_id` | VARCHAR(64) | UUID for this run |
| `dag_name` | VARCHAR(128) | Airflow DAG ID |
| `start_time` | TIMESTAMP | When the DAG run started |
| `end_time` | TIMESTAMP | When the DAG run finished |
| `rows_processed` | INTEGER | Orders generated/loaded in this run |
| `status` | VARCHAR(16) | `"success"` or `"failed"` |
