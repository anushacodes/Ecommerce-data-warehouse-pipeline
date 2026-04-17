# Ecommerce data warehouse pipeline

### Project Overview

This repository contains a self-learning batch data engineering project built with AWS, Apache Airflow, and Amazon Redshift.

The pipeline simulates e-commerce order processing from raw data generation to analytics reporting. Data is generated as parquet files, stored in S3, orchestrated with Airflow, loaded into Redshift staging tables, transformed into a star schema, validated with quality checks, and exposed through a Dash dashboard.



### Features

- Generate synthetic e-commerce orders by date.
- Store data as partitioned parquet files in S3.
- Orchestrate daily pipeline runs with Airflow.
- Support both incremental loading and historical backfill.
- Transform staging data into dimension and fact tables.
- Execute data quality checks after transformation.
- Build daily analytics metrics for reporting.
- Visualize KPI trends in a Dash dashboard.



### Architecture

<img width="3569" height="1069" alt="diagram" src="https://github.com/user-attachments/assets/7b31e3f4-f326-4fb0-94e2-078b04ed026c" />

The orchestration DAG is located at `airflow/dags/ecommerce_pipeline_dag.py` and runs daily at `06:00 UTC`.


### Steps

1. Generate source data using `data_generator/generate_orders.py`.
2. Upload parquet files to `orders/date=YYYY-MM-DD/orders.parquet` in S3.
3. Load S3 partitions into `staging_orders` using Redshift `COPY`.
4. Transform staging data into `dim_*` tables and `fact_orders`.
5. Run data quality checks on transformed warehouse data.
6. Refresh `daily_sales_metrics` from `fact_orders`.
7. Log run metadata into `pipeline_runs`.

#### Loading modes

1. Incremental load  
Loads only partitions newer than `MAX(order_date)` already present in `staging_orders`.

2. Backfill load  
Triggered via Airflow DAG config (`start_date`, `end_date`) and reloads all partitions in the specified range.



### Data Model

#### Warehouse tables

- `dim_customer`
- `dim_product`
- `dim_date`
- `fact_orders`
- `daily_sales_metrics`

#### Operational tables

- `staging_orders`
- `pipeline_runs`

#### DDL scripts

1. `sql/create_tables/staging_tables.sql`
2. `sql/create_tables/warehouse_tables.sql`



### Project Structure

```text
ecommerce-data-warehouse-pipeline/
├── airflow/dags/ecommerce_pipeline_dag.py
├── config/settings.py
├── data_generator/generate_orders.py
├── scripts/
│   ├── redshift_loader.py
│   ├── data_quality_checks.py
│   ├── metadata_logger.py
│   └── upload_to_s3.py
├── sql/
│   ├── create_tables/
│   │   ├── staging_tables.sql
│   │   └── warehouse_tables.sql
│   ├── transformations/transform_orders.sql
│   └── analytics/
│       ├── daily_sales_metrics.sql
│       ├── customer_metrics.sql
│       └── top_products.sql
├── dashboard/app.py
└── docs/
    ├── data_dictionary.md
    └── schema_overview.md
```



### Requirements

- Python 3.10+
- AWS account with S3 access
- Amazon Redshift cluster
- IAM role for Redshift `COPY` access to S3
- Apache Airflow 2.9+



### Setup Instructions

#### 1. Clone and install dependencies

```bash
git clone https://github.com/yourname/ecommerce-data-warehouse-pipeline.git
cd ecommerce-data-warehouse-pipeline

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

#### 2. Configure environment variables

```bash
cp .env.example .env
```

Update `.env` with AWS and Redshift credentials.

#### 3. Create Redshift tables

Execute the following scripts in Redshift:

1. `sql/create_tables/staging_tables.sql`
2. `sql/create_tables/warehouse_tables.sql`

#### 4. Configure Airflow

```bash
export AIRFLOW_HOME=~/airflow
export PYTHONPATH=$(pwd)

airflow db init
airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com

cp airflow/dags/ecommerce_pipeline_dag.py ~/airflow/dags/
```

#### 5. Start Airflow services

```bash
# terminal 1
airflow scheduler

# terminal 2
airflow webserver --port 8080
```


### Running the Pipeline

#### Generate one sample batch

```bash
python data_generator/generate_orders.py
```

#### Trigger full DAG

```bash
airflow dags trigger ecommerce_pipeline
```

#### Backfill historical date range

```bash
airflow dags trigger ecommerce_pipeline \
  --conf '{"start_date":"2026-01-01","end_date":"2026-01-05"}'
```


### Dashboard

```bash
python dashboard/app.py
```

Open `http://localhost:8050`.

Dashboard outputs include:

- Total revenue
- Total orders
- Average order value
- Revenue trend over time
- Orders per day
- Top products by revenue



### Example SQL Queries

```sql
SELECT metric_date, revenue
FROM daily_sales_metrics
ORDER BY metric_date;
```

```sql
SELECT product_id, SUM(total_amount) AS revenue
FROM fact_orders
GROUP BY product_id
ORDER BY revenue DESC
LIMIT 10;
```

```sql
SELECT customer_id, COUNT(*) AS orders
FROM fact_orders
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY orders DESC;
```

