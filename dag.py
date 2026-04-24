import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import sys

sys.path.insert(0, os.path.dirname(__file__))

from generate_orders import generate_orders, save_and_upload
from scripts.pipeline_tasks import (
    run_incremental_load,
    run_backfill,
    run_all_checks,
    log_pipeline_run,
)
import utils

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def get_date_range(**context) -> tuple:
    conf = context["dag_run"].conf or {}
    today = datetime.now().strftime("%Y-%m-%d")
    start = conf.get("start_date", today)
    end = conf.get("end_date", today)
    return start, end


def task_generate_and_upload(**context):
    start_date, end_date = get_date_range(**context)
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    current = start
    total_rows = 0
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        df = generate_orders(date_str)
        save_and_upload(df, date_str)
        total_rows += len(df)
        current += timedelta(days=1)

    context["ti"].xcom_push(key="total_rows", value=total_rows)
    print(f"✓ Generated and uploaded {total_rows} total rows.")


def task_load_staging(**context):
    conf = context["dag_run"].conf or {}
    if "start_date" in conf and "end_date" in conf:
        print(f"BACKFILL mode: {conf['start_date']} → {conf['end_date']}")
        run_backfill(conf["start_date"], conf["end_date"])
    else:
        print(f"INCREMENTAL mode: loading new partitions")
        run_incremental_load()


def task_transform_to_warehouse(**context):
    sql_path = os.path.join(os.path.dirname(__file__), "sql/transform_orders.sql")
    utils.run_sql_file(sql_path)
    print("✓ Transformation complete.")


def task_data_quality_checks(**context):
    run_all_checks()


def task_compute_analytics(**context):
    sql_path = os.path.join(os.path.dirname(__file__), "sql/analytics.sql")
    utils.run_sql_file(sql_path)
    print("✓ Analytics metrics refreshed.")


def task_log_metadata(**context):
    ti = context["ti"]
    total_rows = ti.xcom_pull(task_ids="generate_and_upload", key="total_rows") or 0

    dag_run = context["dag_run"]
    start_time = dag_run.start_date or datetime.utcnow()
    end_time = datetime.utcnow()

    log_pipeline_run(
        start_time=start_time,
        end_time=end_time,
        rows_processed=total_rows,
        status="success",
    )


with DAG(
    dag_id="ecommerce_pipeline",
    default_args=default_args,
    description="Batch ecommerce data pipeline: S3 → Redshift → Analytics",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ecommerce", "batch", "warehouse"],
) as dag:
    generate_and_upload = PythonOperator(
        task_id="generate_and_upload",
        python_callable=task_generate_and_upload,
    )

    load_staging = PythonOperator(
        task_id="load_staging_tables",
        python_callable=task_load_staging,
    )

    transform = PythonOperator(
        task_id="transform_to_warehouse",
        python_callable=task_transform_to_warehouse,
    )

    quality_checks = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=task_data_quality_checks,
    )

    analytics = PythonOperator(
        task_id="compute_analytics_metrics",
        python_callable=task_compute_analytics,
    )

    log_metadata = PythonOperator(
        task_id="log_pipeline_metadata",
        python_callable=task_log_metadata,
    )

    (
        generate_and_upload
        >> load_staging
        >> transform
        >> quality_checks
        >> analytics
        >> log_metadata
    )
