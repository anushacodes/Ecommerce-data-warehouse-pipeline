import os
import uuid
import argparse
from datetime import datetime, timedelta
import utils


def check_no_null_order_ids():
    sql = "SELECT COUNT(*) FROM fact_orders WHERE order_id IS NULL;"
    rows = utils.run_query(sql, fetch=True)
    null_count = rows[0][0]

    if null_count > 0:
        raise Exception(
            f"DATA QUALITY FAILURE: {null_count} NULL order_ids found in fact_orders."
        )

    print(f"✓ No NULL order_ids found.")

def check_no_duplicate_order_ids():
    sql = "SELECT order_id, COUNT(*) FROM fact_orders GROUP BY order_id HAVING COUNT(*) > 1;"
    rows = utils.run_query(sql, fetch=True)

    if rows:
        print(f"WARNING: {len(rows)} duplicate order_id(s) found:")
        for order_id, cnt in rows:
            print(f"  order_id={order_id} appears {cnt} times")
        raise Exception("DATA QUALITY FAILURE: Duplicate order_ids found in fact_orders.")

    print(f"✓ No duplicate order_ids found.")

def check_rows_were_loaded():
    sql = "SELECT COUNT(*) FROM fact_orders WHERE DATE(order_date) = CURRENT_DATE;"
    rows = utils.run_query(sql, fetch=True)
    row_count = rows[0][0]

    if row_count == 0:
        raise Exception(
            "DATA QUALITY FAILURE: No rows loaded into fact_orders for today."
        )

    print(f"✓ {row_count} rows found in fact_orders for today.")

def check_positive_amounts():
    sql = "SELECT COUNT(*) FROM fact_orders WHERE total_amount <= 0;"
    rows = utils.run_query(sql, fetch=True)
    bad_count = rows[0][0]

    if bad_count > 0:
        print(f"WARNING: {bad_count} rows have total_amount <= 0.")
    else:
        print(f"✓ All total_amount values are positive.")

def run_all_checks():
    print("\n── Running Data Quality Checks ──────────────────────")
    check_no_null_order_ids()
    check_no_duplicate_order_ids()
    check_rows_were_loaded()
    check_positive_amounts()
    print("── All checks passed ✓ ──────────────────────────────\n")



def log_pipeline_run(
    start_time: datetime,
    end_time: datetime,
    rows_processed: int,
    status: str,
):
    run_id = str(uuid.uuid4())

    sql = f"""
        INSERT INTO pipeline_runs (run_id, dag_name, run_start_time, run_end_time, rows_processed, status)
        VALUES ('{run_id}', 'ecommerce_pipeline', '{start_time}', '{end_time}', {rows_processed}, '{status}');
    """

    utils.run_query(sql)
    print(f"✓ Pipeline run logged: run_id={run_id}, status={status}")

def get_pipeline_run_history(limit: int = 10) -> list:
    sql = f"""
        SELECT * FROM pipeline_runs LIMIT {limit};
    """
    return utils.run_query(sql, fetch=True)



def get_latest_loaded_date() -> str | None:
    sql = "SELECT MAX(order_date) FROM staging_orders;"
    rows = utils.run_query(sql, fetch=True)
    latest = rows[0][0]  # first row, first column
    return str(latest) if latest else None

def get_available_partitions() -> list:
    prefix = f"orders/"
    all_prefixes = utils.list_partitions(prefix)

    dates = []
    for p in all_prefixes:
        part = p.rstrip("/").split("date=")[-1]
        dates.append(part)

    return sorted(dates)

def load_partition(date_str: str):
    s3_path = (
        f"s3://{os.getenv('S3_BUCKET', 'ecommerce-data')}/orders"
        f"/date={date_str}/orders.parquet"
    )

    copy_sql = f"""
        COPY staging_orders FROM '{s3_path}'
        IAM_ROLE '{os.getenv("REDSHIFT_IAM_ROLE")}'
        FORMAT AS PARQUET;
    """

    print(f"Loading partition date={date_str} from S3...")
    utils.run_query(copy_sql)
    print(f"  ✓ Loaded date={date_str}")

def run_incremental_load():
    latest_loaded = get_latest_loaded_date()
    available    = get_available_partitions()

    print(f"Latest loaded date: {latest_loaded}")
    print(f"Partitions available in S3: {available}")

    if latest_loaded:
        new_partitions = [d for d in available if d > latest_loaded]
    else:
        new_partitions = available

    if not new_partitions:
        print("No new partitions to load. Pipeline is up to date.")
        return

    for date_str in new_partitions:
        load_partition(date_str)

    print(f"✓ Incremental load complete. Loaded {len(new_partitions)} partition(s).")

def run_backfill(start_date: str, end_date: str):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end   = datetime.strptime(end_date,   "%Y-%m-%d")

    date_range = []
    current = start
    while current <= end:
        date_range.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    print(f"Backfilling {len(date_range)} partitions from {start_date} to {end_date}")

    utils.run_query("TRUNCATE TABLE staging_orders;")

    for date_str in date_range:
        load_partition(date_str)

    print(f"✓ Backfill complete for {start_date} → {end_date}")



def upload_orders_for_date(local_path: str, date_str: str):
    s3_key = f"orders/date={date_str}/orders.parquet"
    utils.upload_file(local_path, s3_key)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline Tasks utility")
    parser.add_argument("--task", required=True, choices=["upload", "load", "check", "backfill"], help="Task to run")
    parser.add_argument("--local-path", help="Path to local Parquet file for upload")
    parser.add_argument("--date", help="Partition date YYYY-MM-DD for upload")
    parser.add_argument("--start", help="Start date for backfill")
    parser.add_argument("--end", help="End date for backfill")
    
    args = parser.parse_args()

    if args.task == "upload":
        upload_orders_for_date(args.local_path, args.date)
        print(f"✓ Uploaded {args.local_path} for date={args.date}")
    elif args.task == "load":
        run_incremental_load()
    elif args.task == "check":
        run_all_checks()
    elif args.task == "backfill":
        run_backfill(args.start, args.end)

