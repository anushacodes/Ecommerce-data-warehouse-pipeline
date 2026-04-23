import uuid
import random
import os
import tempfile
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker
import sys

sys.path.insert(0, os.path.dirname(__file__))
import utils

fake = Faker()

PAYMENT_METHODS = ["credit_card", "paypal", "debit_card", "stripe", "apple_pay"]
COUNTRIES = ["US", "UK", "CA", "DE", "FR", "AU", "IN", "BR", "SG", "MX"]
NUM_PRODUCTS = 500
NUM_CUSTOMERS = 2000


def generate_one_order(order_date: str):
    # random hour/minute/second within the given date
    random_seconds = random.randint(0, 86399)
    ts = datetime.strptime(order_date, "%Y-%m-%d") + timedelta(seconds=random_seconds)

    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"cust_{random.randint(1, NUM_CUSTOMERS):05d}",
        "product_id": f"prod_{random.randint(1, NUM_PRODUCTS):04d}",
        "order_timestamp": ts.isoformat(),
        "price": round(random.uniform(5.0, 500.0), 2),
        "quantity": random.randint(1, 10),
        "payment_method": random.choice(PAYMENT_METHODS),
        "country": random.choice(COUNTRIES),
    }


def generate_orders(order_date: str, n: int = 1000):
    rows = [generate_one_order(order_date) for _ in range(n)]
    df = pd.DataFrame(rows)
    print(f"Generated {len(df)} orders for {order_date}")
    return df


def save_and_upload(df: pd.DataFrame, order_date: str):
    # write to a temporary local file first
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        local_path = tmp.name

    df.to_parquet(local_path, index=False)

    # build the s3 key using the date partition
    s3_key = f"orders/date={order_date}/orders.parquet"
    utils.upload_file(local_path, s3_key)

    # clean up temp file
    os.remove(local_path)


if __name__ == "__main__":
    # default: generate orders for today's date
    today = datetime.now().strftime("%Y-%m-%d")

    df = generate_orders(today)
    save_and_upload(df, today)

    print(f" Orders for {today} uploaded to S3 successfully.")
