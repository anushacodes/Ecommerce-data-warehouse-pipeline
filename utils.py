import os
import psycopg2

def get_connection():
    
    conn = psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=int(os.getenv("REDSHIFT_PORT", "5439")),
        dbname=os.getenv("REDSHIFT_DB", "ecommerce_db"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD"),
    )
    return conn

def run_query(sql: str, fetch: bool = False):
    
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)

    result = None
    if fetch:
        result = cursor.fetchall()

    conn.commit()
    cursor.close()
    conn.close()

    return result

def run_sql_file(path: str):
    
    with open(path, "r") as f:
        sql = f.read()

    print(f"Running SQL file: {path}")
    run_query(sql)
    print("Done.")
import os
import boto3

def get_s3_client():
    
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

def upload_file(local_path: str, s3_key: str):
    
    client = get_s3_client()
    client.upload_file(local_path, os.getenv('S3_BUCKET', 'ecommerce-data'), s3_key)
    print(f"Uploaded {local_path} → s3://{os.getenv('S3_BUCKET', 'ecommerce-data')}/{s3_key}")

def list_partitions(prefix: str) -> list:
    
    client = get_s3_client()
    response = client.list_objects_v2(
        Bucket=os.getenv('S3_BUCKET', 'ecommerce-data'),
        Prefix=prefix,
        Delimiter="/",   # treat each date= folder as one entry
    )

    prefixes = []
    for obj in response.get("CommonPrefixes", []):
        prefixes.append(obj["Prefix"])

    return sorted(prefixes)  # sort chronologically (string sort works for date= format)

def download_file(s3_key: str, local_path: str):
    
    client = get_s3_client()
    client.download_file(os.getenv('S3_BUCKET', 'ecommerce-data'), s3_key, local_path)
    print(f"Downloaded s3://{os.getenv('S3_BUCKET', 'ecommerce-data')}/{s3_key} → {local_path}")
