"""COPY dari S3 bronze (via download lokal) -> Postgres schema raw. Idempotent: DELETE partition tanggal lalu INSERT."""
import os, sys
from datetime import date
from pathlib import Path
from io import BytesIO

import boto3
import psycopg2

BUCKET = os.environ["S3_BUCKET"]
DS = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
PG = dict(host=os.getenv("PG_HOST", "postgres"), dbname=os.getenv("POSTGRES_DB", "olist"),
          user=os.getenv("POSTGRES_USER", "de"), password=os.getenv("POSTGRES_PASSWORD", ""))

# mapping file -> (tabel raw, kolom)
TABLES = {
    "olist_orders_dataset.csv": ("raw_orders", "order_id,customer_id,order_status,order_purchase_timestamp"),
    "olist_order_payments_dataset.csv": ("raw_payments", "order_id,payment_sequential,payment_type,payment_installments,payment_value"),
    "olist_customers_dataset.csv": ("raw_customers", "customer_id,customer_unique_id,customer_city,customer_state"),
}

s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "ap-southeast-1"))
conn = psycopg2.connect(**PG)
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")

for fname, (table, cols) in TABLES.items():
    key = f"bronze/{DS}/{fname}"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    except s3.exceptions.NoSuchKey:
        print(f"skip {key}: tidak ada di S3"); continue
    cur.execute(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(c + ' TEXT' for c in cols.split(','))}, ds DATE);")
    cur.execute(f"DELETE FROM {table} WHERE ds = %s;", (DS,))
    cur.copy_expert(f"COPY {table} ({cols}, ds) FROM STDIN WITH CSV HEADER", BytesIO(obj.replace(b"\r", b"")))
    # ds tidak ada di CSV -> isi manual
    cur.execute(f"UPDATE {table} SET ds = %s WHERE ds IS NULL;", (DS,))
    print(f"loaded {table} ds={DS} rows={cur.rowcount}")
