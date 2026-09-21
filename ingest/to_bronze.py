"""Upload CSV lokal/data -> S3 bronze/YYYY-MM-DD/ (idempotent: overwrite key sama)."""
import os, sys
from datetime import date
from pathlib import Path

import boto3

BUCKET = os.environ["S3_BUCKET"]
DS = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
SRC = Path("/opt/airflow/data" if Path("/opt/airflow/data").exists() else "data")

s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "ap-southeast-1"))

files = list(SRC.glob("*.csv"))
if not files:
    print(f"tidak ada CSV di {SRC}. Download Olist: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce -> taruh CSV di data/")
    sys.exit(1)

for f in files:
    key = f"bronze/{DS}/{f.name}"
    s3.upload_file(str(f), BUCKET, key)  # overwrite = idempotent
    print(f"uploaded s3://{BUCKET}/{key}")
