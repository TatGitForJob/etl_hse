from datetime import datetime

from airflow.decorators import dag, task

from scripts.minio_s3 import get_text, put_text
from scripts.process_orders import aggregate_orders, csv_to_rows, rows_to_csv


S3_ENDPOINT_URL = "http://minio:9000"
S3_BUCKET = "etl-bucket"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"


@dag(
    dag_id="process_s3_orders",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["local-etl", "s3", "orders"],
)
def process_s3_orders():
    @task
    def build_status_mart():
        csv_text = get_text(
            S3_ENDPOINT_URL, S3_BUCKET, "raw/orders.csv", S3_ACCESS_KEY, S3_SECRET_KEY
        )
        result_rows = aggregate_orders(csv_to_rows(csv_text))
        result_csv = rows_to_csv(result_rows)
        put_text(
            S3_ENDPOINT_URL,
            S3_BUCKET,
            "mart/orders_by_status.csv",
            S3_ACCESS_KEY,
            S3_SECRET_KEY,
            result_csv,
        )
        return f"Created s3://{S3_BUCKET}/mart/orders_by_status.csv"

    build_status_mart()


process_s3_orders()
