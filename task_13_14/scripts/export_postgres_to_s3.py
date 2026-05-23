import csv
import os
from io import StringIO

from scripts.minio_s3 import put_text


def getenv(name, default):
    return os.environ.get(name, default)


def rows_to_csv(rows):
    if not rows:
        return ""

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


def fetch_orders():
    import psycopg2
    import psycopg2.extras

    conn = psycopg2.connect(
        host=getenv("POSTGRES_HOST", "localhost"),
        port=int(getenv("POSTGRES_PORT", "5432")),
        dbname=getenv("POSTGRES_DB", "source_db"),
        user=getenv("POSTGRES_USER", "etl_user"),
        password=getenv("POSTGRES_PASSWORD", "etl_password"),
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(
                """
                select order_id, customer_name, status, amount, created_at
                from public.orders
                order by order_id
                """
            )
            return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def upload_text_to_s3(key, text):
    put_text(
        getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
        getenv("S3_BUCKET", "etl-bucket"),
        key,
        getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        text,
    )
    bucket = getenv("S3_BUCKET", "etl-bucket")
    client.put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))


def main():
    rows = fetch_orders()
    csv_text = rows_to_csv(rows)
    upload_text_to_s3("raw/orders.csv", csv_text)
    print(f"Exported {len(rows)} rows to s3://{getenv('S3_BUCKET', 'etl-bucket')}/raw/orders.csv")


if __name__ == "__main__":
    main()
