import os
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import Json, execute_values
from pymongo import MongoClient


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Environment variable '{name}' is required")
    return value


def mongo_client() -> MongoClient:
    return MongoClient(env("ETL_MONGO_URI"))


def mongo_database():
    return mongo_client()[env("ETL_MONGO_DB")]


def pg_connection():
    return psycopg2.connect(
        host=env("ETL_PG_HOST"),
        port=env("ETL_PG_PORT"),
        dbname=env("ETL_PG_DB"),
        user=env("ETL_PG_USER"),
        password=env("ETL_PG_PASSWORD"),
    )


def parse_dt(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def json_value(value):
    return Json(value)


def bulk_upsert(connection, statement: str, rows: list[tuple]) -> int:
    if not rows:
        return 0
    with connection.cursor() as cursor:
        execute_values(cursor, statement, rows, page_size=500)
    connection.commit()
    return len(rows)


def normalize_text(value: str | None, fallback: str = "unknown") -> str:
    if value is None:
        return fallback
    cleaned = str(value).strip().lower()
    return cleaned or fallback
