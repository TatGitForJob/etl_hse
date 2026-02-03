from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import logging
import os

log = logging.getLogger(__name__)

DATA_PATH = "/opt/airflow/data/IOT-temp.csv"
OUT_DIR = "/opt/airflow/output"


def transform():
    os.makedirs(OUT_DIR, exist_ok=True)

    df = pd.read_csv(DATA_PATH)
    log.info("Rows total: %s", len(df))
    log.info("Columns: %s", list(df.columns))

    df = df[df["out/in"] == "In"]
    log.info("Rows after out/in=In filter: %s", len(df))

    df["noted_date"] = pd.to_datetime(df["noted_date"], errors="coerce")
    df["temp"] = pd.to_numeric(df["temp"], errors="coerce")
    df = df.dropna(subset=["noted_date", "temp"])
    df["date"] = df["noted_date"].dt.date

    p5 = df["temp"].quantile(0.05)
    p95 = df["temp"].quantile(0.95)
    log.info("Percentiles - p5: %s, p95: %s", p5, p95)

    df_clean = df[(df["temp"] > p5) & (df["temp"] < p95)].copy()
    log.info("Rows after outlier removal: %s", len(df_clean))

    by_day = df_clean.groupby("date", as_index=False).agg(
        avg_temp=("temp", "mean"),
        min_temp=("temp", "min"),
        max_temp=("temp", "max"),
        cnt=("temp", "count"),
    )

    top_hot_days = by_day.nlargest(5, "avg_temp")
    top_cold_days = by_day.nsmallest(5, "avg_temp")

    log.info("=== TOP 5 HOTTEST DAYS ===")
    for _, row in top_hot_days.iterrows():
        log.info("  %s: avg=%.2f, min=%.2f, max=%.2f", row["date"], row["avg_temp"], row["min_temp"], row["max_temp"])

    log.info("=== TOP 5 COLDEST DAYS ===")
    for _, row in top_cold_days.iterrows():
        log.info("  %s: avg=%.2f, min=%.2f, max=%.2f", row["date"], row["avg_temp"], row["min_temp"], row["max_temp"])

    df_clean["noted_date"] = df_clean["noted_date"].apply(lambda x: x.strftime("%Y-%m-%d"))

    df_clean.to_csv(f"{OUT_DIR}/cleaned.csv", index=False)

    log.info("Saved: cleaned.csv")


with DAG(
    dag_id="transform",
    start_date=datetime(2026, 2, 3),
    schedule=None,
    catchup=False,
    tags=["transform", "hw"],
) as dag:
    PythonOperator(task_id="run_transform", python_callable=transform)
