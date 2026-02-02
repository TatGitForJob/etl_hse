from datetime import datetime
import json

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

SOURCE_FILE = "/opt/airflow/data/pets-data.json"
TARGET_TABLE = "json"


def process_json_file():
    with open(SOURCE_FILE, "r", encoding="utf-8") as file_handle:
        raw_data = json.load(file_handle)

    records = raw_data["pets"]
    dataframe = pd.json_normalize(records)

    if "favFoods" in dataframe.columns:
        dataframe["favFoods"] = dataframe["favFoods"].apply(
            lambda items: ", ".join(items) if isinstance(items, list) else None
        )

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    db_engine = pg_hook.get_sqlalchemy_engine()
    dataframe.to_sql(TARGET_TABLE, db_engine, schema="public", if_exists="replace", index=False)


with DAG(
    dag_id="json_to_row",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["elt", "json", "postgres"],
) as pipeline:
    PythonOperator(
        task_id="json_load",
        python_callable=process_json_file,
    )
