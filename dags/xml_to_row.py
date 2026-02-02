from datetime import datetime

import pandas as pd
import xmltodict
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

SOURCE_FILE = "/opt/airflow/data/nutrition.xml"
TARGET_TABLE = "xml"

NUTRIENT_COLS = ["total-fat", "saturated-fat", "cholesterol", "sodium", "carb", "fiber", "protein"]


def process_xml_file():
    with open(SOURCE_FILE, "r", encoding="utf-8") as file_handle:
        parsed_doc = xmltodict.parse(file_handle.read())

    food_items = parsed_doc["nutrition"]["food"]
    records = []

    for item in food_items:
        record = {}
        record["name"] = item.get("name")
        record["manufacturer"] = item.get("mfr")

        serving_info = item.get("serving", {})
        record["serving_value"] = serving_info.get("#text")
        record["serving_units"] = serving_info.get("@units")

        cal_info = item.get("calories", {})
        record["calories_total"] = cal_info.get("@total")
        record["calories_fat"] = cal_info.get("@fat")

        for col in NUTRIENT_COLS:
            record[col.replace("-", "_")] = item.get(col)

        vit_info = item.get("vitamins", {})
        record["vitamin_a"] = vit_info.get("a")
        record["vitamin_c"] = vit_info.get("c")

        min_info = item.get("minerals", {})
        record["calcium"] = min_info.get("ca")
        record["iron"] = min_info.get("fe")

        records.append(record)

    dataframe = pd.DataFrame(records)

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    db_engine = pg_hook.get_sqlalchemy_engine()
    dataframe.to_sql(TARGET_TABLE, db_engine, schema="public", if_exists="replace", index=False)

    print("Записей загружено:", len(dataframe))
    print(dataframe.head())


with DAG(
    dag_id="xml_to_row",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["elt", "xml", "postgres"],
) as pipeline:
    PythonOperator(
        task_id="xml_load",
        python_callable=process_xml_file,
    )
