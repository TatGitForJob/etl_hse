import time
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import datetime

from _etl_common import (
    bulk_upsert,
    json_value,
    mongo_database,
    parse_dt,
    pg_connection,
)


def wait_for_seed_data():
    database = mongo_database()
    deadline = time.time() + 300
    while time.time() < deadline:
        if database["user_sessions"].count_documents({}) > 0:
            return
        time.sleep(10)
    raise TimeoutError("MongoDB seed data was not generated within 5 minutes")


def replicate_user_sessions():
    collection = mongo_database()["user_sessions"]
    documents = collection.find({})
    deduplicated = {}
    for document in documents:
        session_id = str(document.get("session_id", "")).strip()
        user_id = str(document.get("user_id", "")).strip()
        start_time = parse_dt(document.get("start_time"))
        end_time = parse_dt(document.get("end_time"))
        if not session_id or not user_id or not start_time or not end_time or end_time <= start_time:
            continue
        cleaned = (
            session_id,
            user_id,
            start_time,
            end_time,
            round((end_time - start_time).total_seconds() / 60, 2),
            json_value(list(dict.fromkeys(document.get("pages_visited", [])))),
            json_value(document.get("device", {})),
            json_value(list(dict.fromkeys(document.get("actions", [])))),
        )
        current = deduplicated.get(session_id)
        if current is None or cleaned[3] > current[3]:
            deduplicated[session_id] = cleaned

    statement = """
        INSERT INTO raw.user_sessions (
            session_id,
            user_id,
            start_time,
            end_time,
            session_duration_minutes,
            pages_visited,
            device,
            actions
        ) VALUES %s
        ON CONFLICT (session_id, start_time) DO UPDATE SET
            user_id = EXCLUDED.user_id,
            end_time = EXCLUDED.end_time,
            session_duration_minutes = EXCLUDED.session_duration_minutes,
            pages_visited = EXCLUDED.pages_visited,
            device = EXCLUDED.device,
            actions = EXCLUDED.actions,
            loaded_at = NOW()
    """
    with pg_connection() as connection:
        return bulk_upsert(connection, statement, list(deduplicated.values()))


with DAG(
    dag_id="mongo_to_postgres_replication",
    start_date=datetime(2024, 1, 1, tz="UTC"),
    schedule="@once",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "etl", "retries": 2, "retry_delay": timedelta(minutes=1)},
    description="Replicate and transform MongoDB user sessions into PostgreSQL raw schema",
) as dag:
    wait_for_data = PythonOperator(
        task_id="wait_for_seed_data",
        python_callable=wait_for_seed_data,
    )

    load_user_sessions = PythonOperator(
        task_id="load_user_sessions",
        python_callable=replicate_user_sessions,
    )

    trigger_marts = TriggerDagRunOperator(
        task_id="trigger_build_analytics_marts",
        trigger_dag_id="build_analytics_marts",
        wait_for_completion=False,
    )

    wait_for_data >> load_user_sessions >> trigger_marts
