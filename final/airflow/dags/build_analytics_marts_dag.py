from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from pendulum import datetime

from _etl_common import pg_connection


USER_ACTIVITY_SQL = """
    TRUNCATE TABLE marts.user_activity_daily;

    INSERT INTO marts.user_activity_daily (
        activity_date,
        user_id,
        sessions_count,
        total_duration_minutes,
        avg_duration_minutes,
        pages_visited_count,
        actions_count,
        devices
    )
    SELECT
        DATE(start_time) AS activity_date,
        user_id,
        COUNT(*) AS sessions_count,
        ROUND(SUM(session_duration_minutes)::numeric, 2) AS total_duration_minutes,
        ROUND(AVG(session_duration_minutes)::numeric, 2) AS avg_duration_minutes,
        COALESCE(SUM(jsonb_array_length(pages_visited)), 0) AS pages_visited_count,
        COALESCE(SUM(jsonb_array_length(actions)), 0) AS actions_count,
        jsonb_build_object(
            'mobile', COUNT(*) FILTER (WHERE LOWER(COALESCE(device ->> 'type', 'unknown')) = 'mobile'),
            'desktop', COUNT(*) FILTER (WHERE LOWER(COALESCE(device ->> 'type', 'unknown')) = 'desktop'),
            'tablet', COUNT(*) FILTER (WHERE LOWER(COALESCE(device ->> 'type', 'unknown')) = 'tablet')
        ) AS devices
    FROM raw.user_sessions
    GROUP BY 1, 2;
"""


PAGE_ACTION_POPULARITY_SQL = """
    TRUNCATE TABLE marts.page_action_popularity_daily;

    WITH page_stats AS (
        SELECT
            DATE(us.start_time) AS activity_date,
            'page' AS item_type,
            page.value AS item_value,
            COUNT(*) AS hits,
            COUNT(DISTINCT us.user_id) AS unique_users
        FROM raw.user_sessions AS us
        CROSS JOIN LATERAL jsonb_array_elements_text(us.pages_visited) AS page(value)
        GROUP BY 1, 2, 3
    ),
    action_stats AS (
        SELECT
            DATE(us.start_time) AS activity_date,
            'action' AS item_type,
            action.value AS item_value,
            COUNT(*) AS hits,
            COUNT(DISTINCT us.user_id) AS unique_users
        FROM raw.user_sessions AS us
        CROSS JOIN LATERAL jsonb_array_elements_text(us.actions) AS action(value)
        GROUP BY 1, 2, 3
    )
    INSERT INTO marts.page_action_popularity_daily (
        activity_date,
        item_type,
        item_value,
        hits,
        unique_users
    )
    SELECT activity_date, item_type, item_value, hits, unique_users
    FROM page_stats
    UNION ALL
    SELECT activity_date, item_type, item_value, hits, unique_users
    FROM action_stats;
"""


def run_sql(statement: str):
    with pg_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(statement)
        connection.commit()


with DAG(
    dag_id="build_analytics_marts",
    start_date=datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "etl", "retries": 1, "retry_delay": timedelta(minutes=1)},
    description="Build analytics marts in PostgreSQL from replicated raw data",
) as dag:
    build_user_activity = PythonOperator(
        task_id="build_user_activity_daily",
        python_callable=run_sql,
        op_kwargs={"statement": USER_ACTIVITY_SQL},
    )

    build_page_action_popularity = PythonOperator(
        task_id="build_page_action_popularity_daily",
        python_callable=run_sql,
        op_kwargs={"statement": PAGE_ACTION_POPULARITY_SQL},
    )

    [
        build_user_activity,
        build_page_action_popularity,
    ]
