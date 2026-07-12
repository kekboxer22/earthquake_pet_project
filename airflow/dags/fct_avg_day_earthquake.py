"""
DAG: Витрина средней магнитуды землетрясений по дням
"""

import pendulum
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = "mykyta"
DAG_ID = "fct_avg_day_earthquake"
SCHEMA = "dm"
TARGET_TABLE = "fct_avg_day_earthquake"
PG_CONNECT = "postgres_dwh"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 7, 1, tz="UTC"),
    "catchup": False,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 6 * * *",
    default_args=args,
    tags=["dm", "pg"],
    concurrency=1,
    max_active_runs=1,
) as dag:
    start = DummyOperator(task_id="start")

    sensor = ExternalTaskSensor(
        task_id="wait_for_raw",
        external_dag_id="raw_from_api_to_pg",
        allowed_states=["success"],
        mode="reschedule",
        timeout=36000,
        poke_interval=60,
    )

    drop_stg = SQLExecuteQueryOperator(
        task_id="drop_stg_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    create_stg = SQLExecuteQueryOperator(
        task_id="create_stg_table",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        CREATE TABLE stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}" AS
        SELECT
            time::date AS date,
            AVG(mag::float) AS avg_mag
        FROM ods.fct_earthquake
        WHERE time::date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
        GROUP BY 1
        """,
    )

    delete_target = SQLExecuteQueryOperator(
        task_id="delete_from_target",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DELETE FROM {SCHEMA}.{TARGET_TABLE}
        WHERE date = '{{{{ data_interval_start.format('YYYY-MM-DD') }}}}'
        """,
    )

    insert_target = SQLExecuteQueryOperator(
        task_id="insert_into_target",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        INSERT INTO {SCHEMA}.{TARGET_TABLE}
        SELECT * FROM stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    drop_stg_after = SQLExecuteQueryOperator(
        task_id="drop_stg_after",
        conn_id=PG_CONNECT,
        autocommit=True,
        sql=f"""
        DROP TABLE IF EXISTS stg."tmp_{TARGET_TABLE}_{{{{ data_interval_start.format('YYYY-MM-DD') }}}}"
        """,
    )

    end = DummyOperator(task_id="end")

    (
        start
        >> sensor
        >> drop_stg
        >> create_stg
        >> delete_target
        >> insert_target
        >> drop_stg_after
        >> end
    )
