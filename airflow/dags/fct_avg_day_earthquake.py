"""
DAG: Daily average earthquake magnitude mart
"""

import pendulum
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta
from telegram_notify import on_failure_callback, on_success_callback, send_custom_message

OWNER = "mykyta"
DAG_ID = "fct_avg_day_earthquake"
SCHEMA = "dm"
TARGET_TABLE = "fct_avg_day_earthquake"
PG_CONNECT = "postgres_dwh"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 7, 12, tz="UTC"),
    "catchup": False,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
    "on_failure_callback": on_failure_callback,
}


def notify_daily_result(**context):
    # Fetch the freshly computed value and send it to Telegram
    date_str = context["data_interval_start"].format("YYYY-MM-DD")
    hook = PostgresHook(postgres_conn_id=PG_CONNECT)
    result = hook.get_first(
        f"SELECT avg_mag FROM {SCHEMA}.{TARGET_TABLE} WHERE date = %s",
        parameters=(date_str,),
    )
    avg_mag = result[0] if result else None

    if avg_mag is not None:
        text = f"📊 <b>Average magnitude on {date_str}</b>: {avg_mag:.2f}"
    else:
        text = f"📊 No data for {date_str} yet"

    send_custom_message(text)


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 6 * * *",
    default_args=args,
    on_success_callback=on_success_callback,
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
        execution_delta=timedelta(hours=1),  # raw runs at 05:00, this DAG runs at 06:00
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

    notify_result = PythonOperator(
        task_id="notify_daily_result",
        python_callable=notify_daily_result,
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
        >> notify_result
        >> end
    )
