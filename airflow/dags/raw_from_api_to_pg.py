"""
DAG: Загрузка данных из USGS API → DuckDB → PostgreSQL (ODS)
"""

import logging
import duckdb
import pendulum
import requests
from io import BytesIO
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ========== ПАРАМЕТРЫ ==========
OWNER = "mykyta"
DAG_ID = "raw_from_api_to_pg"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 5, 1, tz="UTC"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context):
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date


def load_api_to_postgres(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"⏳ Загрузка за: {start_date} → {end_date}")

    # 1. Забираем данные из API в CSV
    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}"
    response = requests.get(url)
    response.raise_for_status()
    csv_data = BytesIO(response.content)

    # 2. Через DuckDB читаем CSV и сразу вставляем в PostgreSQL
    con = duckdb.connect()
    con.sql(f"""
        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST 'warehouse-postgres',
            PORT 5432,
            DATABASE postgres,
            USER 'postgres',
            PASSWORD 'postgres'
        );
        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);
    """)

    # Создаём таблицу, если её нет
    con.sql(f"""
        CREATE TABLE IF NOT EXISTS dwh_postgres_db.{SCHEMA}.{TARGET_TABLE} (
            time TIMESTAMP,
            latitude FLOAT,
            longitude FLOAT,
            depth FLOAT,
            mag FLOAT,
            mag_type VARCHAR,
            nst INT,
            gap FLOAT,
            dmin FLOAT,
            rms FLOAT,
            net VARCHAR,
            id VARCHAR PRIMARY KEY,
            updated TIMESTAMP,
            place TEXT,
            type VARCHAR,
            horizontal_error FLOAT,
            depth_error FLOAT,
            mag_error FLOAT,
            mag_nst INT,
            status VARCHAR,
            location_source VARCHAR,
            mag_source VARCHAR
        );
    """)

    # Вставка данных
    con.sql(f"""
        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        SELECT
            time,
            latitude,
            longitude,
            depth,
            mag,
            magType AS mag_type,
            nst,
            gap,
            dmin,
            rms,
            net,
            id,
            updated,
            place,
            type,
            horizontalError AS horizontal_error,
            depthError AS depth_error,
            magError AS mag_error,
            magNst AS mag_nst,
            status,
            locationSource AS location_source,
            magSource AS mag_source
        FROM read_csv_auto('{csv_data.getvalue().decode()}')
    """)

    con.close()
    logging.info(f"✅ Данные за {start_date} загружены в {SCHEMA}.{TARGET_TABLE}")


# ========== DAG ==========
with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["api", "pg", "ods"],
    description="Загрузка землетрясений из USGS API в PostgreSQL",
    concurrency=1,
    max_active_runs=1,
) as dag:
    start = DummyOperator(task_id="start")
    load_task = PythonOperator(
        task_id="load_api_to_postgres",
        python_callable=load_api_to_postgres,
    )
    end = DummyOperator(task_id="end")

    start >> load_task >> end
