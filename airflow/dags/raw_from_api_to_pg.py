"""
DAG: Load earthquake data from USGS API -> DuckDB -> PostgreSQL (ODS)
"""

import logging
import duckdb
import pendulum
import requests
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from telegram_notify import on_failure_callback, on_success_callback

# ========== PARAMETERS ==========
OWNER = "mykyta"
DAG_ID = "raw_from_api_to_pg"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 7, 12, tz="UTC"),
    "catchup": False,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
    "on_failure_callback": on_failure_callback,
}


def get_dates(**context):
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date


def load_api_to_postgres(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"⏳ Loading data for: {start_date} → {end_date}")

    # 1. Fetch CSV from the API and write it to a temp file
    #    (read_csv_auto expects a file path, not raw text)
    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}"
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    tmp_path = f"/tmp/earthquake_{start_date}.csv"
    with open(tmp_path, "wb") as f:
        f.write(response.content)

    # 2. Use DuckDB to read the CSV and insert straight into PostgreSQL
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

    # Create the table if it doesn't exist yet
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

    # Delete existing rows for this interval before inserting — keeps retries idempotent
    con.sql(f"""
        DELETE FROM dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        WHERE time::date >= '{start_date}' AND time::date < '{end_date}'
    """)

    # Insert the data
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
        FROM read_csv_auto('{tmp_path}')
    """)

    con.close()
    logging.info(f"✅ Data for {start_date} loaded into {SCHEMA}.{TARGET_TABLE}")


# ========== DAG ==========
with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    on_success_callback=on_success_callback,
    tags=["api", "pg", "ods"],
    description="Load earthquake data from USGS API into PostgreSQL",
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
