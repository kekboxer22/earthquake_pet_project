# earthquake_pet_project
earthquake_pet_project

"""bash
python3.12 -m venv venv
"""

markdown
# Earthquake ELT Pipeline (Airflow + DuckDB + PostgreSQL)

A production‑ready ETL pipeline that ingests earthquake data from the public USGS API, loads it into PostgreSQL (ODS layer), and builds daily analytical marts: **number of earthquakes per day** and **average magnitude per day**.

---

## Architecture
USGS API (CSV)
│
▼
DuckDB (in‑memory, reads CSV and pushes to Postgres via postgres‑extension)
│
▼
PostgreSQL "warehouse"
├─ ods.fct_earthquake ← raw events (raw_from_api_to_pg)
├─ stg.tmp_* ← staging tables for mart recomputation
└─ dm.fct_count_day_earthquake ← count of earthquakes / day
└─ dm.fct_avg_day_earthquake ← average magnitude / day

Orchestration: Apache Airflow (LocalExecutor)
DAG 1: raw_from_api_to_pg (daily, fetches data for the interval)
DAG 2: fct_count_day_earthquake (waits for DAG 1 via ExternalTaskSensor)
DAG 3: fct_avg_day_earthquake (waits for DAG 1 via ExternalTaskSensor)

text

---

## Tech Stack

- Apache Airflow 2.9 (LocalExecutor)
- DuckDB (in‑memory processing, Postgres extension)
- PostgreSQL 15 (DWH: ods / stg / dm)
- Docker Compose

---

## What’s Included

- `docker-compose.yaml` — spins up Airflow (webserver, scheduler, init) + a dedicated Postgres `warehouse-postgres` (hostname matches the one hardcoded in `raw_from_api_to_pg.py`) + a separate Postgres for Airflow metadata.
- `Dockerfile` — based on `apache/airflow:2.9.3-python3.11`, installs additional Python packages.
- `requirements.txt` — `duckdb`, `requests`, `apache-airflow-providers-postgres`, `apache-airflow-providers-common-sql` (required for DAG imports).
- `client-postgres-init/01_init_schemas.sql` — creates `ods/stg/dm` schemas and mart tables (`dm.fct_count_day_earthquake`, `dm.fct_avg_day_earthquake`). Without this, DAGs 2 & 3 will fail on the first `DELETE/INSERT`.
- Airflow connection `postgres_dwh` is injected via `AIRFLOW_CONN_POSTGRES_DWH` environment variable — no manual UI setup needed.

---

## Quick Start

```bash
docker compose up -d --build
Airflow UI: http://localhost:8080 (login: admin / admin)

Postgres DWH (externally): localhost:5433 (user/pass/db: postgres / postgres / postgres)

Then:

Enable DAG raw_from_api_to_pg.

Wait for it to turn green (at least one successful run).

Enable fct_count_day_earthquake and fct_avg_day_earthquake (they wait for the first DAG via sensors).

Known Issues & TODOs
read_csv_auto in raw_from_api_to_pg.py may not work as expected.
Currently it receives the raw CSV content as a string (csv_data.getvalue().decode()), but read_csv_auto expects a file path/glob, not raw text. Fix: either write the CSV to a temporary file and pass the path, or register a DataFrame via con.register(...) with pandas instead of read_csv_auto.

catchup=True + start_date=2025-05-01.
On the first run, Airflow will attempt to backfill every missed day — which can take time and flood logs. For quick demos, set catchup=False or move start_date closer to today.

Hardcoded credentials (postgres/postgres).
Fine for local dev, but for prod — move to Airflow Connections / .env and never commit them.

Why This Project Matters
Demonstrates:

Orchestration (Airflow, cross‑DAG sensors, retries)

Layered data modelling (ods → stg → dm, idempotent delete+insert pattern)

External API integration

DuckDB as an ETL engine

Full Docker Compose setup
