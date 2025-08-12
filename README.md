# OXO Data Platform Engineer — Take‑Home Solution

End‑to‑end local pipeline using Docker Compose: Python extracts / generates sales data and loads it to Postgres, `dbt` builds staging → intermediate → final models, and a lightweight orchestrator runs everything in order with tests.

## Quick start

```bash
# 1) Build images and start Postgres
docker compose up -d --build postgres

# 2) Run the pipeline (ETL → dbt run → dbt test → validate)
./bin/run_pipeline.sh
```

If you're on Windows PowerShell, run:
```powershell
wsl ./bin/run_pipeline.sh
```

## What this pipeline does

1. **Extract/Load (Python)**  
   - Generates synthetic sales data (or reads from `data/example_sales.csv` if you drop one in).  
   - Validates the schema and basic quality rules (non‑nulls, positive amounts, known currencies).  
   - Loads into Postgres `raw.sales` (append) using `COPY` for speed.

2. **Transform (dbt)**  
   - `stg_sales`: type-corrects, de‑duplicates, enforces constraints.  
   - `int_daily_sales`: aggregates per day and per store/product.  
   - `fct_daily_sales_metrics`: final metrics table (incremental by `sale_date`).  
   - Data tests: `not_null`, `unique`, accepted values, and relationship checks.

3. **Orchestrate (bash)**  
   - Waits for Postgres readiness.  
   - Runs the Python loader.  
   - Executes `dbt deps`, `dbt run`, `dbt test`.  
   - Validates row counts + shows a sample of final facts.

## Repo structure

```
.
├─ bin/
│  ├─ run_pipeline.sh
├─ compose.yaml
├─ .env
├─ docker/
│  ├─ etl.Dockerfile
│  └─ dbt.Dockerfile
├─ etl/
│  ├─ requirements.txt
│  └─ load_sales.py
├─ dbt/
│  ├─ dbt_project.yml
│  ├─ profiles.yml
│  └─ models/
│     ├─ staging/
│     │  ├─ stg_sales.sql
│     │  └─ stg_sales.yml
│     ├─ intermediate/
│     │  └─ int_daily_sales.sql
│     └─ marts/
│        ├─ fct_daily_sales_metrics.sql
│        └─ marts.yml
├─ data/
│  └─ example_sales.csv
└─ docs/
   └─ architecture.md
```

## Credentials and configuration

Edit `.env` to adjust defaults:

```
POSTGRES_USER=analytics
POSTGRES_PASSWORD=analytics
POSTGRES_DB=analytics
POSTGRES_PORT=5432
POSTGRES_HOST=postgres
PYTHON_LOG_LEVEL=INFO
```

`dbt/profiles.yml` is wired to read the same values via env vars.

## Running pieces manually (optional)

```bash
# Bring up the database only
docker compose up -d postgres

# Load data only
docker compose run --rm etl python /app/load_sales.py

# Run dbt only
docker compose run --rm dbt dbt deps
docker compose run --rm dbt dbt run
docker compose run --rm dbt dbt test
```

## Teardown

```bash
docker compose down -v
```

## Notes for reviewers

- The final model `fct_daily_sales_metrics` is **incremental** on `sale_date` to demonstrate warehouse‑friendly patterns.
- Basic data quality tests live alongside models; failing tests will exit non‑zero.
- Python ETL uses `COPY` for fast loads and logs metrics for observability.
- The pipeline is easily extended by adding sources to `etl/` and models to `dbt/models`.