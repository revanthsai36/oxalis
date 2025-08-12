#!/usr/bin/env bash
set -euo pipefail

echo "==> Building images (if needed)"
docker compose build

echo "==> Ensuring Postgres is up"
docker compose up -d postgres

echo "==> Waiting for Postgres healthcheck..."
# Compose healthcheck handles this via depends_on; small sleep is still helpful for first-time init
sleep 3

echo "==> Running Python ETL to load raw.sales"
docker compose run --rm etl python /app/load_sales.py

echo "==> Running dbt deps"
docker compose run --rm dbt dbt deps

echo "==> Running dbt models"
docker compose run --rm dbt dbt run

echo "==> Running dbt tests"
docker compose run --rm dbt dbt test

echo "==> Validation query (top 10 days)"
docker compose exec -T postgres psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c "select * from analytics.fct_daily_sales_metrics order by sale_date desc limit 10;"

echo "==> Done."
