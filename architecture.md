# Architecture & Approach

### Why this shape
- **Postgres** stands in for Snowflake; schemas partition raw vs analytics.
- **Python ETL** handles file I/O, validations, fast loads (`COPY`), and basic observability.
- **dbt** formalizes transformations, lineage, docs, and tests; an **incremental** fact shows warehouse patterns.
- **Compose orchestration** keeps it simple: deterministic order, health checks, and an idempotent run script.

### Data flow
```
[CSV/Synthetic] → Python validate → Postgres raw.sales → dbt stg_sales → int_daily_sales → fct_daily_sales_metrics
```

### Error handling
- ETL raises on schema/quality violations and exits non‑zero.
- dbt tests fail the run if key constraints are violated.
- Orchestrator stops on first failure.

### Extending
- Add new raw tables via `etl/*.py` and mirror in `dbt/models/staging`.
- Promote business logic into `intermediate` and `marts` with tests.
- Swap Postgres for Snowflake by changing `dbt/profiles.yml` and connection libs.