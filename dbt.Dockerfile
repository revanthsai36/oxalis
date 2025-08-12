FROM python:3.11-slim

# dbt for Postgres
RUN pip install --no-cache-dir dbt-postgres==1.8.6

# Helpful tools
RUN apt-get update && apt-get install -y --no-install-recommends     git nano &&     rm -rf /var/lib/apt/lists/*

WORKDIR /dbt

# Default command to keep container usable for ad-hoc dbt commands
CMD ["bash", "-lc", "dbt --help"]
