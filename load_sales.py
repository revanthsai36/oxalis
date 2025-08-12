import os
import sys
import io
import csv
import logging
from datetime import datetime, timedelta
import random
import pandas as pd
import psycopg2

LOG_LEVEL = os.getenv("PYTHON_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("etl")

DB_CFG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "analytics"),
    "user": os.getenv("POSTGRES_USER", "analytics"),
    "password": os.getenv("POSTGRES_PASSWORD", "analytics"),
}

RAW_SCHEMA = "raw"
RAW_TABLE = "sales"

ACCEPTED_CURRENCIES = {"USD", "EUR", "GBP"}

def get_connection():
    return psycopg2.connect(**DB_CFG)

def ensure_schema_table(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
            create schema if not exists {RAW_SCHEMA};
            create table if not exists {RAW_SCHEMA}.{RAW_TABLE} (
                sale_id text primary key,
                sale_ts timestamp not null,
                sale_date date not null,
                store_id text not null,
                product_id text not null,
                quantity int not null check (quantity > 0),
                unit_price numeric(12,2) not null check (unit_price >= 0),
                currency text not null,
                total_amount numeric(14,2) not null check (total_amount >= 0)
            );
        """)
    conn.commit()

def generate_synthetic(n_rows=10000, start_days_ago=30):
    random.seed(42)
    now = datetime.utcnow()
    start = now - timedelta(days=start_days_ago)
    rows = []
    for i in range(n_rows):
        ts = start + timedelta(seconds=random.randint(0, start_days_ago*86400))
        date = ts.date()
        store = f"S{random.randint(1,20):03d}"
        product = f"P{random.randint(1,200):04d}"
        qty = random.randint(1,5)
        price = round(random.uniform(1.0, 250.0), 2)
        currency = random.choice(tuple(ACCEPTED_CURRENCIES))
        total = round(qty * price, 2)
        sale_id = f"{store}-{product}-{int(ts.timestamp())}-{i}"
        rows.append({
            "sale_id": sale_id,
            "sale_ts": ts.isoformat(sep=' '),
            "sale_date": date.isoformat(),
            "store_id": store,
            "product_id": product,
            "quantity": qty,
            "unit_price": price,
            "currency": currency,
            "total_amount": total
        })
    return pd.DataFrame(rows)

def read_or_generate():
    csv_path = "/data/example_sales.csv"
    if os.path.exists(csv_path):
        logger.info("Reading provided CSV at %s", csv_path)
        df = pd.read_csv(csv_path)
    else:
        logger.info("No CSV found. Generating synthetic dataset.")
        df = generate_synthetic()
    return df

def validate(df: pd.DataFrame):
    required_cols = ["sale_id","sale_ts","sale_date","store_id","product_id","quantity","unit_price","currency","total_amount"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Basic checks
    if df["sale_id"].isna().any():
        raise ValueError("sale_id contains nulls")
    if (df["quantity"] <= 0).any():
        raise ValueError("quantity must be > 0")
    if (df["unit_price"] < 0).any():
        raise ValueError("unit_price must be >= 0")
    if (df["total_amount"] < 0).any():
        raise ValueError("total_amount must be >= 0")
    bad_currency = ~df["currency"].isin(list(ACCEPTED_CURRENCIES))
    if bad_currency.any():
        raise ValueError(f"currency contains values outside {ACCEPTED_CURRENCIES}")

    # Coerce types
    df["sale_ts"] = pd.to_datetime(df["sale_ts"])
    df["sale_date"] = pd.to_datetime(df["sale_date"]).dt.date

    # De-duplicate on sale_id
    before = len(df)
    df = df.drop_duplicates(subset=["sale_id"]).copy()
    after = len(df)
    if after < before:
        logger.warning("Dropped %d duplicate rows by sale_id", before - after)

    return df

def copy_into_postgres(conn, df: pd.DataFrame):
    # Use COPY FROM STDIN for speed
    buffer = io.StringIO()
    cols = ["sale_id","sale_ts","sale_date","store_id","product_id","quantity","unit_price","currency","total_amount"]
    df_out = df.loc[:, cols].copy()
    # Ensure strings not NaN
    df_out = df_out.fillna("")
    writer = csv.writer(buffer)
    for row in df_out.itertuples(index=False):
        writer.writerow(row)
    buffer.seek(0)

    with conn.cursor() as cur:
        cur.execute(f"set search_path to {RAW_SCHEMA};")
        # Upsert behavior: insert and ignore conflicts
        cur.copy_expert(f"""
            COPY {RAW_TABLE} ({", ".join(cols)}) FROM STDIN WITH (FORMAT csv)
        """, buffer)

    conn.commit()

def main():
    logger.info("Starting ETL load to %s.%s", RAW_SCHEMA, RAW_TABLE)
    df = read_or_generate()
    df = validate(df)
    logger.info("Validated %d rows", len(df))

    with get_connection() as conn:
        ensure_schema_table(conn)
        copy_into_postgres(conn, df)
        # Report counts
        with conn.cursor() as cur:
            cur.execute(f"select count(*) from {RAW_SCHEMA}.{RAW_TABLE}")
            total = cur.fetchone()[0]
            cur.execute(f"select min(sale_date), max(sale_date) from {RAW_SCHEMA}.{RAW_TABLE}")
            min_d, max_d = cur.fetchone()
    logger.info("Load complete. raw.sales rows=%s, date_range=[%s, %s]", total, min_d, max_d)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("ETL failed: %s", e)
        sys.exit(1)
