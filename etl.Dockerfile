FROM python:3.11-slim

WORKDIR /app

# System deps for psycopg2 and CSV fast paths
RUN apt-get update && apt-get install -y --no-install-recommends     gcc libpq-dev &&     rm -rf /var/lib/apt/lists/*

COPY etl/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY etl /app
COPY data /data

ENV PYTHONUNBUFFERED=1

CMD ["python", "/app/load_sales.py"]
