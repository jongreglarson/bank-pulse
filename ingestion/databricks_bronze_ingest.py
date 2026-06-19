# Databricks notebook source
# MAGIC %md
# MAGIC # Bank Pulse — FDIC Bronze Ingestion
# MAGIC
# MAGIC Fetches FDIC BankFind Suite data directly from the API and loads it into
# MAGIC Unity Catalog bronze Delta tables. Run this notebook once to populate the
# MAGIC bronze layer, then run dbt to build silver and gold.

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import time
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG       = "main"
DATABASE      = "bank_pulse"
BRONZE_SCHEMA = f"{DATABASE}_bronze"
BASE_URL      = "https://banks.data.fdic.gov/api"
PAGE_SIZE     = 1000

# COMMAND ----------

# DBTITLE 1,FDIC API client

def fetch_endpoint(endpoint: str, fields: list[str], delay: float = 0.2) -> list[dict]:
    """Pull all records from a FDIC endpoint with automatic pagination."""
    offset = 0
    results = []
    while True:
        params = {
            "fields": ",".join(fields),
            "limit":  PAGE_SIZE,
            "offset": offset,
            "output": "json",
        }
        resp = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=30)
        resp.raise_for_status()
        data  = resp.json()
        page  = data.get("data", [])
        results.extend(page)
        total  = data.get("meta", {}).get("total", 0)
        offset += len(page)
        if offset >= total or not page:
            break
        time.sleep(delay)
    return results


def fetch_institutions():
    return fetch_endpoint("institutions",
        ["cert","name","city","stname","asset","dep","netinc","repdte","active"])

def fetch_history():
    return fetch_endpoint("history",
        ["cert","instname","class","pcity","pstalp","procdate","action"])

def fetch_summary():
    return fetch_endpoint("summary",
        ["repdte","asset","dep","intinc","nonii","netinc","lnlsnet"])

def fetch_failures():
    return fetch_endpoint("failures",
        ["cert","name","faildate","savr","restype","cost","qbfdep","asset"])

# COMMAND ----------

# DBTITLE 1,Setup bronze schema

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
print(f"Schema ready: {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Load each endpoint into a bronze Delta table

ENDPOINTS = {
    "institutions": fetch_institutions,
    "history":      fetch_history,
    "summary":      fetch_summary,
    "failures":     fetch_failures,
}

for table_name, fetch_fn in ENDPOINTS.items():
    full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    print(f"\nFetching {table_name}...")
    records = fetch_fn()
    print(f"  Retrieved {len(records):,} records")

    df = (
        spark.createDataFrame(records)
             .withColumn("_loaded_at", current_timestamp())
    )

    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("mergeSchema", "true")
          .option("delta.autoOptimize.optimizeWrite", "true")
          .saveAsTable(full_table)
    )
    print(f"  Saved -> {full_table}  ({df.count():,} rows)")

print("\nBronze load complete.")

# COMMAND ----------

# DBTITLE 1,Quick row-count verification

for table_name in ENDPOINTS:
    full_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"
    n = spark.table(full_table).count()
    print(f"  {full_table}: {n:,} rows")
