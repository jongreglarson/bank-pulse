# Databricks notebook source
# MAGIC %md
# MAGIC # Bank Pulse — FDIC Bronze Ingestion
# MAGIC
# MAGIC Fetches FDIC BankFind Suite data and writes to Unity Catalog bronze Delta tables:
# MAGIC - **financials** — per-bank quarterly metrics (ROA, ROE, Tier 1 capital, etc.)
# MAGIC - **institutions** — bank metadata (name, city, state, active status)
# MAGIC - **failures** — FDIC-recorded failure events

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import time
import requests
from pyspark.sql import functions as F
from datetime import datetime

CATALOG       = "main"
BRONZE_SCHEMA = "bank_pulse_bronze"
BASE_URL      = "https://banks.data.fdic.gov/api"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
print(f"Schema ready: {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# DBTITLE 1,FDIC fetch helper (with retry + backoff)

def fetch_endpoint(endpoint: str, fields: list, max_records: int = 500000, page_size: int = 1000) -> list:
    all_records = []
    offset = 0

    while offset < max_records:
        params = {
            "fields":     ",".join(fields),
            "limit":      page_size,
            "offset":     offset,
            "sort_by":    fields[0],
            "sort_order": "DESC",
            "output":     "json",
        }

        for attempt in range(5):
            try:
                resp = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=30)
                resp.raise_for_status()
                break
            except requests.exceptions.HTTPError:
                if resp.status_code == 429:
                    wait = 2 ** attempt
                    print(f"  Rate limited — waiting {wait}s (attempt {attempt+1}/5)...")
                    time.sleep(wait)
                else:
                    raise

        records = resp.json().get("data", [])
        if not records:
            break

        # FDIC wraps each record in {"data": {...}}
        all_records.extend([r["data"] if "data" in r else r for r in records])
        offset += len(records)
        print(f"  {endpoint}: {len(all_records):,} records fetched...")
        time.sleep(0.5)

        if len(records) < page_size:
            break

    return all_records

# COMMAND ----------

# DBTITLE 1,1. Financials — per-bank quarterly data

print("Fetching financials...")
fin_records = fetch_endpoint("financials", [
    "repdte", "cert", "intinc", "eintexp",
    "niexp", "netinc", "asset", "dep",
    "lnlsnet", "nperfv", "rbc1rwaj", "roa", "roe"
], max_records=100000)

fin_df = (
    spark.createDataFrame(fin_records)
         .withColumn("_ingested_at", F.current_timestamp())
         .withColumn("_source", F.lit("fdic_financials"))
         .withColumn("_batch_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
)

(
    fin_df.write
          .format("delta")
          .mode("overwrite")
          .option("mergeSchema", "true")
          .partitionBy("REPDTE")
          .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.financials")
)
print(f"  Saved {CATALOG}.{BRONZE_SCHEMA}.financials ({fin_df.count():,} rows)")

# COMMAND ----------

# DBTITLE 1,2. Institutions — bank metadata

print("\nFetching institutions...")
inst_records = fetch_endpoint("institutions", [
    "CERT", "NAME", "CITY", "STNAME", "ASSET", "DEP", "NETINC", "REPDTE", "ACTIVE"
])

inst_df = (
    spark.createDataFrame(inst_records)
         .withColumn("_ingested_at", F.current_timestamp())
)

(
    inst_df.write
           .format("delta")
           .mode("overwrite")
           .option("mergeSchema", "true")
           .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.institutions")
)
print(f"  Saved {CATALOG}.{BRONZE_SCHEMA}.institutions ({inst_df.count():,} rows)")

# COMMAND ----------

# DBTITLE 1,3. Failures — FDIC failure events

print("\nFetching failures...")
fail_records = fetch_endpoint("failures", [
    "CERT", "NAME", "FAILDATE", "SAVR", "RESTYPE", "COST", "QBFDEP", "ASSET"
])

fail_df = (
    spark.createDataFrame(fail_records)
         .withColumn("_ingested_at", F.current_timestamp())
)

(
    fail_df.write
           .format("delta")
           .mode("overwrite")
           .option("mergeSchema", "true")
           .saveAsTable(f"{CATALOG}.{BRONZE_SCHEMA}.failures")
)
print(f"  Saved {CATALOG}.{BRONZE_SCHEMA}.failures ({fail_df.count():,} rows)")

# COMMAND ----------

# DBTITLE 1,Row count verification

for tbl in ["financials", "institutions", "failures"]:
    n = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.{tbl}").count()
    print(f"  main.{BRONZE_SCHEMA}.{tbl}: {n:,} rows")

print("\nBronze load complete.")
