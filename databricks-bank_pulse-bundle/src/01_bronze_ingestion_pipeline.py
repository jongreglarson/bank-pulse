import dlt
import requests
import pandas as pd
import time
from pyspark.sql import functions as F
from datetime import datetime

FDIC_BASE_URL = "https://banks.data.fdic.gov/api/financials"
FDIC_FIELDS = [
    "REPDTE", "CERT", "INTINC", "EINTEXP",
    "NIEXP", "NETINC", "ASSET", "DEP",
    "LNLSNET", "NPERFV", "RBC1RWAJ", "ROA", "ROE",
]


def _fetch_fdic_financials(limit=1000, max_records=50000):
    all_records = []
    offset = 0

    while offset < max_records:
        params = {
            "fields": ",".join(FDIC_FIELDS),
            "limit": limit,
            "offset": offset,
            "sort_by": "REPDTE",
            "sort_order": "DESC",
            "output": "json",
        }

        for attempt in range(5):
            try:
                response = requests.get(FDIC_BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                break
            except requests.exceptions.HTTPError:
                if response.status_code == 429:
                    time.sleep(2 ** attempt)
                else:
                    raise

        data = response.json()
        records = data.get("data", [])
        if not records:
            break

        all_records.extend([r["data"] for r in records])
        offset += limit
        time.sleep(0.5)

        if len(records) < limit:
            break

    return all_records


@dlt.table(
    name="bronze_financials",
    comment="Raw FDIC financial data ingested from BankFind Suite API",
    table_properties={"quality": "bronze"},
)
def bronze_financials():
    records = _fetch_fdic_financials()
    df = spark.createDataFrame(pd.DataFrame(records))
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    return (
        df.withColumn("ingested_at", F.current_timestamp())
          .withColumn("source", F.lit("fdic_bankfind_api"))
          .withColumn("batch_id", F.lit(batch_id))
    )
