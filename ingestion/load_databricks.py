"""
Ingest FDIC data into Databricks bronze layer (Community Edition compatible).

Uses DBFS for file storage and cluster-based SQL via the Databricks SQL connector.
No Unity Catalog required — targets the Hive metastore.

Required env vars (see .env.example):
    DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH
"""

import base64
import json
import os
from datetime import datetime, timezone

import requests
from databricks import sql
from dotenv import load_dotenv

from fdic_client import fetch_failures, fetch_history, fetch_institutions, fetch_summary

load_dotenv()

_host = os.environ["DATABRICKS_HOST"].rstrip("/")
HOST_URL = _host if _host.startswith("http") else f"https://{_host}"
HOSTNAME = HOST_URL.replace("https://", "").replace("http://", "")
TOKEN = os.environ["DATABRICKS_TOKEN"]
HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]

DATABASE = os.environ.get("DATABRICKS_DATABASE", "bank_pulse")
BRONZE_DB = f"{DATABASE}_bronze"
DBFS_BASE = f"dbfs:/{DATABASE}/raw"
CHUNK_SIZE = 900_000  # DBFS add-block limit is 1MB; stay under

ENDPOINTS = {
    "institutions": fetch_institutions,
    "history": fetch_history,
    "summary": fetch_summary,
    "failures": fetch_failures,
}


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _headers() -> dict:
    return {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}


def upload_to_dbfs(endpoint: str, records: list[dict]) -> None:
    """Upload newline-delimited JSON to DBFS using the chunked block API."""
    filename = f"{endpoint}_{_timestamp()}.json"
    dbfs_path = f"{DBFS_BASE}/{endpoint}/{filename}"
    payload = "\n".join(json.dumps(r) for r in records).encode("utf-8")

    resp = requests.post(
        f"{HOST_URL}/api/2.0/dbfs/create",
        headers=_headers(),
        json={"path": dbfs_path, "overwrite": True},
    )
    resp.raise_for_status()
    handle = resp.json()["handle"]

    offset = 0
    while offset < len(payload):
        chunk = payload[offset : offset + CHUNK_SIZE]
        requests.post(
            f"{HOST_URL}/api/2.0/dbfs/add-block",
            headers=_headers(),
            json={"handle": handle, "data": base64.b64encode(chunk).decode()},
        ).raise_for_status()
        offset += CHUNK_SIZE

    requests.post(
        f"{HOST_URL}/api/2.0/dbfs/close",
        headers=_headers(),
        json={"handle": handle},
    ).raise_for_status()

    print(f"  Uploaded {len(records)} records → {dbfs_path}")


def ensure_bronze_objects(cursor) -> None:
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")
    for endpoint in ENDPOINTS:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {BRONZE_DB}.{endpoint}
            USING DELTA
            TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
        """)


def copy_into_bronze(cursor, endpoint: str) -> None:
    table = f"{BRONZE_DB}.{endpoint}"
    source = f"{DBFS_BASE}/{endpoint}/"
    cursor.execute(f"""
        COPY INTO {table}
        FROM (
            SELECT
                *,
                _metadata.file_name AS _source_file,
                current_timestamp() AS _loaded_at
            FROM '{source}'
        )
        FILEFORMAT = JSON
        FORMAT_OPTIONS ('inferSchema' = 'true', 'mergeSchema' = 'true')
        COPY_OPTIONS ('mergeSchema' = 'true')
    """)
    print(f"  COPY INTO {table} complete")


def main() -> None:
    print(f"Connecting to {HOSTNAME}...")
    with sql.connect(
        server_hostname=HOSTNAME,
        http_path=HTTP_PATH,
        access_token=TOKEN,
    ) as conn:
        with conn.cursor() as cursor:
            ensure_bronze_objects(cursor)

            for endpoint, fetch_fn in ENDPOINTS.items():
                print(f"\nFetching {endpoint}...")
                records = fetch_fn()
                print(f"  Retrieved {len(records)} records")
                upload_to_dbfs(endpoint, records)
                copy_into_bronze(cursor, endpoint)

    print("\nDatabricks bronze load complete.")


if __name__ == "__main__":
    main()
