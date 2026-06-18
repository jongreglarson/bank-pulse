"""
Ingest FDIC data into Databricks bronze layer.

Writes raw JSON to a Unity Catalog Volume, then loads into bronze Delta tables
via COPY INTO. Requires DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH
and DATABRICKS_CATALOG env vars (loaded from .env).
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from databricks import sql
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

from fdic_client import fetch_failures, fetch_history, fetch_institutions, fetch_summary

load_dotenv()

HOST = os.environ["DATABRICKS_HOST"]
TOKEN = os.environ["DATABRICKS_TOKEN"]
HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]
CATALOG = os.environ.get("DATABRICKS_CATALOG", "bank_pulse")
SCHEMA_BRONZE = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/raw_json"

ENDPOINTS = {
    "institutions": fetch_institutions,
    "history": fetch_history,
    "summary": fetch_summary,
    "failures": fetch_failures,
}


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def upload_to_volume(client: WorkspaceClient, endpoint: str, records: list[dict]) -> str:
    """Upload a JSON file to the Unity Catalog Volume and return the volume path."""
    filename = f"{endpoint}_{_timestamp()}.json"
    volume_file_path = f"{VOLUME_PATH}/{endpoint}/{filename}"

    payload = "\n".join(json.dumps(r) for r in records)  # newline-delimited JSON

    client.files.upload(volume_file_path, payload.encode("utf-8"), overwrite=True)
    print(f"  Uploaded {len(records)} records → {volume_file_path}")
    return volume_file_path


def copy_into_bronze(cursor, endpoint: str) -> None:
    """COPY INTO the bronze Delta table from the Volume directory."""
    table = f"{CATALOG}.{SCHEMA_BRONZE}.{endpoint}"
    source = f"{VOLUME_PATH}/{endpoint}/"

    cursor.execute(f"""
        COPY INTO {table}
        FROM (
            SELECT
                *,
                _metadata.file_name AS _source_file,
                current_timestamp()  AS _loaded_at
            FROM '{source}'
        )
        FILEFORMAT = JSON
        FORMAT_OPTIONS ('inferSchema' = 'true', 'mergeSchema' = 'true')
        COPY_OPTIONS ('mergeSchema' = 'true')
    """)
    print(f"  COPY INTO {table} complete")


def ensure_bronze_tables(cursor) -> None:
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")
    for endpoint in ENDPOINTS:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}.{endpoint}
            USING DELTA
            TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
        """)


def main() -> None:
    client = WorkspaceClient(host=HOST, token=TOKEN)

    with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=TOKEN) as conn:
        with conn.cursor() as cursor:
            ensure_bronze_tables(cursor)

            for endpoint, fetch_fn in ENDPOINTS.items():
                print(f"\nFetching {endpoint}...")
                records = fetch_fn()
                print(f"  Retrieved {len(records)} records")
                upload_to_volume(client, endpoint, records)
                copy_into_bronze(cursor, endpoint)

    print("\nDatabricks bronze load complete.")


if __name__ == "__main__":
    main()
