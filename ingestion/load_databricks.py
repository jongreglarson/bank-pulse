"""
Ingest FDIC data into Databricks bronze layer.

Targets Databricks free trial / paid workspaces with Unity Catalog and
a Serverless SQL Warehouse. Uploads raw JSON to a Unity Catalog Volume via
the Databricks SDK, then loads into bronze Delta tables via COPY INTO.

Required env vars (see .env.example):
    DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH,
    DATABRICKS_CATALOG (default: main), DATABRICKS_DATABASE (default: bank_pulse)
"""

import io
import json
import os
from datetime import datetime, timezone

from databricks import sql
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

from fdic_client import fetch_failures, fetch_history, fetch_institutions, fetch_summary

load_dotenv()

_host = os.environ["DATABRICKS_HOST"].rstrip("/")
HOST_URL = _host if _host.startswith("http") else f"https://{_host}"
HOSTNAME = HOST_URL.replace("https://", "").replace("http://", "")
TOKEN = os.environ["DATABRICKS_TOKEN"]
HTTP_PATH = os.environ["DATABRICKS_HTTP_PATH"]

CATALOG = os.environ.get("DATABRICKS_CATALOG", "main")
DATABASE = os.environ.get("DATABRICKS_DATABASE", "bank_pulse")
BRONZE_SCHEMA = f"{DATABASE}_bronze"
VOLUME_NAME = "raw_json"
VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/{VOLUME_NAME}"

ENDPOINTS = {
    "institutions": fetch_institutions,
    "history": fetch_history,
    "summary": fetch_summary,
    "failures": fetch_failures,
}


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def upload_to_volume(client: WorkspaceClient, endpoint: str, records: list[dict]) -> None:
    """Upload newline-delimited JSON to a Unity Catalog Volume via the SDK."""
    filename = f"{endpoint}_{_timestamp()}.json"
    file_path = f"{VOLUME_PATH}/{endpoint}/{filename}"
    payload = "\n".join(json.dumps(r) for r in records).encode("utf-8")

    client.files.upload(file_path, io.BytesIO(payload), overwrite=True)
    print(f"  Uploaded {len(records)} records -> {file_path}")


def ensure_bronze_objects(cursor) -> None:
    """Create schema, volume, and tables if they don't exist."""
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}")
    cursor.execute(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{VOLUME_NAME}")
    for endpoint in ENDPOINTS:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {CATALOG}.{BRONZE_SCHEMA}.{endpoint}
            USING DELTA
            TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
        """)


def copy_into_bronze(cursor, endpoint: str) -> None:
    table = f"{CATALOG}.{BRONZE_SCHEMA}.{endpoint}"
    source = f"{VOLUME_PATH}/{endpoint}/"
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
    client = WorkspaceClient(host=HOST_URL, token=TOKEN)

    with sql.connect(
        server_hostname=HOSTNAME,
        http_path=HTTP_PATH,
        access_token=TOKEN,
    ) as conn:
        with conn.cursor() as cursor:
            print("Setting up bronze objects...")
            ensure_bronze_objects(cursor)

            for endpoint, fetch_fn in ENDPOINTS.items():
                print(f"\nFetching {endpoint}...")
                records = fetch_fn()
                print(f"  Retrieved {len(records)} records")
                upload_to_volume(client, endpoint, records)
                copy_into_bronze(cursor, endpoint)

    print("\nDatabricks bronze load complete.")


if __name__ == "__main__":
    main()
