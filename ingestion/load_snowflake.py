"""
Ingest FDIC data into Snowflake bronze layer.

Writes raw JSON to a Snowflake internal stage, then loads into bronze tables
via COPY INTO. Requires SNOWFLAKE_* env vars (loaded from .env).
"""

import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

from fdic_client import fetch_failures, fetch_history, fetch_institutions, fetch_summary

load_dotenv()

SF_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SF_USER = os.environ["SNOWFLAKE_USER"]
SF_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SF_WAREHOUSE = os.environ["SNOWFLAKE_WAREHOUSE"]
SF_DATABASE = os.environ.get("SNOWFLAKE_DATABASE", "BANK_PULSE")
SF_ROLE = os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN")
SCHEMA_BRONZE = "BRONZE"
STAGE_NAME = f"{SF_DATABASE}.{SCHEMA_BRONZE}.FDIC_RAW_STAGE"

ENDPOINTS = {
    "INSTITUTIONS": fetch_institutions,
    "HISTORY": fetch_history,
    "SUMMARY": fetch_summary,
    "FAILURES": fetch_failures,
}


def _timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_bronze_objects(cursor) -> None:
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {SF_DATABASE}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {SF_DATABASE}.{SCHEMA_BRONZE}")
    cursor.execute(f"""
        CREATE STAGE IF NOT EXISTS {STAGE_NAME}
        FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
        COMMENT = 'Internal stage for FDIC raw JSON files'
    """)
    for table in ENDPOINTS:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {SF_DATABASE}.{SCHEMA_BRONZE}.{table} (
                raw_data   VARIANT,
                source_file VARCHAR,
                loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)


def stage_and_copy(cursor, endpoint: str, records: list[dict]) -> None:
    """Write JSON to a temp file, PUT to stage, then COPY INTO the bronze table."""
    filename = f"{endpoint.lower()}_{_timestamp()}.json"

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(records, f)
        tmp_path = f.name

    try:
        put_path = tmp_path.replace("\\", "/")
        cursor.execute(f"PUT 'file:///{put_path}' @{STAGE_NAME}/{endpoint.lower()}/ AUTO_COMPRESS=TRUE OVERWRITE=TRUE")
        print(f"  PUT {len(records)} records -> stage/{endpoint.lower()}/{filename}")

        table = f"{SF_DATABASE}.{SCHEMA_BRONZE}.{endpoint}"
        cursor.execute(f"""
            COPY INTO {table} (raw_data, source_file, loaded_at)
            FROM (
                SELECT
                    $1,
                    METADATA$FILENAME,
                    CURRENT_TIMESTAMP()
                FROM @{STAGE_NAME}/{endpoint.lower()}/
            )
            FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = TRUE)
            ON_ERROR = 'ABORT_STATEMENT'
            PURGE = FALSE
        """)
        print(f"  COPY INTO {table} complete")
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def main() -> None:
    conn = snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        password=SF_PASSWORD,
        warehouse=SF_WAREHOUSE,
        database=SF_DATABASE,
        role=SF_ROLE,
    )

    try:
        with conn.cursor() as cursor:
            ensure_bronze_objects(cursor)

            for endpoint, fetch_fn in ENDPOINTS.items():
                print(f"\nFetching {endpoint}...")
                records = fetch_fn()
                print(f"  Retrieved {len(records)} records")
                stage_and_copy(cursor, endpoint, records)

        conn.commit()
    finally:
        conn.close()

    print("\nSnowflake bronze load complete.")


if __name__ == "__main__":
    main()
