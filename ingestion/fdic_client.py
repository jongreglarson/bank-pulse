"""
FDIC BankFind Suite API client with pagination.
Endpoints: institutions, history, summary, failures
"""

import time
import requests

BASE_URL = "https://banks.data.fdic.gov/api"
DEFAULT_PAGE_SIZE = 1000
DEFAULT_DELAY = 0.2  # seconds between pages to be polite


def fetch_endpoint(endpoint: str, fields: list[str], filters: str = "", delay: float = DEFAULT_DELAY, max_records: int = 0) -> list[dict]:
    """Pull records from a FDIC endpoint, handling pagination automatically.
    Pass max_records > 0 to cap how many rows are fetched."""
    offset = 0
    results = []

    while True:
        limit = DEFAULT_PAGE_SIZE
        if max_records > 0:
            limit = min(DEFAULT_PAGE_SIZE, max_records - len(results))
            if limit <= 0:
                break

        params = {
            "fields": ",".join(fields),
            "limit": limit,
            "offset": offset,
            "output": "json",
        }
        if filters:
            params["filters"] = filters

        response = requests.get(f"{BASE_URL}/{endpoint}", params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        page = data.get("data", [])
        results.extend(page)

        total = data.get("meta", {}).get("total", 0)
        offset += len(page)

        if offset >= total or not page:
            break

        time.sleep(delay)

    return results


def fetch_institutions() -> list[dict]:
    fields = ["cert", "name", "city", "stname", "asset", "dep", "netinc", "repdte", "active"]
    return fetch_endpoint("institutions", fields)


def fetch_history() -> list[dict]:
    fields = ["cert", "instname", "class", "pcity", "pstalp", "procdate", "action"]
    return fetch_endpoint("history", fields)


def fetch_summary() -> list[dict]:
    fields = ["repdte", "asset", "dep", "intinc", "nonii", "netinc", "lnlsnet"]
    return fetch_endpoint("summary", fields)


def fetch_failures() -> list[dict]:
    fields = ["cert", "name", "faildate", "savr", "restype", "cost", "qbfdep", "asset"]
    return fetch_endpoint("failures", fields)


def fetch_financials(max_records: int = 50000) -> list[dict]:
    fields = ["REPDTE", "CERT", "INTINC", "EINTEXP", "NIEXP", "NETINC", "ASSET", "DEP", "LNLSNET", "NPERFV", "RBC1RWAJ", "ROA", "ROE"]
    return fetch_endpoint("financials", fields, delay=0.5, max_records=max_records)
