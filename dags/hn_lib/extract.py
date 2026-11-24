"""Extract: pull the current Hacker News top stories and land them raw in Postgres (bronze)."""
import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import psycopg2
import requests
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API = "https://hacker-news.firebaseio.com/v0"


def _session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=10))
    return s


def extract_top_stories() -> str:
    """Fetch top-N stories and insert them as one snapshot. Returns the snapshot timestamp (UTC)."""
    top_n = int(os.environ.get("HN_TOP_N", "100"))
    snapshot = datetime.now(timezone.utc).replace(microsecond=0, tzinfo=None)
    session = _session()

    ids = session.get(f"{API}/topstories.json", timeout=15).json()[:top_n]

    def fetch(item_id):
        r = session.get(f"{API}/item/{item_id}.json", timeout=15)
        r.raise_for_status()
        return r.json()

    with ThreadPoolExecutor(max_workers=10) as pool:
        items = list(pool.map(fetch, ids))

    rows = [
        (item["id"], snapshot, rank, json.dumps(item))
        for rank, item in enumerate(items, start=1)
        if item  # the API returns null for some removed items
    ]
    if not rows:
        raise ValueError("Hacker News API returned no stories")

    # One transaction: if anything fails, nothing from this run is saved
    with psycopg2.connect(os.environ["HN_PG_DSN"]) as conn, conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO raw_items (item_id, snapshot_ts, rank, payload) VALUES %s "
            "ON CONFLICT (item_id, snapshot_ts) DO NOTHING",
            rows,
            template="(%s, %s, %s, %s::jsonb)",
        )
    conn.close()
    print(f"Loaded {len(rows)} stories for snapshot {snapshot}")
    return snapshot.strftime("%Y-%m-%d %H:%M:%S")
