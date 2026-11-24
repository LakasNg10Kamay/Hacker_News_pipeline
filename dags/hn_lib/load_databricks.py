"""Load: upload the silver Parquet file to a Unity Catalog volume in Databricks Free Edition."""
import os
from pathlib import Path
from urllib.parse import urlparse

import requests

def _read_token() -> str:
    path = Path(os.environ.get("DATABRICKS_TOKEN_FILE", "/run/secrets/databricks_token"))
    if not path.is_file():
        return ""
    return path.read_text(encoding="utf-8-sig").strip()


def _clean_host(raw: str) -> str:
    parsed = urlparse(raw.strip())
    return f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else ""


def upload_to_volume(local_file: str) -> bool:
    """Returns False (and does nothing) when Databricks settings are not filled in."""
    host = _clean_host(os.environ.get("DATABRICKS_HOST", ""))
    token = _read_token()
    volume = os.environ.get("DATABRICKS_VOLUME_PATH", "/Volumes/workspace/hn/landing/silver").rstrip("/")
    if not host or not token:
        return False

    headers = {"Authorization": f"Bearer {token}"}
    target = f"{volume}/{Path(local_file).name}"
    with open(local_file, "rb") as f:
        r = requests.put(f"{host}/api/2.0/fs/files{target}", headers=headers,
                         params={"overwrite": "true"}, data=f, timeout=60)
    r.raise_for_status()
    print(f"Uploaded to {target}")

    job_id = os.environ.get("DATABRICKS_JOB_ID", "").strip()
    if job_id:
        r = requests.post(f"{host}/api/2.1/jobs/run-now", headers=headers,
                          json={"job_id": int(job_id)}, timeout=30)
        r.raise_for_status()
        print(f"Triggered Databricks job {job_id}: run {r.json().get('run_id')}")
    return True
