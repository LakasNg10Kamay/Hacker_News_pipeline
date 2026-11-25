"""Export: write this run's silver rows (built by dbt in Postgres) to a Parquet file for Databricks."""
import os
from datetime import datetime
from pathlib import Path

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq

# Same columns and types as the earlier Parquet files, so Databricks reads old and new files together
SCHEMA = pa.schema([
    ("item_id", pa.int64()),
    ("snapshot_ts", pa.timestamp("us")),
    ("rank", pa.int32()),
    ("title", pa.string()),
    ("url", pa.string()),
    ("domain", pa.string()),
    ("author", pa.string()),
    ("score", pa.int32()),
    ("comment_count", pa.int32()),
    ("posted_at", pa.timestamp("us")),
])

QUERY = f"""
    SELECT {", ".join(SCHEMA.names)}
    FROM silver.silver_stories
    WHERE snapshot_ts = %s
    ORDER BY rank
"""


def export_silver_parquet(snapshot: str) -> str:
    """Returns the path of the Parquet file written for this snapshot."""
    snapshot_ts = datetime.strptime(snapshot, "%Y-%m-%d %H:%M:%S")
    with psycopg2.connect(os.environ["HN_PG_DSN"]) as conn, conn.cursor() as cur:
        cur.execute(QUERY, (snapshot_ts,))
        rows = cur.fetchall()
    conn.close()
    if not rows:
        raise ValueError(f"No silver rows for snapshot {snapshot}; did dbt build run?")

    columns = list(zip(*rows))
    table = pa.Table.from_arrays(
        [pa.array(col, type=field.type) for col, field in zip(columns, SCHEMA)], schema=SCHEMA
    )

    out_dir = Path(os.environ.get("HN_DATA_DIR", "/opt/airflow/data")) / "output" / "silver"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"silver_stories_{snapshot_ts:%Y%m%dT%H%M%S}.parquet"
    pq.write_table(table, out_file)
    print(f"Exported {table.num_rows} silver rows to {out_file}")
    return str(out_file)
