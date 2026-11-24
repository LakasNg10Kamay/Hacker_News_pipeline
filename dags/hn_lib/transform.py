"""Transform: DuckDB reads bronze straight from Postgres, builds silver and gold, exports Parquet."""
import os
from pathlib import Path

import duckdb

SILVER_SQL = r"""
CREATE OR REPLACE TABLE silver_stories AS
WITH parsed AS (
    SELECT item_id, snapshot_ts, rank, payload::VARCHAR AS j
    FROM pg.public.raw_items
)
SELECT
    item_id,
    snapshot_ts,
    rank,
    json_extract_string(j, '$.title')                         AS title,
    json_extract_string(j, '$.url')                           AS url,
    coalesce(
        nullif(regexp_extract(json_extract_string(j, '$.url'),
                              '^https?://(?:www\.)?([^/:?#]+)', 1), ''),
        'news.ycombinator.com')                               AS domain,  -- Ask HN etc. have no URL
    json_extract_string(j, '$.by')                            AS author,
    coalesce(TRY_CAST(json_extract_string(j, '$.score') AS INTEGER), 0)       AS score,
    coalesce(TRY_CAST(json_extract_string(j, '$.descendants') AS INTEGER), 0) AS comment_count,
    epoch_ms(TRY_CAST(json_extract_string(j, '$.time') AS BIGINT) * 1000)     AS posted_at
FROM parsed
WHERE json_extract_string(j, '$.type') = 'story'
  AND json_extract_string(j, '$.deleted') IS NULL
  AND json_extract_string(j, '$.dead') IS NULL
  AND json_extract_string(j, '$.title') IS NOT NULL
"""

GOLD_SQL = """
CREATE OR REPLACE TABLE gold_story_latest AS
SELECT s.*, a.best_rank, a.snapshots_seen, a.peak_score
FROM silver_stories s
JOIN (
    SELECT item_id, min(rank) AS best_rank, count(*) AS snapshots_seen, max(score) AS peak_score
    FROM silver_stories GROUP BY item_id
) a USING (item_id)
QUALIFY row_number() OVER (PARTITION BY s.item_id ORDER BY s.snapshot_ts DESC) = 1;

CREATE OR REPLACE TABLE gold_domain_stats AS
SELECT domain,
       count(*)                  AS stories,
       round(avg(peak_score), 1) AS avg_peak_score,
       sum(comment_count)        AS total_comments
FROM gold_story_latest
GROUP BY domain
ORDER BY stories DESC, avg_peak_score DESC;
"""


def build(con: duckdb.DuckDBPyConnection, snapshot: str, out_dir: Path) -> Path:
    """Build silver + gold, run data quality checks, export this snapshot's silver rows to Parquet."""
    con.execute(SILVER_SQL)

    # Data quality checks on this run's rows
    n, null_ids, null_scores = con.execute(
        "SELECT count(*), count(*) FILTER (item_id IS NULL), count(*) FILTER (score IS NULL) "
        "FROM silver_stories WHERE snapshot_ts = CAST(? AS TIMESTAMP)", [snapshot]
    ).fetchone()
    if n == 0:
        raise ValueError(f"No silver rows for snapshot {snapshot}")
    if null_ids or null_scores:
        raise ValueError(f"Quality check failed: {null_ids} null ids, {null_scores} null scores")

    con.execute(GOLD_SQL)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"silver_stories_{snapshot.replace('-', '').replace(':', '').replace(' ', 'T')}.parquet"
    con.execute(
        f"COPY (SELECT * FROM silver_stories WHERE snapshot_ts = CAST('{snapshot}' AS TIMESTAMP)) "
        f"TO '{out_file}' (FORMAT parquet)"
    )
    print(f"Silver rows this run: {n}. Exported {out_file}")
    return out_file


def run_transform(snapshot: str) -> str:
    data_dir = Path(os.environ.get("HN_DATA_DIR", "/opt/airflow/data"))
    con = duckdb.connect(str(data_dir / "hn.duckdb"))
    try:
        con.execute("LOAD postgres")
        con.execute(f"ATTACH '{os.environ['HN_PG_DSN']}' AS pg (TYPE postgres, READ_ONLY)")
        return str(build(con, snapshot, data_dir / "output" / "silver"))
    finally:
        con.close()
