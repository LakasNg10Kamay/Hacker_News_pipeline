-- Silver: parse the raw JSON into clean columns, one row per story per snapshot.
with source as (
    select item_id, snapshot_ts, rank, payload
    from {{ source('bronze', 'raw_items') }}
)

select
    item_id || '-' || to_char(snapshot_ts, 'YYYYMMDD"T"HH24MISS')          as story_snapshot_id,
    item_id,
    snapshot_ts,
    rank,
    payload ->> 'title'                                                     as title,
    payload ->> 'url'                                                       as url,
    -- Website from the URL; posts with no URL (Ask HN, etc.) belong to Hacker News itself
    coalesce(
        nullif(substring(payload ->> 'url' from '^https?://(?:www\.)?([^/:?#]+)'), ''),
        'news.ycombinator.com'
    )                                                                       as domain,
    payload ->> 'by'                                                        as author,
    coalesce((payload ->> 'score')::int, 0)                                 as score,
    coalesce((payload ->> 'descendants')::int, 0)                           as comment_count,
    to_timestamp((payload ->> 'time')::bigint) at time zone 'UTC'           as posted_at
from source
where payload ->> 'type' = 'story'
  and payload ->> 'deleted' is null
  and payload ->> 'dead' is null
  and payload ->> 'title' is not null
