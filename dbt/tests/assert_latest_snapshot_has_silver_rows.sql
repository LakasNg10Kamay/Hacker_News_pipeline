-- Fails if the newest snapshot in bronze produced no rows in silver.
select max(snapshot_ts) as latest_snapshot
from {{ source('bronze', 'raw_items') }}
having (
    select count(*)
    from {{ ref('silver_stories') }}
    where snapshot_ts = (select max(snapshot_ts) from {{ source('bronze', 'raw_items') }})
) = 0
