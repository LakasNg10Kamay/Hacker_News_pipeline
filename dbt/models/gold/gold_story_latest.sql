-- Gold: the latest version of each story, plus how it performed across all snapshots.
with ranked as (
    select
        *,
        row_number() over (partition by item_id order by snapshot_ts desc) as rn
    from {{ ref('silver_stories') }}
),

history as (
    select
        item_id,
        min(rank)  as best_rank,
        count(*)   as snapshots_seen,
        max(score) as peak_score
    from {{ ref('silver_stories') }}
    group by item_id
)

select
    r.item_id,
    r.snapshot_ts as last_seen_at,
    r.rank        as latest_rank,
    r.title,
    r.url,
    r.domain,
    r.author,
    r.score       as latest_score,
    r.comment_count,
    r.posted_at,
    h.best_rank,
    h.snapshots_seen,
    h.peak_score
from ranked r
join history h using (item_id)
where r.rn = 1
