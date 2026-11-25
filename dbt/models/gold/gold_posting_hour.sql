-- Gold: what hour (UTC) front-page stories were posted, and their average peak score.
select
    extract(hour from posted_at)::int     as posted_hour_utc,
    count(*)                              as stories,
    round(avg(peak_score)::numeric, 1)    as avg_peak_score
from {{ ref('gold_story_latest') }}
where posted_at is not null
group by 1
order by 1
