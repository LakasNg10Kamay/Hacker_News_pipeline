-- Gold: which websites reach the front page most, and how well their stories do.
select
    domain,
    count(*)                              as stories,
    round(avg(peak_score)::numeric, 1)    as avg_peak_score,
    sum(comment_count)                    as total_comments
from {{ ref('gold_story_latest') }}
group by domain
order by stories desc, avg_peak_score desc
