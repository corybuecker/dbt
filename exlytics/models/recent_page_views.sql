select
    *
from
    {{ ref('page_views') }}
where
    occurred_at >= timestamp_sub(current_timestamp(), interval 31 day)
order by
    occurred_at