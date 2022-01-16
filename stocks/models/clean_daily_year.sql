select
    *
from
    {{ ref('clean_daily') }}
where
    date >= date_sub(current_date(), interval 1 year)
order by
    date
