select
    *
from
    {{ ref('events') }}
where
    page_view is not null