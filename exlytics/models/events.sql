{{ config(
    materialized='table',
    partition_by={
        "field": "occurred_at",
        "data_type": "timestamp",
        "granularity": "day"
    }
) }}

select
    parse_timestamp('%FT%H:%M:%E*SZ', occurred_at, 'UTC') as occurred_at
    , page_view
    , click_link
    , user_agent
from
    {{ ref('exlytics', 'extracted_events') }}
where
    occurred_at is not null
