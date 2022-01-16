select
    json_extract_scalar(event, "$.account_id") as account_id
    , json_extract_scalar(event, "$.time") as occurred_at
    , json_extract_scalar(event, "$.click_link") as click_link
    , json_extract_scalar(event, "$.page") as page_view
    , json_extract_scalar(event, "$.user_agent") as user_agent
from
    {{ source('exlytics', 'raw_events') }}
