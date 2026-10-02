select
    id as zendesk_macro_id,
    raw_title as zendesk_macro_title,
    usage_24h as total_zendesk_macro_usage,
    date_sub(export_date, interval 1 day) as zendesk_macro_usage_date
from {{ source("raw_eu1", "zendesk_macro_usage") }}
qualify row_number() over (partition by id, export_date order by usage_24h desc) = 1
