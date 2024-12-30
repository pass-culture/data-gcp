SELECT
    id AS zendesk_macro_id,
    raw_title AS zendesk_macro_title,
    usage_24h AS total_zendesk_macro_usage,
    DATE_SUB(export_date, INTERVAL 1 DAY) AS zendesk_macro_usage_date
FROM {{ source("raw", "zendesk_macro_usage") }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id, export_date ORDER BY usage_24h DESC) = 1
