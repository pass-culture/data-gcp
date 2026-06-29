-- Text widgets are dashboard cards with no question (card_id is null); their
-- markdown lives in visualization_settings.text.
select
    dashboard_id,
    string_agg(
        json_value(visualization_settings, '$.text'),
        '\n\n'
        order by row_position, col_position
    ) as dashboard_markdown
from {{ source("raw", "metabase_report_dashboard_card") }}
where card_id is null and json_value(visualization_settings, '$.text') is not null
group by dashboard_id
