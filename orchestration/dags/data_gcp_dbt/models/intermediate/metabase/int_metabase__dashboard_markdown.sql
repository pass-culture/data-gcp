-- Aggregate the in-dashboard markdown text widgets for each dashboard.
-- Text widgets are dashboard cards with no underlying question (`card_id is null`)
-- whose markdown content lives in `visualization_settings.text`.
-- Source `metabase_report_dashboardcard` is populated by the import_metabase DAG;
-- until it lands this model simply returns no rows (non-blocking).
select
    dashboard_id,
    string_agg(
        json_value(visualization_settings, '$.text'),
        '\n\n'
        order by row_position, col_position
    ) as dashboard_markdown
from {{ source("raw", "metabase_report_dashboardcard") }}
where card_id is null and json_value(visualization_settings, '$.text') is not null
group by dashboard_id
