select
    id,
    created_at,
    updated_at,
    dashboard_id,
    card_id,
    "row" as row_position,
    col as col_position,
    size_x,
    size_y,
    visualization_settings

from public.report_dashboardcard
