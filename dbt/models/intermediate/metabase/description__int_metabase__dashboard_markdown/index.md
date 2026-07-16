Aggregates the in-dashboard markdown text widgets (`visualization_settings.text` of card-less dashboard cards) into one markdown blob per dashboard. Populated once `raw.metabase_report_dashboard_card` is replicated.

## Table description

| name               | data_type | description                                                             |
| ------------------ | --------- | ----------------------------------------------------------------------- |
| dashboard_id       |           | Metabase dashboard id.                                                  |
| dashboard_markdown |           | Concatenated markdown of the dashboard's text widgets, in layout order. |
