One row per Metabase dashboard exposing its filter widgets (parsed from `raw.metabase_report_dashboard.parameters`) as a typed array of `{name, slug, type}`. The `slug` is the URL key Metabase uses to deep-link a filter (`/dashboard/<id>?<slug>=<value>`). Empty array when the dashboard has no filters.

## Table description

| name                 | data_type | description                                                                                                                                                                           |
| -------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| dashboard_id         |           | Metabase dashboard id.                                                                                                                                                                |
| dashboard_parameters |           | Dashboard filter widgets as an array of {name, slug, type}; `slug` is the URL key for deep-linking (`/dashboard/<id>?<slug>=<value>`). Empty array when the dashboard has no filters. |
