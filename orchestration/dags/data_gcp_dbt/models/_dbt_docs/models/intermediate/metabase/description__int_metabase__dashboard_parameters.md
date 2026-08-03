---
title: Metabase Dashboard Parameters
description: Description of the `int_metabase__dashboard_parameters` table.
---

{% docs description__int_metabase__dashboard_parameters %}
One row per Metabase dashboard exposing its filter widgets (parsed from
`raw.metabase_report_dashboard.parameters`) as a typed array of `{name, slug, type}`.
The `slug` is the URL key Metabase uses to deep-link a filter
(`/dashboard/<id>?<slug>=<value>`). Empty array when the dashboard has no filters.
{% enddocs %}

## Table description

{% docs table__int_metabase__dashboard_parameters %}{% enddocs %}
