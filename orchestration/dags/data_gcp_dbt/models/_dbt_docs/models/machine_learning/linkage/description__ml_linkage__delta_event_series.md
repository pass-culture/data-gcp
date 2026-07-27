---
title: Delta Event Series
description: Description of the `ml_linkage__delta_event_series` table.
---

{% docs description__ml_linkage__delta_event_series %}

# Table: Delta Event Series

The `ml_linkage__delta_event_series` table contains the new event series data that must be synchronized with the backend application.
It is an export from the ml_preproc__delta_event_series source computed by the event_linkage DAG.
`add` rows already present in the applicative database (already ingested by the backend) are excluded, so the table only carries changes still to be applied.

{% enddocs %}

## Table description

{% docs table__ml_linkage__delta_event_series %}{% enddocs %}
