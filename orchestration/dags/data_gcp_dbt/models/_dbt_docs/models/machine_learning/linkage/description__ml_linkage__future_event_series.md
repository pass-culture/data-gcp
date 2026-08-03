---
title: Future Event Series
description: Description of the `ml_linkage__future_event_series` table.
---

{% docs description__ml_linkage__future_event_series %}

# Table: Future Event Series

The `ml_linkage__future_event_series` table contains a preview of the event series once ingestion of the delta event series is completed.

This table is used to validate that the future state of the event series is correct before synchronizing with the backend application by running dbt tests on it.

{% enddocs %}

## Table description

{% docs table__ml_linkage__future_event_series %}{% enddocs %}
