---
title: Event Series Pre-Ingestion Checks
description: Description of the `exp_backend__event_series_pre_ingestion_checks` table.
---

{% docs description__exp_backend__event_series_pre_ingestion_checks %}

# Table: Event Series Pre-Ingestion Checks

The `exp_backend__event_series_pre_ingestion_checks` table aggregates data quality checks on the future event series and event series / offer link data before they are ingested by the backend application.
This table will be read by the backend application to determine if the data is ready for ingestion (i.e. all checks pass).
The `ready_for_ingestion` column is `true` when all checks pass (all counts equal zero).
{% enddocs %}
