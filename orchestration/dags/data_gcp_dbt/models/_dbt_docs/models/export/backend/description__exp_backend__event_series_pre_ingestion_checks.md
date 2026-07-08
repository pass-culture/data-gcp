---
title: Event Series Pre-Ingestion Checks
description: Description of the `exp_backend__event_series_pre_ingestion_checks` table.
---

{% docs description__exp_backend__event_series_pre_ingestion_checks %}

# Table: Event Series Pre-Ingestion Checks

The `exp_backend__event_series_pre_ingestion_checks` table aggregates data quality checks on the future event series and event series / offer link data before they are ingested by the backend application.
This table will be read by the backend application to determine if the data is ready for ingestion (i.e. all checks pass).
The `ready_for_ingestion` column is `true` when all checks pass (all counts equal zero).

The checks cover:

- **Uniqueness**: duplicate `event_series_id` in the future event series data, and duplicate `(offer_id, event_series_id)` pairs in the future links.
- **Null keys**: null or empty `event_series_id`, `offer_id`, or `event_series_name`.
- **Referential integrity**: future links pointing to an `event_series_id` that doesn't exist in the future event series data.
- **Delta validity**: rows in either delta with a null action or an action outside `add`, `update`, `remove`.
- **Business rules**: an offer linked to more than one event series; an added/updated event series with an image url but no extracted mediation uuid.
- **Shrinkage guard**: the future event series (or offer link) row count drops below 30% of the current applicative row count, which would indicate a pathological delta wiping out most of the table.

{% enddocs %}
