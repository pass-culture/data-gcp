---
title: Backend Feature Health
description: Description of the `mrt_monitoring__backend_feature_health` table.
---

{% docs description__mrt_monitoring__backend_feature_health %}

# Table: Backend Feature Health

Daily count of successful and failed backend actions, to monitor the error rate of backend features (e.g. bookings and booking cancellations from the app) in Metabase.

Only features reporting failures are kept: an origin / feature / action is monitored as soon as it has logged at least one failure since 2026-09-17, so new features are picked up automatically. Plain event logs (never failing) are excluded.

Each row is a combination of date, origin, feature, action, technical_message_id, severity, exception_class and provider_id.

Error rate must be computed from the counts, never averaged: `sum(error_count) / sum(total_count)`.

{% enddocs %}

## Table description

{% docs table__mrt_monitoring__backend_feature_health %}{% enddocs %}
