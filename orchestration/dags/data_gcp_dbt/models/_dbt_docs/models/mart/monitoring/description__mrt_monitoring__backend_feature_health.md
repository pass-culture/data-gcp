---
title: Backend Feature Health
description: Description of the `mrt_monitoring__backend_feature_health` table.
---

{% docs description__mrt_monitoring__backend_feature_health %}

# Table: Backend Feature Health

Daily count of successful and failed backend actions, to monitor the error rate of backend features (e.g. bookings and booking cancellations from the app) in Metabase.

All origin / feature / action logged since 2026-09-17 are kept, so new features are picked up automatically.

Each row is a combination of date, origin, feature, action, severity, exception_class and provider_id.

Counts are actions, not log lines: the lines of one request (trace) about the same booking are one action, which fails when any of its lines fails. A line without booking_id is attached to its request's booking when the request has exactly one; otherwise each line is an action (bulk actions log one line per stock or offer). `log_line_count` keeps the raw line count.

Error rate must be computed from the counts, never averaged: `sum(error_count) / sum(total_count)`.

{% enddocs %}

## Table description

{% docs table__mrt_monitoring__backend_feature_health %}{% enddocs %}
