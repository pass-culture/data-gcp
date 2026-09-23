---
title: Bonus Application Logs
description: Description of the `int_pcapi__bonus_log` table.
---

{% docs description__int_pcapi__bonus_log %}

# Table: Bonus Application Logs

This model parses, flattens, and normalizes JSON payloads from backend API logs related to youth bonus credit requests (`technical_message_id = 'bonus_credit.statistics.counters'`).

It transforms nested JSON counters into an unpivoted event model, breaking down requests into three main statuses:
- **Grants**: Successful bonus allocations per bonus type.
- **Errors**: Rejections and error reasons per bonus type.
- **Attempts**: Distribution of the number of attempts required by users before obtaining a grant.

Each row represents an aggregated metrics period defined by a start timestamp (`log_started_at`) and an end/publication timestamp (`log_ended_at`).

{% enddocs %}

## Table description

{% docs table__int_pcapi__bonus_log %}{% enddocs %}
