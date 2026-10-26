---
title: Collective booking metrics
description: Key indicators from the `metrics_booking__collective` model.
---

{% docs description__metrics_booking__collective %}

The `metrics_booking__collective` model provides collective booking indicators by month, school year and
**venue** geography (pre-reservations, confirmed bookings, confirmations, actions; monthly and school year to date).
It is the central model for collective booking indicators by cultural partner location, for export, monitoring
and governance. See [`int_kpi__booking_collective`](#!/model/model.data_gcp_dbt.int_kpi__booking_collective) for the rules.

{% enddocs %}

## Table Description

Each row represents the key indicators related to collective bookings, calculated for a specific month, school year and venue geographic level (department / region / city / EPCI).

{% docs table__metrics_booking__collective %}{% enddocs %}
