---
title: Collective booking KPI
description: Description of the `int_kpi__booking_collective` table.
---

{% docs description__int_kpi__booking_collective %}
KPI table aggregating collective bookings by month and venue geography.

Each row represents a `partition_month` / `scholar_year` / venue geography combination.
The model is built from [`mrt_global__collective_booking`](#!/model/model.data_gcp_dbt.mrt_global__collective_booking)
and keeps only confirmed booking statuses.
It exposes monthly metrics (`total_collective_bookings`, `total_collective_amount_spent`)
and cumulative metrics over the scholar year (`cumulative_total_collective_bookings`, `cumulative_total_collective_amount_spent`).
{% enddocs %}
