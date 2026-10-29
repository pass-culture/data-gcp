---
title: Collective booking KPI by format
description: Description of the `int_kpi__booking_collective_by_format` table.
---

{% docs description__int_kpi__booking_collective_by_format %}
KPI table aggregating collective bookings by month, offer format and institution geography.

Each row represents a `partition_month` / `collective_offer_format` / institution geography combination.
The model is built from [`mrt_global__collective_booking`](#!/model/model.data_gcp_dbt.mrt_global__collective_booking)
and keeps only confirmed booking statuses.
It exposes monthly metrics (`total_collective_bookings`, `total_collective_booking_amount`, `total_collective_tickets`, `total_collective_institutions`)
and cumulative metrics over the scholar year (`cumulative_total_collective_bookings`, `cumulative_total_collective_booking_amount`, `cumulative_total_collective_tickets`, `cumulative_total_collective_institutions`).

> **Warning**: formats are not summable across each other — a collective offer can have several formats. Aggregating across `collective_offer_format` will double-count bookings.
{% enddocs %}
