---
title: Collective booking KPI by domain
description: Description of the `int_kpi__booking_collective_by_domain` table.
---

{% docs description__int_kpi__booking_collective_by_domain %}
KPI table aggregating collective bookings by month, educational domain and institution geography.

Each row represents a `partition_month` / `domain_name` / institution geography combination.
The model is built from [`mrt_global__collective_booking`](#!/model/model.data_gcp_dbt.mrt_global__collective_booking)
joined with [`mrt_global__collective_offer_domain`](#!/model/model.data_gcp_dbt.mrt_global__collective_offer_domain)
and keeps only confirmed booking statuses.
It exposes monthly metrics (`total_collective_bookings_by_domain`, `total_collective_booking_amount_by_domain`, `total_collective_tickets_by_domain`, `total_collective_institutions_by_domain`)
and cumulative metrics over the full history (`cumulative_total_collective_bookings_by_domain`, `cumulative_total_collective_booking_amount_by_domain`, `cumulative_total_collective_tickets_by_domain`, `cumulative_total_collective_institutions_by_domain`).
{% enddocs %}
