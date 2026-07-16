---
title: EAC Offer & Booking KPI
description: Description of the `int_kpi__eac_offer_booking` table.
---

{% docs description__int_kpi__eac_offer_booking %}
KPI table aggregating key indicators related to collective (EAC) offers and bookings.

It combines:
- collective offers created, sourced from [`int_global__collective_offer`](#!/model/model.data_gcp_dbt.int_global__collective_offer)
- confirmed collective bookings, sourced from [`mrt_global__collective_booking`](#!/model/model.data_gcp_dbt.mrt_global__collective_booking)

Each row represents a combination of `partition_month`, venue geography (region, academy, department, EPCI, city, density)
and institution geography (academy, department, EPCI, city, density).

The three key metrics exposed are:
- `total_created_collective_offers`: number of collective offers created during the month
- `total_collective_bookings`: number of confirmed collective bookings created during the month (statuses: CONFIRMED, USED, PENDING_REIMBURSEMENT, REIMBURSED)
- `total_booking_amount`: total amount engaged by confirmed bookings during the month
{% enddocs %}
