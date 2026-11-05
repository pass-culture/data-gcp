---
title: Individual booking KPI
description: Description of the `int_kpi__booking_individual` table.
---

{% docs description__int_kpi__booking_individual %}
KPI table of individual bookings by month, **beneficiary** geography (where the user lives: `region_name`,
`department_name`, `epci_code`, `municipality_code`), **venue** geography (where the cultural partner is:
`venue_*`) and offer category. Both geographies are on the row, so the bookings a territory keeps (beneficiary
and venue in the same territory) can be computed at any level.

The model is built from [`mrt_global__booking`](#!/model/model.data_gcp_dbt.mrt_global__booking), joined to
[`mrt_global__user_beneficiary`](#!/model/model.data_gcp_dbt.mrt_global__user_beneficiary) for the beneficiary
geography. Each booking is counted on two dates:

- use: `total_used_bookings` and `total_used_amount`, used bookings on the month of use;
- creation: `total_created_bookings` and `total_created_free_bookings`, all bookings in any state (cancelled
  included) on the month of creation.

Bookings of users missing from `mrt_global__user_beneficiary` (deleted, suspended, GRANT_FREE) are kept with
`user_is_beneficiary` false and no beneficiary geography: filter on `user_is_beneficiary` for beneficiary
KPIs.

All columns are monthly flows: sum them over months and geographies.
{% enddocs %}
