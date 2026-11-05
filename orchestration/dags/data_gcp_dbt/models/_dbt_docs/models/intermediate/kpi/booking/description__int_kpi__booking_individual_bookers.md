---
title: Individual bookers KPI
description: Description of the `int_kpi__booking_individual_bookers` table.
---

{% docs description__int_kpi__booking_individual_bookers %}
New bookers by month, **beneficiary** geography and offer category, since January 2022 (start of the external
reporting history). A beneficiary is a new booker on the month of his first used booking since 2022
(`total_new_bookers`, on the row of the category of that booking), and a new category booker on the month of his
first used booking in each category (`total_new_category_bookers`).

Distinct bookers do not add up across territories or months, new bookers do: a beneficiary has a single
geography and is new once. The distinct bookers of any territory from January 2022 to a given month are the sum
of its new bookers over these months, so distinct counts are available at every level without a row per user.

Built from [`mrt_global__booking`](#!/model/model.data_gcp_dbt.mrt_global__booking) (used bookings) joined to
[`mrt_global__user_beneficiary`](#!/model/model.data_gcp_dbt.mrt_global__user_beneficiary): bookings of users
missing from it (deleted, suspended, GRANT_FREE) are excluded.
{% enddocs %}
