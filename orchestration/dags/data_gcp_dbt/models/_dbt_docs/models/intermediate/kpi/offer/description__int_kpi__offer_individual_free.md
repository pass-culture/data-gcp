---
title: Free individual offer KPI
description: Description of the `int_kpi__offer_individual_free` table.
---

{% docs description__int_kpi__offer_individual_free %}
Free individual offers (last stock price 0) by creation month and **venue** geography (location of the cultural
partner), built from [`mrt_global__offer`](#!/model/model.data_gcp_dbt.mrt_global__offer). Offers in any state
are counted, and the bookable ones (bookable today) separately.

Every month from the first free offer of the territory to the current month has a row, so `cumulative_*`
columns (stock created since the start) can be read on any month and summed across territories. Monthly columns
are flows; sum monthly columns over months, never `cumulative_*` columns.
{% enddocs %}
