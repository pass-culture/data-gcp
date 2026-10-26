---
title: Bookable collective offer metrics
description: Description of the `metrics_offer__collective_bookable` table.
---

{% docs description__metrics_offer__collective_bookable %}
Bookable (non template) collective offers by month, scholar year and **venue** geography (location of the cultural partner), for the cultural partners of a territory.

Thin wrapper over [`int_kpi__offer_collective_bookable`](#!/model/model.data_gcp_dbt.int_kpi__offer_collective_bookable). An offer
belongs to the scholar year of its action (stock beginning date). Its creation month is clamped into that
scholar year: an offer created in June for an action in October is counted in September. Every month of the
scholar year has a row for every geography seen in that scholar year.

Monthly columns are flows; `cumulative_*` columns are scholar year to date. Sum monthly columns over months,
never `cumulative_*` columns. Distinct partners are only available here, by venue geography.
{% enddocs %}
