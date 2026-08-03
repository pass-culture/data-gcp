---
title: Collective offer KPI
description: Description of the `int_kpi__offer_collective` table.
---

{% docs description__int_kpi__offer_collective %}
KPI table aggregating created collective offers by month and venue geography.

Each row represents a `partition_month` / venue geography combination.
The model is built from [`int_global__collective_offer`](#!/model/model.data_gcp_dbt.int_global__collective_offer).
{% enddocs %}
