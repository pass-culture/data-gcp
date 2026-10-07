---
title: Cultural Partner Activation Cohorts
description: Description of the `int_kpi__cultural_partner_activation_cohorts` table.
---

{% docs description__int_kpi__cultural_partner_activation_cohorts %}
Cohort-based activation metrics for cultural partner offerers, grouped by registration month and first-venue geography.

Each row represents a cohort of offerers created in a given `partition_month` (= offerer registration month), aggregated by the geography of their first registered venue.
Activation is measured within 30 days of registration across four dimensions: any offer, individual offer, collective offer, and consultation or Adage DMS application.

The model is built from [`mrt_global__cultural_partner`](#!/model/model.data_gcp_dbt.mrt_global__cultural_partner), [`mrt_global__offerer`](#!/model/model.data_gcp_dbt.mrt_global__offerer), and [`int_firebase__native_consultation`](#!/model/model.data_gcp_dbt.int_firebase__native_consultation).
{% enddocs %}
