---
title: External reporting eac KPI rows
description: Description of the `external_reporting__eac_kpis` model.
---

{% docs description__external_reporting__eac_kpis %}

The `external_reporting__eac_kpis` model is the union of the KPI segments of `external_reporting__eac`, before the statistical secret, zero rows, national values and labels are applied. It is materialized as a table so that `external_reporting__eac`, which reads it several times, stays simple to plan.

{% enddocs %}
