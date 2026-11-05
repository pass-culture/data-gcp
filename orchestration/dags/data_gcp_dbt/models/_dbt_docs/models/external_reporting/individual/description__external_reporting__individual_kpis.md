---
title: External reporting individual KPI rows
description: Description of the `external_reporting__individual_kpis` model.
---

{% docs description__external_reporting__individual_kpis %}

The `external_reporting__individual_kpis` model is the union of the KPI segments of `external_reporting__individual`, before the statistical secret, zero rows, national values and labels are applied. It is materialized as a table so that `external_reporting__individual`, which reads it several times, stays simple to plan.

{% enddocs %}
