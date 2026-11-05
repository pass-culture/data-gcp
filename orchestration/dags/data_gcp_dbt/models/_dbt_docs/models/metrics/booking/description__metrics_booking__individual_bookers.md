---
title: Individual bookers metrics
description: Key indicators from the `metrics_booking__individual_bookers` model.
---

{% docs description__metrics_booking__individual_bookers %}

The `metrics_booking__individual_bookers` model provides the new bookers (first used booking since January 2022)
by month, **beneficiary** geography and offer category. Summed over months, they give the distinct bookers of
any territory since 2022. See
[`int_kpi__booking_individual_bookers`](#!/model/model.data_gcp_dbt.int_kpi__booking_individual_bookers) for
the rules.

{% enddocs %}

## Table Description

Each row represents the new bookers of a month for a beneficiary municipality and an offer category.

{% docs table__metrics_booking__individual_bookers %}{% enddocs %}
