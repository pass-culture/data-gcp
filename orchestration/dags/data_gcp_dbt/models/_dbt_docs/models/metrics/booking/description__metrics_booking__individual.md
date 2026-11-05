---
title: Individual booking metrics
description: Key indicators from the `metrics_booking__individual` model.
---

{% docs description__metrics_booking__individual %}

The `metrics_booking__individual` model provides individual booking indicators by month, **beneficiary**
geography, **venue** geography and offer category: used bookings and amounts by month of use, all and free
bookings (cancelled included) by month of creation. It is the central model for individual booking indicators,
for export, monitoring and governance. See
[`int_kpi__booking_individual`](#!/model/model.data_gcp_dbt.int_kpi__booking_individual) for the rules.

{% enddocs %}

## Table Description

Each row represents the individual booking flows of a month for a beneficiary municipality, a venue municipality
and an offer category.

{% docs table__metrics_booking__individual %}{% enddocs %}
