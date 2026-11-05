---
title: Free individual offer metrics
description: Key indicators from the `metrics_offer__individual_free` model.
---

{% docs description__metrics_offer__individual_free %}

The `metrics_offer__individual_free` model provides the free individual offers (last stock price 0) by creation
month and **venue** geography, monthly and cumulative since the first free offer of the territory. See
[`int_kpi__offer_individual_free`](#!/model/model.data_gcp_dbt.int_kpi__offer_individual_free) for the rules.

{% enddocs %}

## Table Description

Each row represents the free offers of a month for a venue municipality.

{% docs table__metrics_offer__individual_free %}{% enddocs %}
