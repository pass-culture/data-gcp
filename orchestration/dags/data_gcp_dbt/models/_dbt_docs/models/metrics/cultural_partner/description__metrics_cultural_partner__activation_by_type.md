---
title: Cultural Partners activation by type metrics
description: Key indicators from the `metrics_cultural_partner__activation_by_type` model.
---

{% docs description__metrics_cultural_partner__activation_by_type %}

The `metrics_cultural_partner__activation_by_type` model provides aggregated key indicators related to pass Culture cultural partner activations.
It is designed to be the central model providing cultural partners activation indicators for export, monitoring and gouvernance.

The model combines two complementary views:
- **Activity metrics** (columns without `_by_cohort`): `partition_month` represents the **observation month** — indicators measure how many partners were active or had been activated up to that month.
- **Cohort metrics** (columns suffixed `_by_cohort`): `partition_month` represents the **offerer registration month** — indicators measure how many offerers from that cohort activated within 30 days of their registration. Rows where cohort metrics are NULL indicate months with no matching cohort in `int_kpi__cultural_partner_activation_cohorts_by_type` (e.g. months before 2022-01-01).

{% enddocs %}

## Table Description

Each row represents key indicators related to cultural partner activation on the individual and collective parts of pass Culture, calculated for a specific month, a type of partner and geographic aggregation level. For cohort metrics, the month corresponds to the offerer registration month.
This model is created separately from the activation model because the different types of partner are not additive.

{% docs table__metrics_cultural_partner__activation_by_type %}{% enddocs %}
