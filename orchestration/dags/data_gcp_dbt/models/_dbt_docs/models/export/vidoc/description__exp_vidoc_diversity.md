---
title: Beneficiary diversity metrics for vidoc export
description: Key indicators from the `exp_vidoc_diversity` model.
---

{% docs description__exp_vidoc_diversity %}

The `exp_vidoc_diversity` model aggregates key performance indicators to quantify diversity within beneficiary bookings on the pass Culture app.
It is designed to be exported to ministry for vidoc visualisation.

{% enddocs %}

## Table Description

Each row represents a key indicator calculated for a specific month, geographic aggregation and beneficiary dimensions level.

**Grain** (natural key, one row per combination): `deposit_expiration_month`, `region_code`, `department_code`, `is_in_qpv`, `macro_density_label`, `micro_density_label`.

{% docs table__exp_vidoc__diversity %}{% enddocs %}
