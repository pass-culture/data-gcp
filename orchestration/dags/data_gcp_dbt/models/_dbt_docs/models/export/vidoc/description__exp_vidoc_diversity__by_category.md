---
title: Beneficiary diversity metrics by category for vidoc export
description: Key indicators from the `exp_vidoc_diversity__by_category` model.
---

{% docs description__exp_vidoc_diversity__by_category %}

The `exp_vidoc_diversity__by_category` model aggregates key performance indicators to quantify diversity within beneficiary bookings on the pass Culture app, for each offer categories.
It is designed to be exported to ministry for vidoc visualisation.

{% enddocs %}

## Table Description

Each row represents a key indicator calculated for a specific month, geographic aggregation and beneficiary dimensions level.

**Grain**: `deposit_expiration_month`, `region_code`, `department_code`, `is_in_qpv`, `macro_density_label`, `micro_density_label`, `offer_category_id`.

> Rows are **only emitted** for `(cell, category)` pairs where at least one booking exists. Cells with zero bookings for a given category are **absent** from the table — do not compute rates by joining this table on geographic dimensions to `exp_vidoc__diversity`, or you will drop cells from the denominator. See the [Booking share by category](../../../vidoc_kpis/booking_share_by_category.md) KPI doc for the correct aggregation pattern.

{% docs table__exp_vidoc__diversity_by_category %}{% enddocs %}
