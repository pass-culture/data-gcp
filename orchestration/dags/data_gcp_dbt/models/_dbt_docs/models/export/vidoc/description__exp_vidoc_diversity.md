---
title: Beneficiary diversity metrics for vidoc export
description: Key indicators from the `exp_vidoc_diversity` model.
---

{% docs description__exp_vidoc_diversity %}

The `exp_vidoc_diversity` model aggregates key performance indicators on beneficiaries whose credit expired: diversity of bookings (number of categories booked), share of credit spent, amount spent and partners booked.
It is designed to be exported to ministry for vidoc visualisation.

{% enddocs %}

## Table Description

Each row represents a key indicator calculated for a specific month, geographic aggregation and beneficiary dimensions level.

**Grain** (natural key, one row per combination): `deposit_expiration_month`, `region_code`, `department_code`, `is_in_qpv`, `macro_density_label`, `micro_density_label`.

> `total_Nplus_category_booked_beneficiaries` (N = 1 to 5) and `total_spent_share_*_beneficiaries` are capped at `total_expired_deposit_beneficiaries` of the same row, so their shares never exceed 100%.

> `total_actual_amount_spent_at_expiration` and `total_distinct_venue_booked_at_expiration` are not perturbed: they are null when `total_expired_deposit_beneficiaries` is below 5. Divide them by `total_expired_deposit_beneficiaries` to get the average amount spent and the average number of distinct partners booked at expiration.

{% docs table__exp_vidoc__diversity %}{% enddocs %}
