---
title: Beneficiary metrics for vidoc export
description: Key indicators from the `exp_vidoc_beneficiary` model.
---

{% docs description__exp_vidoc_beneficiary %}

The `exp_vidoc_beneficiary` model provides aggregated key indicators related to pass Culture beneficary.
It is designed to be exported to ministry for vidoc visualisation.

{% enddocs %}

## Table Description

Each row represents a key indicator calculated for a specific month, geographic aggregation and beneficiary dimensions level.

**Grain**: `partition_month`, `region_code`, `department_code`, `age_at_calculation`, `is_in_qpv`, `macro_density_label`, `micro_density_label`.

`total_actual_beneficiaries` is a subset of `total_beneficiaries` (active ⊂ ever-credited). The two are perturbed independently, so the perturbed `total_actual_beneficiaries` is capped at the perturbed `total_beneficiaries` of the same row: `total_actual ≤ total_beneficiaries` holds on every row and at any aggregation level.

{% docs table__exp_vidoc__beneficiary %}{% enddocs %}
