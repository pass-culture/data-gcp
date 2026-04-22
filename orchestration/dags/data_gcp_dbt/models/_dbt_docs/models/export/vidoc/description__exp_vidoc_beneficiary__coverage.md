---
title: Beneficiary coverage metrics for vidoc export
description: Key indicators from the `exp_vidoc_beneficiary__coverage` model.
---

{% docs description__exp_vidoc_beneficiary__coverage %}

The `exp_vidoc_beneficiary__coverage` model provides aggregated key indicators related to pass Culture beneficary coverage.
It is designed to be exported to ministry for vidoc visualisation.

{% enddocs %}

## Table Description

Each row represents a key indicator calculated for a specific month, geographic aggregation and beneficiary dimensions level.

**Grain**: `partition_month`, `region_code`, `department_code`, `milestone_age`.

`total_beneficiaries_last_12_months` is a **12-month rolling sum of monthly distinct counts** — a user active across all 12 months contributes 12 times. Semantic is person-months, not "distinct users over the last year".

{% docs table__exp_vidoc__beneficiary_coverage %}{% enddocs %}
