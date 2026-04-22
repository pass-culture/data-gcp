---
title: Population coverage metrics for vidoc export
description: Key indicators from the `vidoc_export_population__coverage` model.
---

{% docs description__exp_vidoc_population__coverage %}

The `exp_vidoc__population_coverage` model aggregates key performance indicators to quantify population coverage at department level.
It is designed to be exported to ministry for vidoc visualisation.

{% enddocs %}

## Table Description

Each row represents a key indicator calculated for a specific month and a geographic aggregation level.

**Grain**: `partition_month`, `birth_month`, `milestone_age`, `department_code`.

Public INSEE population data — **not perturbed**. `total_population_last_12_months` uses a 12-month rolling window matching `exp_vidoc__beneficiary_coverage`.

{% docs table__exp_vidoc__population_coverage %}{% enddocs %}
