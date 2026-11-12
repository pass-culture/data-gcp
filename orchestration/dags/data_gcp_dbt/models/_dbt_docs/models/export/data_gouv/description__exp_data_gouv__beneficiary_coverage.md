---
title: Beneficiary coverage numerator (data.gouv.fr)
description: Description of the `exp_data_gouv__beneficiary_coverage` table, published on data.gouv.fr.
---

{% docs description__exp_data_gouv__beneficiary_coverage %}

# Table: Beneficiary coverage numerator (data.gouv.fr)

The `exp_data_gouv__beneficiary_coverage` table is the number of beneficiaries who reached a milestone age over the last 12 months, by department of residence. Milestone ages 16 to 19 give the coverage rates 15+ to 18+.

{% enddocs %}

## Table description

Published on data.gouv.fr as the `beneficiary_coverage` resource. It is the numerator of the [coverage rate](../../../kpis/beneficiaries/beneficiary_coverage_rate.md). The denominator (INSEE population estimates) is **not** republished: it is INSEE data, and its computation is public in [metrics_population__coverage](https://github.com/pass-culture/data-gcp/tree/master/orchestration/dags/data_gcp_dbt/models/metrics/population).

Counts come from the perturbed base [exp_base__beneficiary_coverage](../export_base/description__exp_base__beneficiary_coverage.md), closed months only, geographic codes formatted as in [exp_data_gouv_beneficiary](description__exp_data_gouv__beneficiary.md).

**Grain**: `partition_month`, `department_name`, `milestone_age`.

{% docs table__exp_data_gouv__beneficiary_coverage %}{% enddocs %}
