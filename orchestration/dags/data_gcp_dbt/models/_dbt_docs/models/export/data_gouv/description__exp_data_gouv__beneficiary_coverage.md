---
title: Beneficiary coverage numerator (data.gouv.fr)
description: Description of the `exp_data_gouv__beneficiary_coverage` table, published on data.gouv.fr.
---

{% docs description__exp_data_gouv__beneficiary_coverage %}

# Table: Beneficiary coverage numerator (data.gouv.fr)

The `exp_data_gouv__beneficiary_coverage` table is the open data version of [exp_vidoc_beneficiary_coverage](../vidoc/description__exp_vidoc_beneficiary__coverage.md): number of beneficiaries who reached a milestone age over the last 12 months, by department of residence.

{% enddocs %}

## Table description

Published on data.gouv.fr as the `beneficiary_coverage` resource. It is the numerator of the [coverage rate](../../../vidoc_kpis/beneficiaries/beneficiary_coverage_rate.md). The denominator (INSEE population estimates) is **not** republished: it is INSEE data, and its computation is public in [metrics_population__coverage](https://github.com/pass-culture/data-gcp/tree/master/orchestration/dags/data_gcp_dbt/models/metrics/population).

Same perturbed counts as the vidoc export, closed months only, geographic codes formatted as in [exp_data_gouv_beneficiary](description__exp_data_gouv__beneficiary.md).

**Grain**: `partition_month`, `department_name`, `milestone_age`.

{% docs table__exp_data_gouv__beneficiary_coverage %}{% enddocs %}
