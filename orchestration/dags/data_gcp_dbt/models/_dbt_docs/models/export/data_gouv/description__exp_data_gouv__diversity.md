---
title: Diversity at credit expiration (data.gouv.fr)
description: Description of the `exp_data_gouv__diversity` table, published on data.gouv.fr.
---

{% docs description__exp_data_gouv__diversity %}

# Table: Diversity at credit expiration (data.gouv.fr)

The `exp_data_gouv__diversity` table counts the beneficiaries whose credit expired in the month, and those among them who booked in at least 3 distinct offer categories.

{% enddocs %}

## Table description

Published on data.gouv.fr as the `diversity` resource. Only the 3+ categories indicator and its denominator are published. Counts come from the perturbed base [exp_base__diversity](../export_base/description__exp_base__diversity.md), closed months only, geographic codes formatted as in [exp_data_gouv_beneficiary](description__exp_data_gouv__beneficiary.md).

**Grain**: `deposit_expiration_month`, `department_name`, `is_in_qpv`, `macro_density_label`, `micro_density_label`.

KPI definition: [Diversity rate (3+ categories)](../../../kpis/diversity/diversity_3plus_categories.md).

{% docs table__exp_data_gouv__diversity %}{% enddocs %}
