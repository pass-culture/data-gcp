---
title: Diversity at credit expiration (data.gouv.fr)
description: Description of the `exp_data_gouv__diversity` table, published on data.gouv.fr.
---

{% docs description__exp_data_gouv__diversity %}

# Table: Diversity at credit expiration (data.gouv.fr)

The `exp_data_gouv__diversity` table is the open data version of [exp_vidoc_diversity](../vidoc/description__exp_vidoc_diversity.md), restricted to the lot 1 indicator: beneficiaries whose credit expired in the month, and those among them who booked in at least 3 distinct offer categories.

{% enddocs %}

## Table description

Published on data.gouv.fr as the `diversity` resource. Amounts, spent share buckets and 1+/2+/4+/5+ counts of the vidoc table are not published. Same perturbed counts as the vidoc export, closed months only, geographic codes formatted as in [exp_data_gouv_beneficiary](description__exp_data_gouv__beneficiary.md).

**Grain**: `deposit_expiration_month`, `department_name`, `is_in_qpv`, `macro_density_label`, `micro_density_label`.

KPI definition: [Diversity rate (3+ categories)](../../../vidoc_kpis/diversity/diversity_3plus_categories.md).

{% docs table__exp_data_gouv__diversity %}{% enddocs %}
