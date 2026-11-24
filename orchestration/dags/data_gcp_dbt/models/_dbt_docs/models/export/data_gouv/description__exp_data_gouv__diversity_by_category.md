---
title: Diversity by offer category (data.gouv.fr)
description: Description of the `exp_data_gouv__diversity_by_category` table, published on data.gouv.fr.
---

{% docs description__exp_data_gouv__diversity_by_category %}

# Table: Diversity by offer category (data.gouv.fr)

The `exp_data_gouv__diversity_by_category` table gives, for each offer category, beneficiaries whose credit expired in the month and who booked at least one offer of that category.

{% enddocs %}

## Table description

Published on data.gouv.fr as the `diversity_by_category` resource. The denominator is `total_expired_deposit_beneficiaries` of [exp_data_gouv_diversity](description__exp_data_gouv__diversity.md) for the same month and cell. Counts come from the perturbed base [exp_base__diversity_by_category](../export_base/description__exp_base__diversity_by_category.md), closed months only, geographic codes formatted as in [exp_data_gouv_beneficiary](description__exp_data_gouv__beneficiary.md).

**Grain**: `deposit_expiration_month`, `department_name`, `is_in_qpv`, `macro_density_label`, `micro_density_label`, `offer_category_id`.

KPI definition: [Booking share by category](../../../kpis/diversity/booking_share_by_category.md).

{% docs table__exp_data_gouv__diversity_by_category %}{% enddocs %}
