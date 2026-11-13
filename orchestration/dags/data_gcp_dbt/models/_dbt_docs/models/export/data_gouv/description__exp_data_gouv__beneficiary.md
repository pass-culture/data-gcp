---
title: Beneficiaries (data.gouv.fr)
description: Description of the `exp_data_gouv__beneficiary` table, published on data.gouv.fr.
---

{% docs description__exp_data_gouv__beneficiary %}

# Table: Beneficiaries (data.gouv.fr)

The `exp_data_gouv__beneficiary` table is the monthly stock of ever-credited and active (current) beneficiaries by territory of residence, age, QPV and density.

{% enddocs %}

## Table description

Published on data.gouv.fr as the `beneficiary` resource. Counts come from the perturbed base [exp_base__beneficiary](../export_base/description__exp_base__beneficiary.md) (cell key perturbation, see [Statistical confidentiality](../../../references/statistical_confidentiality.md)), with:

- closed months only (the current month is excluded);
- `region_code` as a zero-padded INSEE code (`01` for Guadeloupe); `region_code` and `department_code` are empty for non-localised beneficiaries (`non localisé`), and `region_code` is empty for overseas collectivities (975, 978, 986, 987, 988).

**Grain**: `partition_month`, `department_name`, `age_at_calculation`, `is_in_qpv`, `macro_density_label`, `micro_density_label`.

`age_at_calculation` is measured on the first day of the month (a young person credited at 15 during the month shows as 14), and the ever-credited stock keeps beneficiaries after their credit expired, so ages go beyond 21. It is empty when the birth date is unknown, or when the age is impossible given the credit history (inconsistent declared birth dates): below 14 until the March 2025 reform and then one more year per year (no credit at 15 since), or above 19 in February 2019 plus one per year. They are merged into the empty age in the perturbed base (see [Statistical confidentiality](../../../references/statistical_confidentiality.md#a-single-perturbed-base-for-every-export)), so totals are unchanged.

KPI definitions: [Ever-credited beneficiaries](../../../kpis/beneficiaries/ever_credited_beneficiaries.md), [Active beneficiaries](../../../kpis/beneficiaries/active_beneficiaries.md).

{% docs table__exp_data_gouv__beneficiary %}{% enddocs %}
