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

`age_at_calculation` is empty when the birth date is unknown. A few cells show ages outside 15-20 (inconsistent declared birth dates, or beneficiaries kept in the ever-credited stock after their credit expired).

KPI definitions: [Ever-credited beneficiaries](../../../kpis/beneficiaries/ever_credited_beneficiaries.md), [Active beneficiaries](../../../kpis/beneficiaries/active_beneficiaries.md).

{% docs table__exp_data_gouv__beneficiary %}{% enddocs %}
