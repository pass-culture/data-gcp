---
title: Beneficiaries (perturbed base)
description: Description of the `exp_base__beneficiary` table, the perturbed base of the beneficiary exports.
---

{% docs description__exp_base__beneficiary %}

# Table: Beneficiaries (perturbed base)

The `exp_base__beneficiary` table holds the beneficiary counts at their finest grain, already protected with the cell key perturbation method. It is the single place where the statistical confidentiality rules of these counts are applied.

{% enddocs %}

## Table description

Every export reads this table and only **sums** its perturbed cells, never re-perturbs them, so that the figures published by every tool match for a given territory and month:

- [exp_vidoc_beneficiary](../vidoc/description__exp_vidoc_beneficiary.md): `select *` for the ministry's vidoc tool;
- [exp_data_gouv_beneficiary](../data_gouv/description__exp_data_gouv__beneficiary.md): lot 1 columns, closed months, for data.gouv.fr;
- external reporting: national, regional and departmental figures are sums of these cells.

Columns, grain and capping rules are described in [exp_vidoc_beneficiary](../vidoc/description__exp_vidoc_beneficiary.md). See [Statistical confidentiality](../../../references/statistical_confidentiality.md).

{% docs table__exp_base__beneficiary %}{% enddocs %}
