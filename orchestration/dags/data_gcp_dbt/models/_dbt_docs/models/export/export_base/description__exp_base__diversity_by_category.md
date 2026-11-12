---
title: Diversity by offer category (perturbed base)
description: Description of the `exp_base__diversity_by_category` table, the perturbed base of the diversity_by_category exports.
---

{% docs description__exp_base__diversity_by_category %}

# Table: Diversity by offer category (perturbed base)

The `exp_base__diversity_by_category` table holds the diversity_by_category counts at their finest grain, already protected with the cell key perturbation method. It is the single place where the statistical confidentiality rules of these counts are applied.

{% enddocs %}

## Table description

Every export reads this table and only **sums** its perturbed cells, never re-perturbs them, so that the figures published by every tool match for a given territory and month:

- [exp_vidoc_diversity_by_category](../vidoc/description__exp_vidoc_diversity__by_category.md): `select *` for the ministry's vidoc tool;
- [exp_data_gouv_diversity_by_category](../data_gouv/description__exp_data_gouv__diversity_by_category.md): lot 1 columns, closed months, for data.gouv.fr;
- external reporting: national, regional and departmental figures are sums of these cells.

Columns, grain and capping rules are described in [exp_vidoc_diversity_by_category](../vidoc/description__exp_vidoc_diversity__by_category.md). See [Statistical confidentiality](../../../references/statistical_confidentiality.md).

{% docs table__exp_base__diversity_by_category %}{% enddocs %}
