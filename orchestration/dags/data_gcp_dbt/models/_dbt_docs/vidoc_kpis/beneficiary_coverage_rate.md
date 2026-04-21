---
title: Beneficiary coverage rate
description: How to calculate the proportion of eligible young people who have received a pass Culture credit.
---

## Beneficiary coverage rate among eligible youth

### Definition

Proportion of eligible young people who have benefited or are benefiting from a pass Culture credit over a given period and territory.

### Source tables

- [exp_vidoc_beneficiary_coverage](../models/export/vidoc/description__exp_vidoc_beneficiary__coverage.md) — beneficiary count (rolling 12-month window)
- [exp_vidoc_population_coverage](../models/export/vidoc/description__exp_vidoc_population__coverage.md) — eligible population count (rolling 12-month window)

### Calculation

The coverage rate is the ratio of beneficiaries to eligible population, joined on the same month, age, and department.

```sql
SELECT
    b.partition_month,
    b.department_code,
    b.department_name,
    b.milestone_age,
    SUM(b.total_beneficiaries_last_12_months) AS total_beneficiaries,
    SUM(p.total_population_last_12_months) AS total_population,
    SAFE_DIVIDE(
        SUM(b.total_beneficiaries_last_12_months),
        SUM(p.total_population_last_12_months)
    ) AS coverage_rate
FROM `beneficiary_coverage` AS b
INNER JOIN `population_coverage` AS p
    ON b.partition_month = p.partition_month
    AND b.department_code = p.department_code
    AND CAST(b.milestone_age AS STRING) = p.milestone_age
GROUP BY b.partition_month, b.department_code, b.department_name, b.milestone_age
ORDER BY b.partition_month, b.department_code, b.milestone_age
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Age (milestone) | `milestone_age` | Age at credit activation (16, 17, 18, 19, 20) |
| Department | `department_code`, `department_name` | Department of residence |
| Region | `region_code`, `region_name` | Region of residence |

### Temporal tracking

**Annual.** Both tables use a rolling 12-month window (`total_beneficiaries_last_12_months` / `total_population_last_12_months`).

### Data protection

Beneficiary counts are protected using the [Cell Key Perturbation method](../references/statistical_confidentiality.md). Population counts are public data and are not perturbed. Coverage rates computed on small departments may be slightly affected by the perturbation on the numerator.
