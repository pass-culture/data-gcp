---
title: Number of active beneficiaries
description: How to calculate the number of beneficiaries with a non-expired and non-exhausted credit.
---

## Number of active beneficiaries

### Definition

Number of beneficiaries whose credit is neither expired nor fully spent at the observation date.

A beneficiary is considered active if, on the last day of the observed month, their cumulative used bookings amount is strictly less than the initial credit amount.

### Source table

[exp_vidoc_beneficiary](../models/export/vidoc/description__exp_vidoc_beneficiary.md)

### Calculation

Sum the `total_actual_beneficiaries` column over the desired dimensions.

```sql
SELECT
    partition_month,
    SUM(total_actual_beneficiaries) AS active_beneficiaries
FROM `beneficiary`
GROUP BY partition_month
ORDER BY partition_month
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Age (integer, at date) | `age_at_calculation` | Beneficiary's age at calculation date |
| Department | `department_code`, `department_name` | Department of residence |
| Region | `region_code`, `region_name` | Region of residence |
| QPV | `is_in_qpv` | Priority neighbourhood flag |
| Rurality (macro) | `macro_density_label` | Macro density level |
| Rurality (micro) | `micro_density_label` | Micro density level |

### Temporal tracking

**Quarterly.** The `partition_month` column tracks changes over time.

### Data protection

Counts are protected using the [Cell Key Perturbation method](../references/statistical_confidentiality.md). Small counts are slightly modified; large counts and aggregated totals are virtually exact.

Note: `total_actual_beneficiaries` and `total_beneficiaries` correspond to nested cohorts (active ⊂ ever-credited) but are perturbed **independently**. At the cell level, the invariant `total_actual_beneficiaries ≤ total_beneficiaries` can be violated after perturbation; aggregate over many cells before computing an "active share" ratio.
