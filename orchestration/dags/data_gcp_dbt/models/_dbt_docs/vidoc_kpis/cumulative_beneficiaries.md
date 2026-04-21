---
title: Cumulative number of beneficiaries
description: How to calculate the cumulative number of young people who have received a pass Culture credit.
---

## Cumulative number of beneficiaries

### Definition

Cumulative number of young people who have received a pass Culture credit, whether the credit is still active or not.

### Source table

[exp_vidoc_beneficiary](../models/export/vidoc/description__exp_vidoc_beneficiary.md)

### Calculation

Sum the `total_beneficiaries` column over the desired dimensions.

```sql
SELECT
    partition_month,
    SUM(total_beneficiaries) AS cumulative_beneficiaries
FROM exp_vidoc_beneficiary
GROUP BY partition_month
ORDER BY partition_month
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Age (integer, ever reached) | `age_at_calculation` | Beneficiary's age at calculation date |
| Department | `department_code`, `department_name` | Department of residence |
| Region | `region_code`, `region_name` | Region of residence |
| QPV | `is_in_qpv` | Priority neighbourhood flag |
| Rurality (macro) | `macro_density_label` | Macro density level |
| Rurality (micro) | `micro_density_label` | Micro density level |

### Temporal tracking

**Quarterly.** The `partition_month` column tracks changes over time.

### Data protection

Counts are protected using the [Cell Key Perturbation method](../references/statistical_confidentiality.md). Small counts are slightly modified; large counts and aggregated totals are virtually exact.
