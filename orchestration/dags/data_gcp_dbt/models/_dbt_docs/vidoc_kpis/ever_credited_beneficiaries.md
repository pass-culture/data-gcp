---
title: Number of ever-credited beneficiaries
description: How to calculate the number of young people who have ever received a pass Culture credit, whether the credit is still active or not.
---

## Number of ever-credited beneficiaries

### Definition

Number of young people who have ever received a pass Culture credit, whether that credit is still active, exhausted, or expired.

Reported as a **stock at the end of each `partition_month`**, broken down by `age_at_calculation` (age reached at that month). This is not a true running total:

- A user only enters the stock from the month in which they first received a credit.
- A user's `age_at_calculation` is recomputed each month, so the same user appears in different age buckets across months as they age.
- A user leaves the stock if their account becomes inactive for any reason other than "suspension upon user request" (e.g. account deletion).
- Users with a `GRANT_FREE` deposit type are excluded.

### Source table

[exp_vidoc_beneficiary](../models/export/vidoc/description__exp_vidoc_beneficiary.md)

### Calculation

Sum the `total_beneficiaries` column over the desired dimensions.

```sql
SELECT
    partition_month,
    SUM(total_beneficiaries) AS total_ever_credited_beneficiaries
FROM `beneficiary`
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
