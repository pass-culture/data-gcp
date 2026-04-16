---
title: Diversity rate (3+ categories)
description: How to calculate the share of beneficiaries who booked in at least 3 different offer categories by credit expiration.
---

## Share of beneficiaries booking in 3+ distinct categories at credit expiration

### Definition

Share of credited beneficiaries who made a validated booking in at least 3 different offer categories by the time their credit expired. Bookings across all granted credits (ages 15-20) are observed. Categories are as declared by the cultural partner when creating the offer on the pass Culture portal.

Only beneficiaries who received an age-18 credit are considered.

### Source table

[exp_vidoc_diversity](../models/export/vidoc/description__exp_vidoc_diversity.md)

### Calculation

The share is the ratio of `total_3plus_category_booked_beneficiaries` to `total_expired_credit_beneficiaries`.

```sql
SELECT
    deposit_expiration_month,
    SUM(total_3plus_category_booked_beneficiaries) AS booked_3plus_categories,
    SUM(total_expired_credit_beneficiaries) AS total_expired,
    SAFE_DIVIDE(
        SUM(total_3plus_category_booked_beneficiaries),
        SUM(total_expired_credit_beneficiaries)
    ) AS diversity_rate
FROM exp_vidoc_diversity
GROUP BY deposit_expiration_month
ORDER BY deposit_expiration_month
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Expiration quarter | `deposit_expiration_month` | Month when the beneficiary's credit expired |
| Department | `department_code`, `department_name` | Department of residence |
| Region | `region_code`, `region_name` | Region of residence |
| QPV | `is_in_qpv` | Priority neighbourhood flag |
| Rurality (macro) | `macro_density_label` | Macro density level |
| Rurality (micro) | `micro_density_label` | Micro density level |

### Temporal tracking

**Quarterly.** The `deposit_expiration_month` column indicates when the beneficiary's credit expired.

### Data protection

Both numerator and denominator are protected using the [Cell Key Perturbation method](../references/statistical_confidentiality.md). Rates computed on small cells may be slightly affected.
