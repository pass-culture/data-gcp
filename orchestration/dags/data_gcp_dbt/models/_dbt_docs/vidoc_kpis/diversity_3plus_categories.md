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

The share is the ratio of `total_3plus_category_booked_beneficiaries` to `total_expired_deposit_beneficiaries`.

```sql
SELECT
    deposit_expiration_month,
    SUM(total_3plus_category_booked_beneficiaries) AS booked_3plus_categories,
    SUM(total_expired_deposit_beneficiaries) AS total_expired,
    SAFE_DIVIDE(
        SUM(total_3plus_category_booked_beneficiaries),
        SUM(total_expired_deposit_beneficiaries)
    ) AS diversity_rate
FROM `diversity`
GROUP BY deposit_expiration_month
ORDER BY deposit_expiration_month
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Expiration month | `deposit_expiration_month` | Month when the beneficiary's credit expired |
| Department | `department_code`, `department_name` | Department of residence |
| Region | `region_code`, `region_name` | Region of residence |
| QPV | `is_in_qpv` | Priority neighbourhood flag |
| Rurality (macro) | `macro_density_label` | Macro density level |
| Rurality (micro) | `micro_density_label` | Micro density level |

### Temporal tracking

**Monthly.** One cohort per `deposit_expiration_month`, the month when the beneficiaries' credit expired. Sum several months (e.g. a quarter or a year) for small territories.

### Data protection

Both columns are protected using the [Cell Key Perturbation method](../references/statistical_confidentiality.md). They correspond to **nested cohorts** (`3plus ⊂ expired`) but are perturbed **independently** (each column has its own cell key). Consequences:

- On a single small cell, the invariant `total_3plus_category_booked_beneficiaries ≤ total_expired_deposit_beneficiaries` can be **violated** after perturbation.
- Always aggregate over many cells (national, regional, several months) before computing the rate; the law of large numbers brings the ratio back close to the true value.
- For a national rate over a quarter or more, the relative error is negligible.
