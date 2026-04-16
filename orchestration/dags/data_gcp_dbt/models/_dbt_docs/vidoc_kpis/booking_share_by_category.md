---
title: Booking share by category
description: How to calculate the share of beneficiaries who booked at least one offer in a given category by credit expiration.
---

## Share of beneficiaries booking in category X at credit expiration

### Definition

Share of credited beneficiaries who made a validated booking in a given offer category by the time their credit expired. Bookings across all granted credits (ages 15-20) are observed. Categories are as declared by the cultural partner when creating the offer on the pass Culture portal.

Only beneficiaries who received an age-18 credit are considered.

### Source tables

- [exp_vidoc_diversity_by_category](../models/export/vidoc/description__exp_vidoc_diversity__by_category.md) — beneficiary count per category
- [exp_vidoc_diversity](../models/export/vidoc/description__exp_vidoc_diversity.md) — total expired-credit beneficiaries (denominator)

### Calculation

The share is the ratio of `total_category_booked_beneficiaries` (per category) to `total_expired_credit_beneficiaries` (total), joined on the same expiration month and geographic dimensions.

```sql
SELECT
    cat.deposit_expiration_month,
    cat.offer_category_id,
    SUM(cat.total_category_booked_beneficiaries) AS booked_in_category,
    SUM(div.total_expired_credit_beneficiaries) AS total_expired,
    SAFE_DIVIDE(
        SUM(cat.total_category_booked_beneficiaries),
        SUM(div.total_expired_credit_beneficiaries)
    ) AS category_booking_rate
FROM exp_vidoc_diversity_by_category AS cat
INNER JOIN exp_vidoc_diversity AS div
    ON cat.deposit_expiration_month = div.deposit_expiration_month
    AND cat.department_code = div.department_code
    AND cat.is_in_qpv = div.is_in_qpv
    AND cat.macro_density_label = div.macro_density_label
    AND cat.micro_density_label = div.micro_density_label
GROUP BY cat.deposit_expiration_month, cat.offer_category_id
ORDER BY cat.deposit_expiration_month, cat.offer_category_id
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Expiration quarter | `deposit_expiration_month` | Month when the beneficiary's credit expired |
| Offer category | `offer_category_id` | Offer category as declared by the cultural partner |
| Department | `department_code`, `department_name` | Department of residence |
| Region | `region_code`, `region_name` | Region of residence |
| QPV | `is_in_qpv` | Priority neighbourhood flag |
| Rurality (macro) | `macro_density_label` | Macro density level |
| Rurality (micro) | `micro_density_label` | Micro density level |

### Temporal tracking

**Quarterly.** The `deposit_expiration_month` column indicates when the beneficiary's credit expired.

### Data protection

Counts are protected using the [Cell Key Perturbation method](../references/statistical_confidentiality.md). Both the per-category numerator and the denominator from `exp_vidoc_diversity` are perturbed independently.
