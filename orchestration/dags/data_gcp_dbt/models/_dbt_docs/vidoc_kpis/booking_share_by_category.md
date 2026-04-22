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

The share is the ratio of `total_category_booked_beneficiaries` (per category) to `total_expired_credit_beneficiaries` (total, **independent of the category**).

Because the denominator is not category-specific, **aggregate the numerator and the denominator separately** before dividing:

```sql
WITH numerator AS (
    SELECT
        deposit_expiration_month,
        offer_category_id,
        SUM(total_category_booked_beneficiaries) AS booked_in_category
    FROM `diversity_by_category`
    GROUP BY deposit_expiration_month, offer_category_id
),
denominator AS (
    SELECT
        deposit_expiration_month,
        SUM(total_expired_credit_beneficiaries) AS total_expired
    FROM `diversity`
    GROUP BY deposit_expiration_month
)
SELECT
    n.deposit_expiration_month,
    n.offer_category_id,
    n.booked_in_category,
    d.total_expired,
    SAFE_DIVIDE(n.booked_in_category, d.total_expired) AS category_booking_rate
FROM numerator AS n
INNER JOIN denominator AS d USING (deposit_expiration_month)
ORDER BY n.deposit_expiration_month, n.offer_category_id
```

> **Do not** join `diversity_by_category` to `diversity` on all geographic dimensions before summing: `diversity_by_category` has one row per `(cell, category present)`, so an `INNER JOIN` at the cell level silently drops from the denominator every cell where the category has zero bookings. The rate is then **overestimated**, especially for rare categories. Always aggregate each side to its natural grain first, then join.

To compute the rate on a sub-group (e.g. per department, per QPV), add the grouping dimensions to both CTEs and join on them. Cross-join with the list of categories if you need to emit explicit zero-booking rows:

```sql
WITH denom AS (
    SELECT deposit_expiration_month, department_code,
           SUM(total_expired_credit_beneficiaries) AS total_expired
    FROM `diversity`
    WHERE department_code = 'XX' -- optional filter to a specific sub-group
    GROUP BY deposit_expiration_month, department_code
),
num AS (
    SELECT deposit_expiration_month, department_code, offer_category_id,
           SUM(total_category_booked_beneficiaries) AS booked
    FROM `diversity_by_category`
    WHERE department_code = 'XX' -- optional filter to a specific sub-group
    GROUP BY deposit_expiration_month, department_code, offer_category_id
)
SELECT d.deposit_expiration_month, d.department_code, n.offer_category_id,
       SAFE_DIVIDE(n.booked, d.total_expired) AS rate
FROM denom AS d
LEFT JOIN num AS n USING (deposit_expiration_month, department_code)
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

Counts are protected using the [Cell Key Perturbation method](../references/statistical_confidentiality.md). The numerator (`total_category_booked_beneficiaries`) and the denominator (`total_expired_credit_beneficiaries`) come from **different cohorts of users** and are therefore perturbed **independently** (different cell keys → independent noise). Consequences:

- On a single small cell, the ratio can be unstable (numerator and denominator each shifted by up to ±5).
- The invariant `booked_in_category ≤ total_expired` can be **violated** at the cell level after perturbation. Always aggregate over many cells (national, regional, quarterly) before computing a rate.
- For very rare categories, a rate computed at a fine geographic grain should be treated as indicative only.
