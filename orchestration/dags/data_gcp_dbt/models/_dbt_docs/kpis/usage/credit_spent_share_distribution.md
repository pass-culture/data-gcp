---
title: Distribution by share of credit spent at expiration
description: How to calculate the distribution of beneficiaries by share of their credit spent when it expired.
---

## Distribution of beneficiaries by share of credit spent at expiration

### Definition

For beneficiaries whose credit expired in the month, split by the share of all the credits they received that they spent: `0` (no booking), `]0-25]`, `]25-50]`, `]50-75]`, `]75-90]`, `]90-100]` %. The last bucket includes beneficiaries with a few cents left.

### Published in

**VIDOC** (ministry dashboard):

[exp_vidoc_diversity](../../models/export/vidoc/description__exp_vidoc_diversity.md)

### Calculation

```sql
SELECT
    deposit_expiration_month,
    SAFE_DIVIDE(SUM(total_spent_share_0_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_0,
    SAFE_DIVIDE(SUM(total_spent_share_0_25_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_0_25,
    SAFE_DIVIDE(SUM(total_spent_share_25_50_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_25_50,
    SAFE_DIVIDE(SUM(total_spent_share_50_75_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_50_75,
    SAFE_DIVIDE(SUM(total_spent_share_75_90_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_75_90,
    SAFE_DIVIDE(SUM(total_spent_share_90_100_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_90_100
FROM `diversity`
GROUP BY deposit_expiration_month
ORDER BY deposit_expiration_month
```

For a lighter display, the buckets can be merged into 4: `0`, `< 50` (`0_25 + 25_50`), `50-90` (`50_75 + 75_90`), `> 90`.

### Available dimensions

Same as the [diversity rate](../diversity/diversity_3plus_categories.md#available-dimensions).

### Data protection and caveats

Each bucket is perturbed independently and capped at `total_expired_deposit_beneficiaries`: the sum of the 6 buckets can differ from the total by a few units in a row. Compute shares on aggregated sums; for exact 100 % stacked charts, divide by the sum of the buckets instead of the total.
