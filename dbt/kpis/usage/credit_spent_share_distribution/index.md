## Distribution of beneficiaries by share of credit spent at expiration

### Definition

For beneficiaries whose credit expired in the month, split by the share of all the credits they received that they spent: `0` (no booking), then 10-point steps: `]0-10]`, `]10-20]`, …, `]80-90]`, `]90-100]` %. The last bucket includes beneficiaries with a few cents left.

### Published in

**VIDOC** (ministry dashboard):

[exp_vidoc_diversity](https://pass-culture.github.io/data-gcp/dbt/models/export/vidoc/description__exp_vidoc_diversity/index.md)

### Calculation

```
SELECT
    deposit_expiration_month,
    SAFE_DIVIDE(SUM(total_spent_share_0_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_0,
    SAFE_DIVIDE(SUM(total_spent_share_0_10_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_0_10,
    SAFE_DIVIDE(SUM(total_spent_share_10_20_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_10_20,
    SAFE_DIVIDE(SUM(total_spent_share_20_30_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_20_30,
    SAFE_DIVIDE(SUM(total_spent_share_30_40_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_30_40,
    SAFE_DIVIDE(SUM(total_spent_share_40_50_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_40_50,
    SAFE_DIVIDE(SUM(total_spent_share_50_60_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_50_60,
    SAFE_DIVIDE(SUM(total_spent_share_60_70_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_60_70,
    SAFE_DIVIDE(SUM(total_spent_share_70_80_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_70_80,
    SAFE_DIVIDE(SUM(total_spent_share_80_90_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_80_90,
    SAFE_DIVIDE(SUM(total_spent_share_90_100_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_90_100
FROM `diversity`
GROUP BY deposit_expiration_month
ORDER BY deposit_expiration_month
```

For a lighter display, the buckets can be merged into 4: `0`, `< 50` (`0_10` to `40_50`), `50-90` (`50_60` to `80_90`), `> 90` (`90_100`).

### Available dimensions

Same as the [diversity rate](https://pass-culture.github.io/data-gcp/dbt/kpis/diversity/diversity_3plus_categories/#available-dimensions).

### Data protection and caveats

Each bucket is perturbed independently and capped at `total_expired_deposit_beneficiaries`: the sum of the 11 buckets can differ from the total by a few units in a row. Compute shares on aggregated sums; for exact 100 % stacked charts, divide by the sum of the buckets instead of the total.
