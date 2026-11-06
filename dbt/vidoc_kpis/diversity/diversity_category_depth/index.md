## Share of beneficiaries booking in N+ distinct categories at credit expiration

### Definition

For N = 1 to 5, share of beneficiaries whose credit expired in the month and who made a non-cancelled booking in at least N different offer categories before expiration. `1+` is the share of beneficiaries who booked at least once. The 3+ threshold is the lot 1 [diversity rate](https://pass-culture.github.io/data-gcp/dbt/vidoc_kpis/diversity/diversity_3plus_categories/index.md).

Population: beneficiaries whose last credit is a 17-18 credit (`GRANT_18`, `GRANT_17_18`) and expired in a closed month. Bookings made with all the credits they received (15-17 included) are observed. Categories are declared by the cultural partner.

### Source table

[exp_vidoc_diversity](https://pass-culture.github.io/data-gcp/dbt/models/export/vidoc/description__exp_vidoc_diversity/index.md)

### Calculation

```
SELECT
    deposit_expiration_month,
    SAFE_DIVIDE(SUM(total_1plus_category_booked_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_1plus,
    SAFE_DIVIDE(SUM(total_2plus_category_booked_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_2plus,
    SAFE_DIVIDE(SUM(total_3plus_category_booked_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_3plus,
    SAFE_DIVIDE(SUM(total_4plus_category_booked_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_4plus,
    SAFE_DIVIDE(SUM(total_5plus_category_booked_beneficiaries), SUM(total_expired_deposit_beneficiaries)) AS share_5plus
FROM `diversity`
GROUP BY deposit_expiration_month
ORDER BY deposit_expiration_month
```

### Available dimensions

Same as the [diversity rate](https://pass-culture.github.io/data-gcp/dbt/vidoc_kpis/diversity/diversity_3plus_categories/#available-dimensions): expiration month, region, department, QPV, density.

### Temporal tracking

**Monthly** cohorts of expiration. Display by quarter or year: from April 2025 to March 2026 few credits expired (2025 reform), so these cohorts are small and not representative.

### Data protection and caveats

- All counts are perturbed with their own cell key, then each one is capped at `total_expired_deposit_beneficiaries` in its row. The thresholds are perturbed independently: on a small cell the order `5+ ≤ 4+ ≤ … ≤ 1+` can break; aggregate before reading.
- The thresholds are cumulative: do not add them up. The share with exactly N categories is `N+ − (N+1)+`.
