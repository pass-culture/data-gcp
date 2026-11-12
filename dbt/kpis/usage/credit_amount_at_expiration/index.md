## Average amount granted, spent and spending rate at credit expiration

### Definition

For beneficiaries whose credit expired in the month (same population as the [diversity rate](https://pass-culture.github.io/data-gcp/dbt/kpis/diversity/diversity_3plus_categories/index.md)):

- **Average amount granted**: total of all the credits the beneficiary received (15-17 credits, 17-18 credit, bonus), per beneficiary.
- **Average amount spent**: amount of non-cancelled bookings made with those credits, per beneficiary.
- **Spending rate**: amount spent ÷ amount granted (a ratio of sums, not an average of individual rates).

The credit amount depends on the generation (500 € before the 2023 reform, then 300 €, then 150 € + 50 € bonus after 2025): compare generations with the spending rate rather than with amounts.

### Published in

**VIDOC** (ministry dashboard):

[exp_vidoc_diversity](https://pass-culture.github.io/data-gcp/dbt/models/export/vidoc/description__exp_vidoc_diversity/index.md)

### Calculation

Same definition as the external reporting KPIs `montant_moyen_octroye_a_l_expiration_du_credit` and `montant_moyen_depense_a_l_expiration_du_credit` (read from `metrics_beneficiary__expired_cohorts`, same values).

The amounts are not perturbed: they are **null** when `total_expired_deposit_beneficiaries` is below 5. Exclude those rows from the denominator too, otherwise the averages are underestimated.

```
SELECT
    deposit_expiration_month,
    SAFE_DIVIDE(SUM(total_deposit_amount_at_expiration), SUM(total_expired_deposit_beneficiaries)) AS avg_amount_granted,
    SAFE_DIVIDE(SUM(total_actual_amount_spent_at_expiration), SUM(total_expired_deposit_beneficiaries)) AS avg_amount_spent,
    SAFE_DIVIDE(SUM(total_actual_amount_spent_at_expiration), SUM(total_deposit_amount_at_expiration)) AS spending_rate
FROM `diversity`
WHERE total_actual_amount_spent_at_expiration IS NOT NULL
GROUP BY deposit_expiration_month
ORDER BY deposit_expiration_month
```

### Available dimensions

Same as the [diversity rate](https://pass-culture.github.io/data-gcp/dbt/kpis/diversity/diversity_3plus_categories/#available-dimensions).

### Temporal tracking

**Monthly** cohorts of expiration, displayed by quarter or year (from April 2025 to March 2026 few credits expired after the 2025 reform: these cohorts are small).
