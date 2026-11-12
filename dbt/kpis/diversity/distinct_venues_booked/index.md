## Average number of distinct venues booked at credit expiration

### Definition

For beneficiaries whose credit expired in the month, average number of distinct venues (`venue_id`) where they made a non-cancelled booking before expiration. A partner with several venues counts several times.

### Published in

**VIDOC** (ministry dashboard):

[exp_vidoc_diversity](https://pass-culture.github.io/data-gcp/dbt/models/export/vidoc/description__exp_vidoc_diversity/index.md)

### Calculation

`total_distinct_venue_booked_at_expiration` is not perturbed: it is **null** when `total_expired_deposit_beneficiaries` is below 5. Exclude those rows from the denominator.

```
SELECT
    deposit_expiration_month,
    SAFE_DIVIDE(
        SUM(total_distinct_venue_booked_at_expiration),
        SUM(total_expired_deposit_beneficiaries)
    ) AS avg_distinct_venues
FROM `diversity`
WHERE total_distinct_venue_booked_at_expiration IS NOT NULL
GROUP BY deposit_expiration_month
ORDER BY deposit_expiration_month
```

### Available dimensions

Same as the [diversity rate](https://pass-culture.github.io/data-gcp/dbt/kpis/diversity/diversity_3plus_categories/#available-dimensions).
