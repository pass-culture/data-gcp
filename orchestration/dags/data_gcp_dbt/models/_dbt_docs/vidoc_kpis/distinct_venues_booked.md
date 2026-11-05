---
title: Average number of distinct venues booked at expiration
description: How to calculate the average number of distinct venues booked by beneficiaries whose credit expired.
---

## Average number of distinct venues booked at credit expiration

### Definition

For beneficiaries whose credit expired in the month, average number of distinct venues (`venue_id`) where they made a non-cancelled booking before expiration. A partner with several venues counts several times.

### Source table

[exp_vidoc_diversity](../models/export/vidoc/description__exp_vidoc_diversity.md)

### Calculation

`total_distinct_venue_booked_at_expiration` is not perturbed: it is **null** when `total_expired_deposit_beneficiaries` is below 5. Exclude those rows from the denominator.

```sql
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

Same as the [diversity rate](diversity_3plus_categories.md#available-dimensions).
