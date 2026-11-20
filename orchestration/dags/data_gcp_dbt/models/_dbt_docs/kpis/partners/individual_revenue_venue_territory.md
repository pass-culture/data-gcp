---
title: Individual booking revenue by venue territory
description: How to calculate the revenue of the cultural partners from individual bookings, and its split between pass Culture and partners, by venue territory.
---

## Individual booking revenue by venue territory

### Definition

Revenue generated for the cultural partners by used individual bookings (`total_revenue_amount`, booking amount in euros), by month of use and territory of the **venue**. It is split into:

- `total_reimbursed_amount`: amount reimbursed by pass Culture to the partners;
- `total_contribution_amount`: share borne by the partners (revenue minus reimbursed amount, e.g. when the reimbursement rate is below 100%).

Only bookings already included in a payment batch are counted.

### Published in

**VIDOC** (ministry dashboard):

[exp_vidoc_booking_partner_finance_individual](../../models/export/vidoc/description__exp_vidoc_booking_partner_finance__individual.md)

### Calculation

```sql
SELECT
    partition_month,
    total_revenue_amount AS revenue,
    total_reimbursed_amount AS reimbursed,
    total_contribution_amount AS partner_contribution,
    SAFE_DIVIDE(total_reimbursed_amount, total_revenue_amount) AS reimbursement_rate
FROM `booking_partner_finance_individual`
WHERE territory_level = 'NAT'
ORDER BY partition_month
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Month of use | `partition_month` | Month when the booking was used |
| Territory | `territory_level`, `territory_code`, `territory_name` | Venue territory, one level per row: `DEP` (department), `REG` (region), `GROUP` (METRO / DROM / COM), `NAT` (France) |
| Parent region | `region_code` | Region of a department (NULL for overseas collectivities) |

### Temporal tracking

**Monthly.** Months up to the month before the run date, as in external reporting. The last month is about 99% complete when first published (bookings not yet in a payment batch) and may be slightly revised afterwards.

### Data protection

**Statistical secret of businesses** (INSEE rule, not cell key perturbation), applied once in [exp_base__booking_venue_individual](../../models/export/export_base/description__exp_base__booking_venue_individual.md): a territory is secret when, at the last complete month, for the 6 or 48 month window and on the individual or the collective side, it has fewer than 3 cultural partners with revenue, one partner above 85% of the revenue, or a secondary secret applies. It then has `is_statistic_secret` TRUE and NULL figures on every month. Each level (department, region, group, France) is computed on unmasked data and published with its own secret status; NAT is never secret. Revenue is never published at venue or offer level.

### Notes and caveats

- Read each level from its own rows (`WHERE territory_level = 'REG'`): never sum departments to get a region or France, the masked departments would be missing. Overseas collectivities are small and often secret one by one: read them from the `GROUP` row `COM`.
- Not broken down by offer category, by design: by venue location, a category often has fewer than 3 partners in a department (about a third of the department x category cells over 12 months), so the revenue by category would reveal a partner's revenue. Categories are read on the young people's side (practices), as in the observatoire: see [Individual bookings by category and beneficiary territory](../usage/individual_bookings_beneficiary_territory.md).
