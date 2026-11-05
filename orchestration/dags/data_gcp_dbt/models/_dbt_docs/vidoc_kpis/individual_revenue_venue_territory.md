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

### Source table

[exp_vidoc_booking_partner_finance_individual](../models/export/vidoc/description__exp_vidoc_booking_partner_finance__individual.md)

### Calculation

```sql
SELECT
    partition_month,
    SUM(total_revenue_amount) AS revenue,
    SUM(total_reimbursed_amount) AS reimbursed,
    SUM(total_contribution_amount) AS partner_contribution,
    SAFE_DIVIDE(SUM(total_reimbursed_amount), SUM(total_revenue_amount)) AS reimbursement_rate
FROM `booking_partner_finance_individual`
GROUP BY partition_month
ORDER BY partition_month
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Month of use | `partition_month` | Month when the booking was used |
| Department | `venue_department_code`, `venue_department_name` | Department of the venue |
| Region | `venue_region_code`, `venue_region_name` | Region of the venue |

### Temporal tracking

**Monthly.** Months up to the month before the run date, as in external reporting. The last month is about 99% complete when first published (bookings not yet in a payment batch) and may be slightly revised afterwards.

### Data protection

**Statistical secret** on the venue department, with the rule of external reporting: the department is secret at the last complete month, for the 6 or 48 month window, on the individual or the collective side (fewer than 3 cultural partners with revenue, one partner above 85% of the revenue, or secondary secret). A secret department has `is_statistic_secret` TRUE and NULL amounts on every month. Revenue is never published at venue or offer level.

### Notes and caveats

- Sums of departments underestimate the region and national revenue by the secret departments (less than 0.03% of the national revenue in 2026). External reporting publishes REG and NAT unmasked; this table has the department grain only.
- `venue_region_code` is NULL for the overseas collectivities; `venue_region_name` is filled.
- Not broken down by offer category, by design: by venue location, a category often has fewer than 3 partners in a department (about a third of the department x category cells over 12 months), so the revenue by category would reveal a partner's revenue. Categories are read on the young people's side (practices), as in the observatoire: see [Individual bookings by category and beneficiary territory](individual_bookings_beneficiary_territory.md).
