---
title: Individual bookings and tickets by venue territory
description: How to calculate the number of used individual bookings and tickets by territory of the cultural partner.
---

## Used individual bookings and tickets by venue territory

### Definition

Number of used individual bookings (`total_bookings`) and of tickets or items booked (`total_quantities`, a booking can hold several tickets, e.g. duo offers) on the pass Culture, by month of use and by territory of the **venue** (where the cultural partner is, not where the young person lives).

Only bookings already included in a payment batch are counted: a used booking is counted once its pricing is invoiced to the cultural partner.

### Source table

[exp_vidoc_booking_partner_finance_individual](../../models/export/vidoc/description__exp_vidoc_booking_partner_finance__individual.md)

### Calculation

```sql
SELECT
    partition_month,
    venue_region_code,
    venue_region_name,
    SUM(total_bookings) AS used_bookings,
    SUM(total_quantities) AS tickets
FROM `booking_partner_finance_individual`
GROUP BY partition_month, venue_region_code, venue_region_name
ORDER BY partition_month
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Month of use | `partition_month` | Month when the booking was used |
| Department | `venue_department_code`, `venue_department_name` | Department of the venue |
| Region | `venue_region_code`, `venue_region_name` | Region of the venue |

### Temporal tracking

**Monthly.** One row per month of use and venue department. Months up to the month before the run date are exported, as in external reporting. Bookings enter the table when their payment batch is closed: the last month is about 99% complete when first published and may be slightly revised (+1%) on the next exports.

### Data protection

Partner revenue is under **statistical secret** (not cell key perturbation), with the rule of external reporting: when the department is secret at the last complete month, for the 6 or 48 month window, on the individual or the collective side (fewer than 3 cultural partners with revenue, one partner above 85% of the revenue, or secondary secret), `is_statistic_secret` is TRUE and all KPIs of the department, bookings and tickets included, are NULL on every month. `department_secret_reason_label` gives the reasons.

### Notes and caveats

- Region and national totals are sums of the visible departments, so they leave out the secret departments (mostly overseas collectivities and Lozère): less than 0.06% of the national volume in 2026. External reporting publishes REG and NAT unmasked; this table has the department grain only.
- `venue_region_code` is NULL for the overseas collectivities (Saint-Martin, Saint-Pierre-et-Miquelon, Nouvelle-Calédonie, ...); `venue_region_name` is filled.
- Not broken down by offer category, by design (partner side: a category often has fewer than 3 partners in a department). Categories are read on the young people's side (practices), as in the observatoire. For bookings by category, see [Individual bookings by category and beneficiary territory](../usage/individual_bookings_beneficiary_territory.md) (residence of the young person, no payment batch condition).
