---
title: Individual bookings and tickets by venue territory
description: How to calculate the number of used individual bookings and tickets by territory of the cultural partner.
---

## Used individual bookings and tickets by venue territory

### Definition

Number of used individual bookings (`total_bookings`) and of tickets or items booked (`total_quantities`, a booking can hold several tickets, e.g. duo offers) on the pass Culture, by month of use and by territory of the **venue** (where the cultural partner is, not where the young person lives).

Only bookings already included in a payment batch are counted: a used booking is counted once its pricing is invoiced to the cultural partner.

### Published in

**VIDOC** (ministry dashboard):

[exp_vidoc_booking_partner_finance_individual](../../models/export/vidoc/description__exp_vidoc_booking_partner_finance__individual.md)

### Calculation

```sql
SELECT
    partition_month,
    territory_code,
    territory_name,
    total_bookings AS used_bookings,
    total_quantities AS tickets
FROM `booking_partner_finance_individual`
WHERE territory_level = 'REG'
ORDER BY partition_month
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Month of use | `partition_month` | Month when the booking was used |
| Territory | `territory_level`, `territory_code`, `territory_name` | Venue territory, one level per row: `DEP` (department), `REG` (region), `GROUP` (METRO / DROM / COM), `NAT` (France) |
| Parent region | `region_code` | Region of a department (NULL for overseas collectivities) |

### Temporal tracking

**Monthly.** One row per month of use and published venue territory. Months up to the month before the run date are exported, as in external reporting. Bookings enter the table when their payment batch is closed: the last month is about 99% complete when first published and may be slightly revised (+1%) on the next exports.

### Data protection

**Statistical secret of businesses** (INSEE rule, not cell key perturbation), applied once in [exp_base__booking_venue_individual](../../models/export/export_base/description__exp_base__booking_venue_individual.md): a territory is secret when, at the last complete month, for the 6 or 48 month window and on the individual or the collective side, it has fewer than 3 cultural partners with revenue, one partner above 85% of the revenue, or a secondary secret applies. It then has `is_statistic_secret` TRUE and NULL figures on every month. Each level (department, region, group, France) is computed on unmasked data and published with its own secret status; NAT is never secret.

### Notes and caveats

- Read each level from its own rows (`WHERE territory_level = 'REG'`): never sum departments to get a region or France, the masked departments would be missing. Overseas collectivities are small and often secret one by one: read them from the `GROUP` row `COM`.
- Not broken down by offer category, by design (partner side: a category often has fewer than 3 partners in a department). Categories are read on the young people's side (practices), as in the observatoire. For bookings by category, see [Individual bookings by category and beneficiary territory](../usage/individual_bookings_beneficiary_territory.md) (residence of the young person, no payment batch condition).
