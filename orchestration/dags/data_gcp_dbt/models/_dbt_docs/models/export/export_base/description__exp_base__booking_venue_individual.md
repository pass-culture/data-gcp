---
title: Individual bookings by venue territory (export base)
description: Description of the `exp_base__booking_venue_individual` table, the base of the venue-side individual booking exports, under the statistical secret of businesses.
---

{% docs description__exp_base__booking_venue_individual %}

# Table: Individual bookings by venue territory (export base)

The `exp_base__booking_venue_individual` table holds the used individual bookings and their amounts (revenue, reimbursed, contribution) by month and **venue (cultural partner) territory**, at every published level. It is the single place where the statistical secret of businesses is applied to these figures.

{% enddocs %}

## Table description

**Grain**: `partition_month`, `territory_level`, `territory_code`.

Bookings and amounts of a venue are economic data of a business: they follow the **statistical secret of businesses** (INSEE rule), not the cell key perturbation of people. A territory is secret when, over the last 6 or 48 months, it has fewer than 3 cultural partners with revenue, one partner holds more than 85% of the revenue, or a secondary secret applies (see `statistical_secret__cultural_partner_individual` and `_collective`).

Masked cells cannot be summed, so every level is **computed on unmasked data and published on its own** (`territory_level`):

| Level | Territories | Secret |
|---|---|---|
| `DEP` | every department, overseas collectivities included | `is_department_secret_stat` |
| `REG` | the 18 regions (overseas collectivities have no region) | `is_region_secret_stat` |
| `GROUP` | METRO, DROM, COM | `is_territory_group_secret_stat` |
| `NAT` | France, venues missing from the referential included | never |

- The secret model already masks a second department when a region or territory group total would give a masked one by subtraction (secondary secret), and flags a region or group total that would still disclose it. Publishing the REG, GROUP and NAT rows therefore never discloses a masked department.
- Overseas collectivities (975, 977, 978, 986 to 988) are small and often secret one by one, but the COM group is not: read them from the `GROUP` row.
- A territory is secret when it is secret at the **last complete month**, for any window and on the individual or collective side (partner pages show both). Its figures are then NULL on **every month** (`is_statistic_secret` TRUE). The status can change from one run to the next for the whole history.
- Never sum a level to get another one: the sum of the published departments of a region is lower than the region row when a department is masked.
- Same source and months as `external_reporting__finance` (all categories, `metrics_booking__finance_individual`): bookings already in a payment batch, on their month of use, from January 2022 to the month before the run date.

Read by [exp_vidoc_booking_partner_finance_individual](../vidoc/description__exp_vidoc_booking_partner_finance__individual.md). See [Statistical confidentiality](../../../references/statistical_confidentiality.md).

{% docs table__exp_base__booking_venue_individual %}{% enddocs %}
