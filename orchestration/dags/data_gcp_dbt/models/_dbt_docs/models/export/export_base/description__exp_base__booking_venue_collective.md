---
title: Collective bookings by venue territory (export base)
description: Description of the `exp_base__booking_venue_collective` table, the base of the venue-side collective booking exports, under the statistical secret of businesses.
---

{% docs description__exp_base__booking_venue_collective %}

# Table: Collective bookings by venue territory (export base)

The `exp_base__booking_venue_collective` table holds the confirmed collective (EAC) bookings and their amount by month, scholar year and **venue (cultural partner) territory**, at every published level (DEP, REG, GROUP, NAT), under the statistical secret of businesses.

{% enddocs %}

## Table description

**Grain**: `partition_month`, `scholar_year`, `territory_level`, `territory_code`.

Same levels and secret rules as [exp_base__booking_venue_individual](description__exp_base__booking_venue_individual.md): each level is computed on unmasked data and published on its own, never sum a level to get another one.

- Same source and values as `external_reporting__collective_partner_booking` (`metrics_booking__collective`): monthly flows summable over a scholar year, bookings created before the scholar year counted in September, months from January 2022 to the month before the run date.
- Bookings by **school** territory are public money and not secret: see [exp_vidoc_booking_collective](../vidoc/description__exp_vidoc_booking_collective.md).

Read by [exp_vidoc_booking_collective_venue](../vidoc/description__exp_vidoc_booking_collective_venue.md).

{% docs table__exp_base__booking_venue_collective %}{% enddocs %}
