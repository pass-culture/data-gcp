---
title: Collective booking metrics (venue geography) for vidoc export
description: Key indicators from the `exp_vidoc__booking_collective_venue` model.
---

{% docs description__exp_vidoc_booking_collective_venue %}

The `exp_vidoc__booking_collective_venue` model provides the confirmed collective (EAC) bookings and their amount by month, scholar year and **cultural partner (venue) territory**. It reads [exp_base__booking_venue_collective](../export_base/description__exp_base__booking_venue_collective.md), where the statistical secret of businesses is applied; values are those of `external_reporting__collective_partner_booking` (`total_reservations_collectives`, `total_montant_engage`).

{% enddocs %}

## Table Description

**Grain**: `partition_month`, `scholar_year`, `territory_level`, `territory_code`.

- One row per month, scholar year and published territory: `DEP`, `REG`, `GROUP` (METRO / DROM / COM) and `NAT`. Read each level from its own rows: never sum departments to get a region or France.
- When `is_statistic_secret` is TRUE, both figures are NULL on every month. NAT is never secret.
- Same rules as [`exp_vidoc__booking_collective`](description__exp_vidoc_booking_collective.md) (school territory, not secret): monthly flows summable over a scholar year, bookings created before the scholar year counted in September, months from January 2022 to the last complete month.
