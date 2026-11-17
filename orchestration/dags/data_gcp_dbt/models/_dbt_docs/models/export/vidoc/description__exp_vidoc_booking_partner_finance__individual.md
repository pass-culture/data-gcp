---
title: Booking finance individual metrics for vidoc export
description: Key indicators from the `exp_vidoc_booking_partner_finance_individual` model.
---

{% docs description__exp_vidoc_booking_partner_finance__individual %}

The `exp_vidoc_booking_partner_finance_individual` model provides the used individual bookings and their amounts by month and venue territory (the venue's location, not the beneficiary's residence).
It is designed to be exported to the ministry for vidoc visualisation. It reads [exp_base__booking_venue_individual](../export_base/description__exp_base__booking_venue_individual.md), where the statistical secret of businesses is applied.

{% enddocs %}

## Table Description

**Grain**: `partition_month`, `territory_level`, `territory_code`.

- One row per month and published territory: departments (`DEP`), regions (`REG`), territory groups METRO / DROM / COM (`GROUP`) and France (`NAT`). Read each level from its own rows: never sum departments to get a region or France.
- When `is_statistic_secret` is TRUE, all figures are NULL on every month. NAT is never secret; the COM group is the way to read the overseas collectivities.
- Only bookings already included in a payment batch are counted, on their month of use. Months from January 2022 to the month before the run date; the last month is about 99% complete when first published and may be slightly revised afterwards.
