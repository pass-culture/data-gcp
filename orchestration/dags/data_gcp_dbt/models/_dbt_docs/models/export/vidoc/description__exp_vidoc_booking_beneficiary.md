---
title: Individual booking metrics by beneficiary territory for vidoc export
description: Key indicators from the `exp_vidoc_booking_beneficiary` model.
---

{% docs description__exp_vidoc_booking_beneficiary %}

The `exp_vidoc_booking_beneficiary` model provides the used individual bookings and the amounts spent by young people, by month of use, territory of residence of the beneficiary and offer category.
It is designed to be exported to the ministry for vidoc visualisation.
It reads [exp_base__booking_beneficiary](../export_base/description__exp_base__booking_beneficiary.md), where the low count rule is applied.

{% enddocs %}

## Table Description

**Grain**: `partition_month`, `territory_level`, `territory_code`, `offer_category_id`.

- One row per month, published territory (`DEP`, `REG`, `GROUP` METRO / DROM / COM, `NAT`) and category. Rows with `offer_category_id = 'ALL'` hold the total, all categories: **do not sum them with the category rows**. Read each level from its own rows: never sum departments to get a region or France.
- The territory is the **current** address of the beneficiary. Beneficiaries without a located address are counted in France only.
- When `is_low_count` is TRUE, both figures are NULL on every month (rules in the base).
- Months from January 2022 to the month before the run date; rows only for cells with at least one used booking.

{% docs table__exp_vidoc__booking_beneficiary %}{% enddocs %}
