---
title: Individual bookings by beneficiary territory (export base)
description: Description of the `exp_base__booking_beneficiary` table, the base of the beneficiary-side booking exports, under the low count rule.
---

{% docs description__exp_base__booking_beneficiary %}

# Table: Individual bookings by beneficiary territory (export base)

The `exp_base__booking_beneficiary` table holds the used individual bookings and the amounts spent by young people, by month of use, **territory of residence** and offer category, at every published level (DEP, REG, GROUP, NAT). It is the single place where the low count rule of these figures is applied.

{% enddocs %}

{% docs column__exp_base__booking_beneficiary__is_low_count %}
True when the row is masked by the low count rule: 1 to 4 beneficiaries booked since 2022 (in the category for a category row), 1 to 19 bookers in the territory for a category row, a secondary mask (a department hidden so that a parent total does not give a small masked set by subtraction), or, for a region, group or France, hidden departments with 1 to 4 bookers together that no other department can cover. `total_used_bookings` and `total_used_amount` are then NULL on every month.
{% enddocs %}

## Table description

**Grain**: `partition_month`, `territory_level`, `territory_code`, `offer_category_id`. Rows with `offer_category_id = 'ALL'` hold the total, all categories: do not sum them with the category rows.

Bookings and amounts are not counts of people and cannot be perturbed. They follow the **low count rule** of external reporting, on the distinct beneficiaries who booked since 2022 (sum of the new bookers of `metrics_booking__individual_bookers`):

- **primary**: the `ALL` row is masked when 1 to 4 beneficiaries account for it; a category row when 1 to 4 beneficiaries booked in the category, or when the territory has 1 to 19 bookers (category breakdown);
- **secondary**: for each region, then territory group, then France, when the hidden departments of the parent have 1 to 4 bookers together, the parent total would give them by subtraction. The visible departments of the parent are hidden from the smallest up until the hidden set has at least 5 bookers; when none can be added, the parent is masked. Departments missing from the referential (`non localisé`) are never published and count as hidden for France.

Masked rows are NULL on every month. Each level is computed on unmasked data and published on its own rows: never sum a level to get another one. Overseas collectivities have no region: read them as a whole from the `GROUP` row `COM`.

Same sources as external reporting (`external_reporting__beneficiary_booking`): current address of the beneficiary (`mrt_global__user_beneficiary`), bookings of users missing from it (deleted, suspended, GRANT_FREE) left out, categories of `get_categories()`, months from January 2022 to the month before the run date, rows only for cells with at least one used booking.

Read by [exp_vidoc_booking_beneficiary](../vidoc/description__exp_vidoc_booking_beneficiary.md). See [Statistical confidentiality](../../../references/statistical_confidentiality.md).

{% docs table__exp_base__booking_beneficiary %}{% enddocs %}
