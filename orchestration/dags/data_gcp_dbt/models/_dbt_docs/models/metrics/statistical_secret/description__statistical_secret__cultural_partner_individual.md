---
title: Statistical Secret Cultural Partner Individual
description: Description of the `statistical_secret__cultural_partner_individual` table.
---

{% docs description__statistical_secret__cultural_partner_individual %}

Statistical secrecy of cultural partners' revenue from **individual** bookings, by territory: whether a department, an
EPCI or a municipality can be published. One row per `partition_month` × `rolling_window_months_count` (6 or 48
months) × `municipality_code`, for every municipality of the IRIS reference (most have no revenue). Revenue is
the intermediary amount (`booking_intermediary_amount`) of used bookings over the window ending with the month, by venue territory.

The department and EPCI columns repeat on every municipality row of that department or EPCI: read them once per
code (`select distinct`), never sum them. Always filter one `rolling_window_months_count`. The current month is
partial. The table carries no indicator: it says whether a revenue figure for a territory may be published.

{% enddocs %}

## Table description

Built by the `evaluate_statistical_secret` macro on `int_global__booking`, from 2022-01 on.

For each territory and window:

1. **Primary secret** when the territory has fewer than 3 active partners (distinct venues with at least one used
   booking over the window), or when its top partner holds more than 85% of the revenue. A territory with no
   revenue is secret (`no_revenue`).
2. **Secondary secret**: when exactly one municipality is under primary secret in its EPCI, or one EPCI in its
   department, the non-secret territory with the smallest revenue in that parent is masked too
   (`secondary_secret`). For departments, the masked departments of each region, taken together, must pass the
   primary rule, so that they cannot be recovered by subtraction from the region total; otherwise the visible
   departments of the region are masked too, smallest revenue first. The same is then done within each territory
   group (`territory_group`: METRO, DROM, COM), so that an overseas department never masks a mainland one. A department code missing from the
   referential (a venue geocoded abroad) forms its own region ('non localisé') and group (`NON_LOCALISE`), with a
   single row per month and window (no municipality), so that it never masks a real department.
3. **Region secret** (`is_region_secret_stat`): a region is secret when its masked departments still fail the
   primary rule, or when one of its departments was masked at the territory group step.
   `is_territory_group_secret_stat` is true when a group total cannot be published either.

`*_secret_reason_code` gives the reason (`no_revenue`, `low_partners`, `high_concentration`, `both`,
`secondary_secret`, `none`); `*_secret_reason_label` is its French display label.

This rule differs from the `is_statistic_secret` flag of `metrics_booking__finance_individual`: at most 3 active
partners in the city (venues with a bookable offer over 365 days, from `int_kpi__cultural_partner_activation`),
with no concentration test and no secondary secret.
