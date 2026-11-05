---
title: Booking finance individual metrics for vidoc export
description: Key indicators from the `exp_vidoc_booking_partner_finance_individual` model.
---

{% docs description__exp_vidoc_booking_partner_finance__individual %}

The `exp_vidoc_booking_partner_finance_individual` model provides aggregated financial KPIs related to individual bookings.
It is designed to be exported to the ministry for vidoc visualisation.
It applies the statistical secret of external reporting (`apply_statistical_secret` in `external_reporting__individual`): a department is secret when it is secret (`is_department_secret_stat`) at the last complete month, for any rolling window (6 or 48 months) of `statistical_secret__cultural_partner_individual` or `statistical_secret__cultural_partner_collective` (fewer than 3 cultural partners with revenue, one partner above 85% of the revenue, or secondary secret). The financial KPIs of a secret department are then NULL on every month.

{% enddocs %}

## Table Description

Each row represents the financial indicators of used individual bookings for a month and a venue department (the venue's location, not the beneficiary's residence).

**Grain**: `partition_month`, `venue_department_code`.

When `is_statistic_secret` is TRUE, all financial KPIs (`total_bookings`, `total_quantities`, `total_revenue_amount`, `total_reimbursed_amount`, `total_contribution_amount`) are NULL on every month. `department_secret_reason_label` lists the reasons, with the side (individual / collective) and the window. The secret status is evaluated at the last complete month, so it can change from one export to the next for the whole history of a department.

- External reporting never masks REG and NAT; this table has the department grain only, so region and national totals summed from it leave out the secret departments (mostly overseas collectivities and Lozère: less than 0.03% of the national revenue in 2026).
- Only bookings already included in a payment batch are counted, on their month of use. Months from January 2022 to the month before the run date are exported, as in external reporting; the last month is about 99% complete when first published and may be slightly revised afterwards.
- `venue_region_code` is NULL for the overseas collectivities (Saint-Martin, Saint-Pierre-et-Miquelon, Nouvelle-Calédonie, ...), which have no region code; `venue_region_name` is filled.
