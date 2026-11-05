---
title: Booking finance individual metrics for vidoc export
description: Key indicators from the `exp_vidoc_booking_finance_individual` model.
---

{% docs description__exp_vidoc_booking_finance__individual %}

The `exp_vidoc_booking_finance_individual` model provides aggregated financial KPIs related to individual bookings.
It is designed to be exported to the ministry for vidoc visualisation.
Financial KPIs are nullified when the department is under statistical secrecy over the last 6 months (`statistical_secret__cultural_partner_individual`, 6-month window): fewer than 3 cultural partners with revenue, one partner above 85% of the revenue, or secondary secrecy.

{% enddocs %}

## Table Description

Each row represents the financial indicators of used individual bookings for a month and a venue department (the venue's location, not the beneficiary's residence).

**Grain**: `partition_month`, `venue_department_code`.

When `is_statistic_secret` is TRUE, all financial KPIs (`total_bookings`, `total_quantities`, `total_revenue_amount`, `total_reimbursed_amount`, `total_contribution_amount`) are set to NULL. `department_secret_reason_code` and `department_secret_reason_label` give the reason. Region and national totals summed from departments therefore leave out the secret departments.
