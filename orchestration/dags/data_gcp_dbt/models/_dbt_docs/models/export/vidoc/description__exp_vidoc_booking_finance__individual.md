---
title: Booking finance individual metrics for vidoc export
description: Key indicators from the `exp_vidoc_booking_finance_individual` model.
---

{% docs description__exp_vidoc_booking_finance__individual %}

The `exp_vidoc_booking_finance_individual` model provides aggregated financial KPIs related to individual bookings.
It is designed to be exported to the ministry for vidoc visualisation.
Financial KPIs are nullified when statistical secrecy applies (fewer than 3 active cultural partners in the commune for the given month).

{% enddocs %}

## Table Description

Each row represents financial indicators calculated for a specific month, geographic area, offer category and EPN status.

**Grain**: `partition_month`, `venue_department_code`, `venue_epci_code`, `venue_city_code`, `offerer_is_epn`, `offer_category_id`.

When `is_statistic_secret` is TRUE, all financial KPIs (`total_bookings`, `total_quantities`, `total_revenue_amount`, `total_reimbursed_amount`, `total_contribution_amount`) are set to NULL.
