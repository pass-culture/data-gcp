---
title: "Vidoc KPIs: Cultural partners"
description: Active and activated cultural partners, and their bookings and revenue from individual bookings by venue territory.
---

## Cultural partners

Active and activated cultural partners, and their bookings and revenue from individual bookings by venue territory.

See the [Vidoc KPIs overview](../index.md) for the data protection rules that apply to every export.

| KPI | Definition | Source table(s) | Tracking |
|-----|-----------|-----------------|----------|
| [Number of active cultural partners by channel](active_cultural_partners.md) | Venues with a bookable offer in the last 365 days: individual only, collective only, both | `exp_vidoc_cultural_partner` | Monthly (rolling 365 days) |
| [Cumulative number of activated cultural partners](activated_cultural_partners.md) | Venues that have had at least one bookable offer since 2022-06-10 | `exp_vidoc_cultural_partner` | Monthly (cumulative) |
| [Number of active and activated cultural partners by type](active_cultural_partners_by_type.md) | Active and activated venues for one partner type (never summed over types) | `exp_vidoc_cultural_partner_by_type` | Monthly (rolling 365 days) |
| [Individual bookings and tickets by venue territory](individual_bookings_venue_territory.md) | Used individual bookings and tickets in a payment batch, by venue department | `exp_vidoc_booking_partner_finance_individual` | Monthly |
| [Individual booking revenue by venue territory](individual_revenue_venue_territory.md) | Partner revenue, reimbursed and contributed amounts, by venue department | `exp_vidoc_booking_partner_finance_individual` | Monthly |
