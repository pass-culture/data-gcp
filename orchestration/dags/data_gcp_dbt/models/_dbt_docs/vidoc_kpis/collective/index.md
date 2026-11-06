---
title: "Vidoc KPIs: Collective (EAC)"
description: Collective part of the pass Culture (EAC): school and student participation, credit consumption, collective bookings and offers.
---

## Collective (EAC)

Collective part of the pass Culture (EAC): school and student participation, credit consumption, collective bookings and offers.

See the [Vidoc KPIs overview](../index.md) for the data protection rules that apply to every export.

| KPI | Definition | Source table(s) | Tracking |
|-----|-----------|-----------------|----------|
| [Student EAC participation rate](student_eac_participation_rate.md) | Share of eligible students engaged in at least one EAC action | `exp_vidoc_student_coverage` | School year (monthly, cumulative) |
| [Institution EAC participation rate](institution_eac_participation_rate.md) | Share of credited institutions with at least one collective booking | `exp_vidoc_institution_activity` | School year (monthly, cumulative) |
| [EAC credit consumption rate](eac_credit_consumption_rate.md) | Share of the collective credit allocated to institutions that was engaged | `exp_vidoc_institution_activity` | School year (monthly, cumulative) |
| [EAC collective bookings](eac_collective_bookings.md) | Confirmed collective bookings and amount, by school (default) or cultural partner geography | `exp_vidoc_booking_collective` + `exp_vidoc_booking_collective_venue` | Monthly (scholar year) |
| [EAC bookings by educational domain](eac_bookings_by_domain.md) | Confirmed collective bookings, amount, tickets and schools by domain (domains not summable) | `exp_vidoc_booking_collective_by_domain` | Monthly (scholar year) |
| [EAC bookings by offer format](eac_bookings_by_format.md) | Confirmed collective bookings, amount, tickets and schools by format (formats not summable) | `exp_vidoc_booking_collective_by_format` | Monthly (scholar year) |
| [EAC bookable collective offers](eac_bookable_offers.md) | Bookable (non showcase) collective offers created and pre-reserved, and partners creating them | `exp_vidoc_offer_collective_bookable` | Monthly (scholar year) |
