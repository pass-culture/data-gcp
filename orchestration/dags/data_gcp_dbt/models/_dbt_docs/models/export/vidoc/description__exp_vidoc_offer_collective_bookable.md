---
title: Bookable collective offer metrics for vidoc export
description: Key indicators from the `exp_vidoc__offer_collective_bookable` model.
---

{% docs description__exp_vidoc_offer_collective_bookable %}

The `exp_vidoc__offer_collective_bookable` model provides the bookable collective (EAC) offers created, the ones pre-reserved and the cultural partners creating them, by month, scholar year and **cultural partner (venue) department**, rolled up from `metrics_offer__collective_bookable`.
It computes the same values as the `total_offres_reservables_creees`, `total_offres_reservables_prereservees` and `total_partenaires_offres_reservables` KPIs of `external_reporting__eac_offer_booking`.
Only bookable offers are counted: showcase (template, "vitrine") offers are excluded.

{% enddocs %}

## Table Description

Each row represents the bookable collective offers created in a month, for a scholar year and a venue department.

**Grain**: `partition_month`, `scholar_year`, `venue_department_code`.

- An offer belongs to the scholar year of its action (stock beginning date); an offer created before that scholar year starts is counted in its first month (September).
- A bookable offer is made for one school and has in practice at most one non-cancelled booking (5 exceptions out of about 594,000 booked offers), so an offer is counted once; `total_prereserved_bookable_collective_offers` counts the offers created in the month that have a non-cancelled booking.
- All columns are **monthly flows**, summable over the months of a scholar year and over departments. `new_bookable_offer_partners` counts a partner in the month of its first bookable offer of the scholar year, so its sum over the months gives the number of distinct partners.
- Months from January 2022 to the last complete month.
- No statistical secret: offer and partner counts are not revenue (external reporting does not publish these KPIs at department level, and does not mask them at region and academy level).
