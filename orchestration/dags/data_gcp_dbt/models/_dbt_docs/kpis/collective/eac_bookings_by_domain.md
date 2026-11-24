---
title: EAC bookings by educational domain
description: How to calculate confirmed collective (EAC) bookings, amounts, tickets and involved schools by artistic / educational domain.
---

## Confirmed collective (EAC) bookings by educational domain

### Definition

Confirmed collective bookings (same definition as [EAC collective bookings](eac_collective_bookings.md)) split by the **educational domain** of the booked offer (e.g. Théâtre, Cinéma, Patrimoine), as declared by the cultural partner. Indicators: bookings, amount, tickets, bookings made with a venue labelled by the Ministry of Culture, and distinct schools involved in the month. Same values as `external_reporting__eac_domain`.

**An offer can have several domains** (about 21% of collective offers have 2 or more): its bookings count in **each** of its domains. Domains are therefore **not summable**: in 2025-2026 the sum over domains is 133,227 bookings for 99,019 actual bookings (+35%), and 51.8 M€ for 37.6 M€. Take totals from `exp_vidoc__booking_collective`.

### Published in

**VIDOC** (ministry dashboard):

[exp_vidoc__booking_collective_by_domain](../../models/export/vidoc/description__exp_vidoc_booking_collective_by_domain.md) (school geography)

### Calculation

```sql
SELECT
    scholar_year,
    educational_domain_name,
    SUM(total_collective_bookings) AS collective_bookings,
    SUM(total_collective_booking_amount) AS collective_amount,
    SUM(total_collective_tickets) AS collective_tickets,
    SAFE_DIVIDE(SUM(total_labelled_collective_bookings), SUM(total_collective_bookings)) AS labelled_share,
    -- distinct schools since the start of the scholar year: read the last month
    SUM(IF(partition_month = '2026-08-01', cumulative_total_collective_institutions, 0)) AS schools
FROM `booking_collective_by_domain`
WHERE scholar_year = '2025-2026'
GROUP BY scholar_year, educational_domain_name
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Month | `partition_month` | Creation month of the booking, clamped into its scholar year |
| Scholar year | `scholar_year` | September to August |
| Domain | `educational_domain_name` | Educational domain of the offer |
| Department | `institution_department_code`, `institution_department_name` | Department of the school |
| Region | `institution_region_code`, `institution_region_name` | Region of the school |
| Academy | `institution_academy_name` | Academy of the school |

### Temporal tracking

**Monthly**, within the scholar year, from January 2022 to the last complete month. All columns are flows. `total_collective_institutions` counts distinct schools in the month: summed over months, a school is counted once per month with a booking. `cumulative_total_collective_institutions` counts distinct schools since September (read the last month, never sum months).

### Data protection

Bookings and schools are not counts of people: no cell key perturbation. School geography: no statistical secret.
