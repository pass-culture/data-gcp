---
title: EAC bookings by offer format
description: How to calculate confirmed collective (EAC) bookings, amounts and tickets by offer format.
---

## Confirmed collective (EAC) bookings by offer format

### Definition

Confirmed collective bookings (same definition as [EAC collective bookings](eac_collective_bookings.md)) split by the **format** of the booked offer (e.g. REPRESENTATION, ATELIER_DE_PRATIQUE, VISITE_GUIDEE), as declared by the cultural partner. Indicators: bookings, amount and tickets (same values as `external_reporting__eac_format`), and distinct schools involved.

**An offer can have several formats** (11% of the 2025-2026 bookings): its bookings count in **each** of its formats. Formats are therefore **not summable**: in 2025-2026 the sum over formats is 113,189 bookings for 99,019 actual bookings (+14%). Take totals from `exp_vidoc__booking_collective`.

### Source table

[exp_vidoc__booking_collective_by_format](../../models/export/vidoc/description__exp_vidoc_booking_collective_by_format.md) (school geography)

### Calculation

```sql
SELECT
    scholar_year,
    collective_offer_format,
    SUM(total_collective_bookings) AS collective_bookings,
    SUM(total_collective_booking_amount) AS collective_amount,
    SUM(total_collective_tickets) AS collective_tickets,
    SUM(IF(partition_month = '2026-08-01', cumulative_total_collective_institutions, 0)) AS schools
FROM `booking_collective_by_format`
WHERE scholar_year = '2025-2026'
GROUP BY scholar_year, collective_offer_format
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| Month | `partition_month` | Creation month of the booking, clamped into its scholar year |
| Scholar year | `scholar_year` | September to August |
| Format | `collective_offer_format` | One format of the offer |
| Department | `institution_department_code`, `institution_department_name` | Department of the school |
| Region | `institution_region_code`, `institution_region_name` | Region of the school |
| Academy | `institution_academy_name` | Academy of the school |

### Temporal tracking

**Monthly**, within the scholar year, from January 2022 to the last complete month. Bookings, amounts and tickets are flows (sum the months). `total_collective_institutions` is distinct schools in the month; `cumulative_total_collective_institutions` is distinct schools since September (read the last month).

### Data protection

Bookings are not counts of people: no cell key perturbation. School geography: no statistical secret.
