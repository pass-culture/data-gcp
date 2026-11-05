The `exp_vidoc__booking_collective_by_format` model provides the confirmed collective (EAC) bookings, amounts and tickets by **offer format** (e.g. REPRESENTATION, ATELIER_DE_PRATIQUE), month, scholar year and school department, rolled up from `metrics_booking__collective_by_format`. Bookings, amounts and tickets are the same values as `external_reporting__eac_format` (bookings without a format left out); the school counts come from `metrics_booking__collective_by_format`. An offer can have several formats: its bookings count in each of them, so **formats must not be summed**.

## Table Description

Each row represents the confirmed collective bookings created in a month in an offer format, for a scholar year and a school department.

**Grain**: `partition_month`, `scholar_year`, `collective_offer_format`, `institution_department_code`.

- All columns are **monthly flows**, summable over the months of a scholar year and over departments, never over formats. Totals across formats come from `exp_vidoc__booking_collective`.
- `total_collective_institutions` counts distinct schools in the month; `cumulative_total_collective_institutions` counts distinct schools since the start of the scholar year (read the last month). Both are summable over departments, not over formats.
- Bookings created before their scholar year starts are counted in its first month (September). Months from January 2022 to the last complete month.
- No statistical secret (school geography).
