The `exp_vidoc__booking_collective_by_domain` model provides the confirmed collective (EAC) bookings, amounts, tickets, bookings with a labelled venue and involved schools by **educational domain**, month, scholar year and school department, rolled up from `metrics_booking__collective_by_domain`. It computes the same values as `external_reporting__eac_domain` (17 domains of `get_domains`). An offer can have several domains: its bookings count in each of them, so **domains must not be summed**.

## Table Description

Each row represents the confirmed collective bookings created in a month in an educational domain, for a scholar year and a school department.

**Grain**: `partition_month`, `scholar_year`, `educational_domain_name`, `institution_department_code`.

- All columns are **monthly flows**, summable over departments, never over domains. Totals across domains come from `exp_vidoc__booking_collective`.
- `total_collective_institutions` is the number of distinct schools with a booking in the domain in the month (external reporting `total_eple_impliquees`); summed over months, a school is counted once per month. `cumulative_total_collective_institutions` (from `metrics_booking__collective_by_domain`) is the number of distinct schools with a booking in the domain since the start of the scholar year: read the last month, do not sum months. Both are summable over departments (a school is in one department), not over domains.
- Bookings created before their scholar year starts are counted in its first month (September). Months from January 2022 to the last complete month.
- No statistical secret (school geography).
