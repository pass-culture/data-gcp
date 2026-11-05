The `exp_vidoc__booking_collective` model provides the confirmed collective (EAC) bookings and their amount by month, scholar year and **school (educational institution) department**, rolled up from `metrics_booking__collective_institution`. It computes the same values as the `total_reservations_collectives_etablissements` and `total_montant_engage_etablissements` KPIs of `external_reporting__eac_offer_booking`. It is the vidoc default for collective bookings: it follows where pupils are, like the domain and format splits. The same indicators by cultural partner location are in `exp_vidoc__booking_collective_venue`.

## Table Description

Each row represents the confirmed collective bookings (confirmed, used, pending reimbursement or reimbursed) created in a month, for a scholar year and a school department.

**Grain**: `partition_month`, `scholar_year`, `institution_department_code`.

- All columns are **monthly flows**: sum the months of a scholar year (September to August) for a scholar year total.
- A booking created before its scholar year starts (e.g. in June for the next September) is counted in the first month of that scholar year (September).
- Months from January 2022 to the last complete month, as in external reporting.
- No statistical secret: amounts by school territory are public money spent by schools, not the revenue of cultural partners.
- `institution_department_code` is null for the bookings of schools that are not located (2022-2023 and 2023-2024 only); they are kept so that national totals match.
