## Number and amount of confirmed collective (EAC) bookings

### Definition

Collective bookings are made by schools (EPLE) on the pass Culture EAC share for a class, on a bookable collective offer of a cultural partner. A booking is counted when it is **confirmed**: status confirmed, used, pending reimbursement or reimbursed (pre-reservations not yet confirmed by the school and cancelled bookings are excluded).

- **Bookings (`total_collective_bookings`)**: number of confirmed collective bookings.
- **Amount (`total_collective_amount_spent`)**: booking amount, in euros, charged to the school's EAC credit.

A booking is counted in the month it was **created**, within its scholar year (September to August). A booking created **before its scholar year starts** (e.g. in June for an action in the next school year) is counted in the **first month of that scholar year (September)**.

Same values as the external reporting KPIs `total_reservations_collectives_etablissements` / `total_montant_engage_etablissements` (school geography) and `total_reservations_collectives` / `total_montant_engage` (cultural partner geography).

### Published in

**VIDOC** (ministry dashboard):

Two geographies are available; **the school geography is the vidoc default**.

- [exp_vidoc\_\_booking_collective](https://pass-culture.github.io/data-gcp/dbt/models/export/vidoc/description__exp_vidoc_booking_collective/index.md): by **school** department (where pupils are), consistent with the domain and format splits; no statistical secret.
- [exp_vidoc\_\_booking_collective_venue](https://pass-culture.github.io/data-gcp/dbt/models/export/vidoc/description__exp_vidoc_booking_collective_venue/index.md): by **cultural partner (venue)** department, under the statistical secret.

National totals are the same on both sides (except the venue departments masked by the secret).

### Calculation

All columns are monthly flows: sum the months of the scholar year (or up to the selected month for a year-to-date value).

```
SELECT
    scholar_year,
    institution_region_code,
    institution_region_name,
    SUM(total_collective_bookings) AS collective_bookings,
    SUM(total_collective_amount_spent) AS collective_amount,
    SAFE_DIVIDE(SUM(total_collective_amount_spent), SUM(total_collective_bookings)) AS average_booking_amount
FROM `booking_collective`
WHERE scholar_year = '2025-2026'
GROUP BY scholar_year, institution_region_code, institution_region_name
```

Scholar year 2025-2026 (national): 99,019 bookings, 37.6 M€ (2024-2025: 166,522 bookings, 87.1 M€).

### Available dimensions

| Dimension    | Column                                                                                      | Description                                                  |
| ------------ | ------------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| Month        | `partition_month`                                                                           | Creation month of the booking, clamped into its scholar year |
| Scholar year | `scholar_year`                                                                              | September to August                                          |
| Department   | `institution_department_code`, `institution_department_name` (`venue_*` in the venue table) | Department of the school (or of the cultural partner)        |
| Region       | `institution_region_code`, `institution_region_name` (`venue_*` in the venue table)         | Region of the school (or of the cultural partner)            |
| Academy      | `institution_academy_name`                                                                  | Academy of the school (school table only)                    |

### Temporal tracking

**Monthly**, within the scholar year, from January 2022 to the last complete month. September includes all bookings created before the scholar year.

### Data protection

Bookings are not counts of people: no cell key perturbation.

- **School geography**: no secret (public money spent by schools).
- **Venue geography**: amounts by venue territory are the collective revenue of the partners located there. As in external reporting, a department is masked (`is_statistic_secret` TRUE, indicators NULL on every month, rules in `department_secret_reason_label`) when it is under the individual or collective cultural partner secret at the last complete month (6 or 48 month window). Region totals summed from departments leave out the masked departments.
