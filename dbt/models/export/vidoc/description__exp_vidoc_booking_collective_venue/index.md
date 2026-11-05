The `exp_vidoc__booking_collective_venue` model provides the confirmed collective (EAC) bookings and their amount by month, scholar year and **cultural partner (venue) department**, rolled up from `metrics_booking__collective`. It computes the same values, with the same statistical secret, as `external_reporting__collective_partner_booking` (`total_reservations_collectives`, `total_montant_engage`) at department level.

## Table Description

Each row represents the confirmed collective bookings created in a month, for a scholar year and a venue department (where the cultural partner is, not where the school is).

**Grain**: `partition_month`, `scholar_year`, `venue_department_code`.

- Same rules as [`exp_vidoc__booking_collective`](https://pass-culture.github.io/data-gcp/dbt/models/export/vidoc/description__exp_vidoc_booking_collective/index.md): monthly flows summable over a scholar year, bookings created before the scholar year counted in September, months from January 2022 to the last complete month.
- Statistical secret, as in external reporting: a department is secret when it is secret (`is_department_secret_stat`) at the **last complete month**, on the individual or the collective cultural partner secret (`statistical_secret__cultural_partner_individual` / `_collective`), on the 6 or the 48 month window. `total_collective_bookings` and `total_collective_amount_spent` are then NULL on **every month** of the department (`is_statistic_secret` TRUE); `department_secret_reason_label` gives the rules behind the mask, worded as the `secret_detail` of external reporting (e.g. `Moins de 3 partenaires (collectif, 6 mois)`). Region and national totals summed from departments leave out the secret departments: national totals are read from `exp_vidoc__booking_collective`.
