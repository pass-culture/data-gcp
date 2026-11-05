---
title: Institution EAC activity metrics for vidoc export
description: Key indicators from the `exp_vidoc_institution__activity` model.
---

{% docs description__exp_vidoc_institution__activity %}

The `exp_vidoc_institution__activity` model provides, for the collective part of the pass Culture, the number of credited and engaged institutions (schools), the credit allocated to them and the credit they consumed, by school year, month and territory.
It is designed to be exported to ministry for vidoc visualisation.

{% enddocs %}

## Table Description

Each row gives, for a school year, a month and a department, the key indicators of the institutions located in that department.

**Grain**: `partition_month`, `scholar_year`, `department_code` (`region_*` and `academy_name` are attributes of the department). Only complete months are exported.

- **Geography**: the institution's (school's) location, not the students' or the cultural partners'.
- **Monthly stocks, not flows**: every value is a stock at the end of the month for the school year; never sum over months, take one month (usually the latest `partition_month` of the school year, August for a complete school year).
    - `total_institutions`: institutions credited for the school year (constant over the school year, apart from deposits created later);
    - `total_engaged_institutions`: credited institutions with at least one non-cancelled booking since the start of the school year (cumulative);
    - `total_ever_credited_institutions`: institutions credited at least once since the generalisation (cumulative);
    - `total_deposit`: credit allocated for the whole school year, repeated on every month;
    - `cumulative_amount_spent`: credit engaged since the start of the school year (confirmed, used or reimbursed bookings, by month of booking creation) — the "consumed credit" also used by the external reporting;
    - `cumulative_reimbursed_amount`: amount of the reimbursed bookings since the start of the school year, by month of use.
- **Ongoing school year**: the consumption rate `cumulative_amount_spent / total_deposit` is partial until the end of the school year.
- **Unknown geography**: bookings of institutions without a known department are kept on a row with a null `department_code` and no institution or credit, so that national sums match the source. Department and region codes are derived from the department name (`region_department` seed); overseas collectivities with no region code have `region_code` = -1.
- **No perturbation, no statistical secret**: institutions are public bodies, not people, and the amounts are the spending of all the schools of a department, not the revenue of a cultural partner. Same as `external_reporting__eac`, where these KPIs are not subject to the statistical secret nor to low-count masking.
- **No credited institution**: a territory with no institution credited for the school year (e.g. Polynésie Française in 2026-2027) has `total_institutions` = 0 and `total_deposit` = 0, so its rates are undefined (null in `external_reporting__eac`).

{% docs table__exp_vidoc__institution_activity %}{% enddocs %}
