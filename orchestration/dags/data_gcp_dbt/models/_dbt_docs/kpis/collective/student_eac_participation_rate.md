---
title: Student EAC participation rate
description: How to calculate the share of eligible students engaged in at least one EAC action funded by the collective part of the pass Culture.
---

## Share of eligible students engaged in at least one EAC action

### Definition

Share of the students eligible to the collective part of the pass Culture who took part in at least one EAC (Education Artistique et Culturelle) action since the start of the school year. Both counts are provided by ADAGE (Ministry of Education). Same definition as `taux_participation_eac_jeunes` in `external_reporting__eac_coverage`.

Eligible levels: 4e to Terminale (incl. CAP, SEGPA, MLDS) up to 2022-2023, 6e to Terminale from 2023-2024 (6e and 5e opened at the rentrée 2023). Rates before and after 2023-2024 are not computed on the same population.

### Published in

**VIDOC** (ministry dashboard):

[exp_vidoc_student_coverage](../../models/export/vidoc/description__exp_vidoc_student__coverage.md)

### Calculation

The rate is the ratio of `total_engaged_students` to `total_eligible_students`, on the latest month of each school year (both counts are cumulative since the start of the school year).

```sql
SELECT
    scholar_year,
    partition_month,
    SUM(total_engaged_students) AS engaged_students,
    SUM(total_eligible_students) AS eligible_students,
    SAFE_DIVIDE(
        SUM(total_engaged_students), SUM(total_eligible_students)
    ) AS student_participation_rate
FROM `student_coverage`
GROUP BY scholar_year, partition_month
QUALIFY partition_month = MAX(partition_month) OVER (PARTITION BY scholar_year)
ORDER BY scholar_year
```

### Available dimensions

| Dimension | Column | Description |
|-----------|--------|-------------|
| School year | `scholar_year` | 1 September to 31 August |
| Month | `partition_month` | Month of the ADAGE snapshot (last day of the month) |
| Department | `department_code`, `department_name` | Department of the school |
| Academy | `academy_name` | Academy of the school |
| Region | `region_code`, `region_name` | Region of the school |

### Temporal tracking

**School year**, with one value per month: each month is the situation since the start of the school year (a stock, never sum over months). The final value of a school year is its August month.

### Notes and caveats

- Geography is the school's, not the student's residence.
- During an ongoing school year the rate is partial.
- **Known issue, fix pending**: in the first months of a school year the ADAGE headcounts of some académies are missing (e.g. Toulouse in 2026-2027), which distorts territorial rates. A fix is planned in a separate change.
- Counts are ADAGE aggregates of school headcounts, not individual pass Culture data: they are not perturbed.
