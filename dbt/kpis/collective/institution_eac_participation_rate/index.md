## Share of credited institutions engaged in the collective part

### Definition

Share of the institutions (schools) credited for a school year that made at least one non-cancelled collective booking since the start of that school year. Same definition as `taux_participation_eac_eple` in `external_reporting__eac_institution`.

### Published in

**VIDOC** (ministry dashboard):

[exp_vidoc_institution_activity](https://pass-culture.github.io/data-gcp/dbt/models/export/vidoc/description__exp_vidoc_institution__activity/index.md)

### Calculation

The rate is the ratio of `total_engaged_institutions` to `total_institutions`, on the latest month of each school year.

```
SELECT
    scholar_year,
    partition_month,
    SUM(total_engaged_institutions) AS engaged_institutions,
    SUM(total_institutions) AS credited_institutions,
    SAFE_DIVIDE(
        SUM(total_engaged_institutions), SUM(total_institutions)
    ) AS institution_participation_rate
FROM `institution_activity`
GROUP BY scholar_year, partition_month
QUALIFY partition_month = MAX(partition_month) OVER (PARTITION BY scholar_year)
ORDER BY scholar_year
```

### Available dimensions

| Dimension   | Column                               | Description                   |
| ----------- | ------------------------------------ | ----------------------------- |
| School year | `scholar_year`                       | 1 September to 31 August      |
| Month       | `partition_month`                    | End of month situation        |
| Department  | `department_code`, `department_name` | Department of the institution |
| Academy     | `academy_name`                       | Academy of the institution    |
| Region      | `region_code`, `region_name`         | Region of the institution     |

### Temporal tracking

**School year**, with one value per month: `total_engaged_institutions` is cumulative since the start of the school year, never sum over months.

### Notes and caveats

- `total_ever_credited_institutions` gives the institutions credited at least once since the generalisation (2021-2022).
- Institutions are public bodies, not people: counts are not perturbed.
