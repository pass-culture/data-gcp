## Beneficiary coverage rate among eligible youth

### Definition

Proportion of eligible young people who have benefited or are benefiting from a pass Culture credit over a given period and territory.

### Published in

**VIDOC** (ministry dashboard):

- [exp_vidoc_beneficiary_coverage](https://pass-culture.github.io/data-gcp/dbt/models/export/vidoc/description__exp_vidoc_beneficiary__coverage/index.md) — beneficiary count (rolling 12-month window)
- [exp_vidoc_population_coverage](https://pass-culture.github.io/data-gcp/dbt/models/export/vidoc/description__exp_vidoc_population__coverage/index.md) — eligible population count (rolling 12-month window)

**[data.gouv.fr](https://pass-culture.github.io/data-gcp/dbt/data_gouv/index.md)** (open data): [`beneficiary_coverage`](https://pass-culture.github.io/data-gcp/dbt/models/export/data_gouv/description__exp_data_gouv__beneficiary_coverage/index.md) (numerator only: the INSEE population is not republished)

### Calculation

The coverage rate is the ratio of beneficiaries to eligible population, joined on the same month, age, and department.

```
SELECT
    b.partition_month,
    b.department_code,
    b.department_name,
    b.milestone_age,
    SUM(b.total_beneficiaries_last_12_months) AS total_beneficiaries,
    SUM(p.total_population_last_12_months) AS total_population,
    SAFE_DIVIDE(
        SUM(b.total_beneficiaries_last_12_months),
        SUM(p.total_population_last_12_months)
    ) AS coverage_rate
FROM `beneficiary_coverage` AS b
INNER JOIN `population_coverage` AS p
    ON b.partition_month = p.partition_month
    AND b.department_code = p.department_code
    AND b.milestone_age = p.milestone_age
GROUP BY b.partition_month, b.department_code, b.department_name, b.milestone_age
ORDER BY b.partition_month, b.department_code, b.milestone_age
```

### Available dimensions

| Dimension       | Column                               | Description                                                                                            |
| --------------- | ------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| Age (milestone) | `milestone_age`                      | Age reached over the 12 months (16 to 19): milestone N gives the coverage rate (N-1)+, e.g. 19 for 18+ |
| Department      | `department_code`, `department_name` | Department of residence                                                                                |
| Region          | `region_code`, `region_name`         | Region of residence                                                                                    |

### Temporal tracking

**Monthly, over a rolling 12-month window.** Each `partition_month` counts the beneficiaries and the population of the 12 months ending with that month (`total_beneficiaries_last_12_months` / `total_population_last_12_months`).

The series starts with the first complete 12-month window after the first credits (2019): months whose window is incomplete are not published. 2019-2021 figures cover the pilot phase (a few departments) before the national opening in May 2021.

### Data protection

Beneficiary counts are protected using the [Cell Key Perturbation method](https://pass-culture.github.io/data-gcp/dbt/references/statistical_confidentiality/index.md). Population counts are public data and are not perturbed. Coverage rates computed on small departments may be slightly affected by the perturbation on the numerator.

> The rate **can slightly exceed 100%**, even without perturbation: the denominator is an INSEE population estimate, not a superset of the beneficiaries (e.g. department 22, age 19: ~6,800 beneficiaries for ~6,650 estimated residents). It is therefore not capped, unlike the nested counts of the other exports.
