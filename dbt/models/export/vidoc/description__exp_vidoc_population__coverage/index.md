The `exp_vidoc__population_coverage` model aggregates key performance indicators to quantify population coverage at department level. It is designed to be exported to ministry for vidoc visualisation.

**Population reference:** Coverage indicators are computed against the monthly French population estimation produced by the [`data-insee-population`](https://github.com/pass-culture/data-insee-population/) project. In short, that pipeline takes the 2022 INSEE census as a baseline, projects each birth cohort forward while keeping its geographic distribution stable, anchors totals to INSEE's yearly departmental estimates, and splits cohorts into birth months using regional patterns. Full methodology: [method.md](https://github.com/pass-culture/data-insee-population/blob/main/docs/method.md).

**Related models:**

- [`int_seed__monthy_population_france`](#!/model/model.data_gcp_dbt.int_seed__monthy_population_france) — the seed table holding the population estimation used as denominator here.
- [`metrics_population__coverage`](#!/model/model.data_gcp_dbt.metrics_population__coverage) — upstream metrics model this export is built from.

## Table Description

Each row represents a key indicator calculated for a specific month and a geographic aggregation level.

**Grain**: `partition_month`, `birth_month`, `milestone_age`, `department_code`.

Public INSEE population data — see [data-insee-population](https://github.com/pass-culture/data-insee-population/) for details.

| name                            | data_type | description                                                                                   |
| ------------------------------- | --------- | --------------------------------------------------------------------------------------------- |
| partition_month                 |           | Indicates the first day of the month the KPI refers to.                                       |
| birth_month                     |           | The estimated birth month of individuals based on observed birth trends.                      |
| milestone_age                   |           | The age reached by the user at a specific key milestone, used to determine users eligibility. |
| department_code                 |           | The official code of the department (département) containing the IRIS area.                   |
| department_name                 |           | The official name of the department containing the IRIS area.                                 |
| region_name                     |           | The official name of the region containing the IRIS area.                                     |
| region_code                     |           | The official code of the region (région) containing the IRIS area.                            |
| total_population_last_12_months |           | Rolling sum of french population over the last 12 months for a given age and department.      |
