This table presents the total number of young individuals and the number of beneficiaries for each age group per department at a given active month.

### **Business Rules**

- Each row is uniquely identified by (`population_snapshot_month`, `population_decimal_age`, `population_department_code`).
- The dataset captures the evolution of beneficiaries over time and enables analysis of coverage trends.

### **Example Usage**

This table is used to:

1. Track the proportion of young beneficiaries in various age brackets.
1. Analyze the population coverage per department.
1. Identify seasonal trends enrollment MoM.

### **Sources**

- `int_global__daily_deposit`: Tracks daily user deposits.
- `int_seed__monthly_insee_population_estimation`: Provides population estimates by age and department.

## Table description

| name                            | data_type | description                                                                                                                                                                                                                                                                                           |
| ------------------------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| population_snapshot_month       | DATE      | Month of the population snapshot, as the first day of the month. Population estimates cover whole years up to the last valid INSEE year (INSEE_DATA_LAST_VALID_YEAR), so months after the current one can be present: they carry an estimated population and no observed facts (such as user counts). |
| population_birth_month          | DATE      | The estimated birth month of individuals based on observed birth trends.                                                                                                                                                                                                                              |
| population_decimal_age          | STRING    | The estimated age of the population in decimal format for more granularity (month over month).                                                                                                                                                                                                        |
| population_age_decimal_set      | BOOLEAN   | Boolean. TRUE when population_decimal_age is one of the half-year milestones 15, 15.5, 16, 16.5, 17, 17.5, 18, 18.5 or 19; FALSE for all other rows.                                                                                                                                                  |
| population_age_bracket          | STRING    | Categorization of the population by age groups:                                                                                                                                                                                                                                                       |
|                                 |           | - `15_17` for ages 15-17                                                                                                                                                                                                                                                                              |
|                                 |           | - `18_19` for ages 18-19                                                                                                                                                                                                                                                                              |
|                                 |           | - `20_25` for ages 20-25                                                                                                                                                                                                                                                                              |
| population_department_code      | STRING    | The official department code in France.                                                                                                                                                                                                                                                               |
| population_department_name      | STRING    | The official name of the department.                                                                                                                                                                                                                                                                  |
| population_region_name          | STRING    | The name of the region in which the department is located.                                                                                                                                                                                                                                            |
| population_academy_name         | STRING    | The name of the educational academy associated with the department.                                                                                                                                                                                                                                   |
| total_users                     | INTEGER   | Number of distinct users with an active deposit (any deposit type) during the month, for the row's birth month and department (decimal-age grain). Only the last 48 months are computed.                                                                                                              |
| total_population                | INTEGER   | INSEE estimated population for the row's snapshot month, birth month and department (decimal-age grain). A population count, not a count of registered users.                                                                                                                                         |
| total_users_last_12_months      | INTEGER   | Rolling sum of distinct users over the last 12 months for a given age and department: the sum of monthly distinct-user counts over the current and 11 previous months, so a user active in several months is counted once per month.                                                                  |
| total_population_last_12_months | INTEGER   | Rolling sum of french population over the last 12 months for a given age and department.                                                                                                                                                                                                              |
