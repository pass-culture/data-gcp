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
- `int_seed__monthly_france_population`: Provides population estimates by age and department.

## Table description

| name                            | data_type | description                                                                                                                |
| ------------------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------- |
| population_snapshot_month       | DATE      | Month of the population snapshot in order to track estimated changes month over month time of the whole French population. |
| population_birth_month          | DATE      | The estimated birth month of individuals based on observed birth trends.                                                   |
| population_decimal_age          | STRING    | The estimated age of the population in decimal format for more granularity (month over month).                             |
| population_age_decimal_set      | BOOLEAN   | Boolean indicator specifying whether the age falls on a set decimal value (e.g., 15, 15.5, 16).                            |
| population_age_bracket          | STRING    | Categorization of the population by age groups:                                                                            |
|                                 |           | - `15_17` for ages 15-17                                                                                                   |
|                                 |           | - `18_19` for ages 18-19                                                                                                   |
|                                 |           | - `20_25` for ages 20-25                                                                                                   |
| population_department_code      | STRING    | The official department code in France.                                                                                    |
| population_department_name      | STRING    | The official name of the department.                                                                                       |
| population_region_name          | STRING    | The name of the region in which the department is located.                                                                 |
| population_academy_name         | STRING    | The name of the educational academy associated with the department.                                                        |
| total_users                     | INTEGER   | The total number of users.                                                                                                 |
| total_population                | INTEGER   | The total number of french population.                                                                                     |
| total_users_last_12_months      | INTEGER   | Rolling sum of distinct users over the last 12 months for a given age and department.                                      |
| total_population_last_12_months | INTEGER   | Rolling sum of french population over the last 12 months for a given age and department.                                   |
