The `metrics_population__coverage` model aggregates key performance indicators to quantify population coverage at department level. It is designed to be the central model providing population coverage indicators for export, monitoring and gouvernance.

## Table Description

Each row represents a key indicator calculated for a specific month and a geographic aggregation level.

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
