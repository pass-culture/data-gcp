The `exp_vidoc_beneficiary__coverage` model provides aggregated key indicators related to pass Culture beneficary coverage. It is designed to be exported to ministry for vidoc visualisation.

## Table Description

Each row represents a key indicator calculated for a specific month, geographic aggregation and beneficiary dimensions level.

**Grain**: `partition_month`, `region_code`, `department_code`, `milestone_age`.

| name                               | data_type | description                                                                                   |
| ---------------------------------- | --------- | --------------------------------------------------------------------------------------------- |
| partition_month                    |           | Indicates the first day of the month the KPI refers to.                                       |
| region_name                        |           | The official name of the region containing the IRIS area.                                     |
| region_code                        |           | The official code of the region (région) containing the IRIS area.                            |
| department_name                    |           | The official name of the department containing the IRIS area.                                 |
| department_code                    |           | The official code of the department (département) containing the IRIS area.                   |
| milestone_age                      |           | The age reached by the user at a specific key milestone, used to determine users eligibility. |
| total_beneficiaries_last_12_months |           | Rolling sum of distinct users over the last 12 months for a given age and department.         |
