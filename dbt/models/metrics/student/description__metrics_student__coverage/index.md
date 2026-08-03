The `metrics_student__coverage` model provides aggregated key indicators related to student participation in EAC (Education Artistique et Culturelle). It is designed to be the central model providing student coverage indicators for export, monitoring and governance.

## Table Description

Each row represents the key indicators related to student EAC participation, calculated for a specific month, school year and geographic aggregation level.

| name                    | data_type | description                                                                                                                                  |
| ----------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| partition_month         | STRING    | Indicates the first day of the month the KPI refers to.                                                                                      |
| scholar_year            | STRING    | Scholar year of the deposit.                                                                                                                 |
| region_code             | INTEGER   | The official code of the region (région) containing the IRIS area.                                                                           |
| region_name             | STRING    | The official name of the region containing the IRIS area.                                                                                    |
| academy_name            | STRING    | The name of the educational academy (académie) associated with the IRIS area.                                                                |
| department_code         | STRING    | The official code of the department (département) containing the IRIS area.                                                                  |
| department_name         | STRING    | The official name of the department containing the IRIS area.                                                                                |
| total_eligible_students | INTEGER   | Total number of students eligible for EAC (Education Artistique et Culturelle) within the territory for the specified month and school year. |
| total_engaged_students  | INTEGER   | Total number of students who participated in at least one EAC activity within the territory for the specified month and school year.         |
