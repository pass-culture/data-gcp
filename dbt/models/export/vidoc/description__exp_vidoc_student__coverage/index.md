The `exp_vidoc_student__coverage` model provides the number of students eligible for EAC (Education Artistique et Culturelle) and the number of students engaged in at least one EAC action, by school year, month and territory. It is designed to be exported to ministry for vidoc visualisation.

## Table Description

Each row gives, for a school year, a month and a department, the ADAGE counts of eligible and engaged students.

**Grain**: `partition_month`, `scholar_year`, `department_code` (there is exactly one `scholar_year` per `partition_month`; `region_*` and `academy_name` are attributes of the department).

- **Source**: ADAGE statistics (Ministry of Education), last daily snapshot of each month (`metrics_student__coverage`). Only complete months are exported.
- **Cumulative values**: both counts are a stock since the start of the school year (1 September), not a monthly flow. Never sum them over months: take one month, usually the latest `partition_month` of the school year (August for a complete school year).
- **Geography**: the department of the student's school (as reported by ADAGE), not the student's residence.
- **Eligible students**: students of the levels eligible to the collective part of the pass Culture as reported by ADAGE: 4e to Terminale (incl. CAP, SEGPA, MLDS) up to 2022-2023, 6e to Terminale from 2023-2024 (opening of 6e and 5e at the rentrée 2023). The denominator jumps from about 4.1 million to 5.8 million students between these two school years.
- **Known issue, fix pending**: in the first months of a school year the ADAGE headcounts of some académies are missing (e.g. Toulouse in 2026-2027, Limoges and Corse in 2025-2026), so the eligible students of these territories are heavily underestimated. A fix is planned in a separate change; until then, territorial rates of the first months of a school year must not be used.
- **Overseas collectivities** (975, 977, 978) have `region_code` = -1.
- **No perturbation**: the counts are department-level administrative aggregates produced by ADAGE from school headcounts, not counts of pass Culture users built from individual records; there is no cell key for them and the smallest cells (overseas collectivities) are counts of students of whole territories. Same as `external_reporting__eac`, where this KPI is not subject to the statistical secret nor to low-count masking.

| name                    | data_type | description                                                                                                                                  |
| ----------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| partition_month         |           | Indicates the first day of the month the KPI refers to.                                                                                      |
| scholar_year            |           | Scholar year of the deposit.                                                                                                                 |
| region_code             |           | The official code of the region (région) containing the IRIS area.                                                                           |
| region_name             |           | The official name of the region containing the IRIS area.                                                                                    |
| academy_name            |           | The name of the educational academy (académie) associated with the IRIS area.                                                                |
| department_code         |           | The official code of the department (département) containing the IRIS area.                                                                  |
| department_name         |           | The official name of the department containing the IRIS area.                                                                                |
| total_eligible_students |           | Total number of students eligible for EAC (Education Artistique et Culturelle) within the territory for the specified month and school year. |
| total_engaged_students  |           | Total number of students who participated in at least one EAC activity within the territory for the specified month and school year.         |
