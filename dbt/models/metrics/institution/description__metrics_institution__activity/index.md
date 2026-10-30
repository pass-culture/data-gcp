The `metrics_institution__activity` model provides aggregated key indicators related to pass Culture institutions on the collective part. It is designed to be the central model providing institutions activity indicators for export, monitoring and gouvernance.

## Table Description

Each row represents the key indicators related to institution activity and collective part of the pass Culture, calculated for a specific month, a sholar year and geographic aggregation level.

| name                          | data_type | description                                                                                                                                                          |
| ----------------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| partition_month               |           | Indicates the first day of the month the KPI refers to.                                                                                                              |
| scholar_year                  |           | Scholar year of the deposit.                                                                                                                                         |
| institution_region_name       |           | Region name of the educational institution.                                                                                                                          |
| institution_academy_name      |           | Academy name of the educational institution.                                                                                                                         |
| institution_department_name   |           | Department name of the educational institution.                                                                                                                      |
| institution_epci_code         |           | EPCI code of the educational institution.                                                                                                                            |
| institution_city_code         |           | INSEE code of the city where the educational institution is located, at the arrondissement level for Paris, Lyon and Marseille (e.g. `75101`).                       |
| total_institutions            |           | Total number of institutions (schools) registered or eligible within the territory (at the given geographical granularity), for the specified month and school year. |
| total_engaged_institutions    |           | Total cumulative number of "engaged" institutions (having made at least one booking) within the territory for the specified month and school year.                   |
| total_deposit                 |           | Total credit amount allocated and made available to the institutions of the territory for the specified month and school year.                                       |
| total_amount_spent            |           | Total amount spent (consumed) by the institutions of the territory during the specified month and school year.                                                       |
| total_bookings                |           | Total number of bookings made by the institutions or beneficiaries of the territory during the specified month and school year.                                      |
| cumulative_total_amount_spent |           | Cumulative total amount spent by the institutions of the territory from the start of the given school year up to and including the specified month.                  |
| cumulative_total_bookings     |           | Cumulative total number of bookings made within the territory from the start of the given school year up to and including the specified month.                       |
