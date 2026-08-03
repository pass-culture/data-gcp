The `metrics_booking__collective` model provides aggregated key indicators related to collective bookings by venue geography and school year. It is designed to be the central model providing collective booking indicators for export, monitoring and governance.

## Table Description

Each row represents the key indicators related to collective bookings, calculated for a specific month, school year and venue geographic level (department / region / city / EPCI).

| name                                     | data_type | description                                                                                                                                                         |
| ---------------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| partition_month                          | STRING    | Indicates the first day of the month the KPI refers to.                                                                                                             |
| scholar_year                             | STRING    | Scholar year of the deposit.                                                                                                                                        |
| venue_region_name                        | STRING    | Region name where the venue is located.                                                                                                                             |
| venue_region_code                        | STRING    | Region code where the venue is located.                                                                                                                             |
| venue_department_name                    | STRING    | Department name where of the venue.                                                                                                                                 |
| venue_department_code                    | STRING    | Department code of the venue.                                                                                                                                       |
| venue_epci_name                          | STRING    | EPCI name of the venue.                                                                                                                                             |
| venue_epci_code                          | STRING    | EPCI code of the venue.                                                                                                                                             |
| venue_city_name                          | STRING    | City where the venue is located.                                                                                                                                    |
| venue_city_code                          | STRING    | City code where the venue is located.                                                                                                                               |
| total_collective_bookings                | INTEGER   | Total number of collective bookings.                                                                                                                                |
| total_collective_amount_spent            | FLOAT     | Total amount spent for collective bookings.                                                                                                                         |
| cumulative_total_collective_bookings     | INTEGER   | Cumulative total number of collective bookings for the same venue geography from the start of the given school year up to and including the specified month.        |
| cumulative_total_collective_amount_spent | FLOAT     | Cumulative total amount spent for collective bookings for the same venue geography from the start of the given school year up to and including the specified month. |
