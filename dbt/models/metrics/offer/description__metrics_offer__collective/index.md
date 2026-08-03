The `metrics_offer__collective` model provides aggregated key indicators related to collective offer creation by venue geography. It is designed to be the central model providing collective offer creation indicators for export, monitoring and governance.

## Table Description

Each row represents the key indicators related to collective offer creation, calculated for a specific month and venue geographic level (department / region / city / EPCI).

| name                            | data_type | description                                             |
| ------------------------------- | --------- | ------------------------------------------------------- |
| partition_month                 | STRING    | Indicates the first day of the month the KPI refers to. |
| venue_region_name               | STRING    | Region name where the venue is located.                 |
| venue_region_code               | STRING    | Region code where the venue is located.                 |
| venue_department_name           | STRING    | Department name where of the venue.                     |
| venue_department_code           | STRING    | Department code of the venue.                           |
| venue_epci_name                 | STRING    | EPCI name of the venue.                                 |
| venue_epci_code                 | STRING    | EPCI code of the venue.                                 |
| venue_city_name                 | STRING    | City where the venue is located.                        |
| venue_city_code                 | STRING    | City code where the venue is located.                   |
| total_created_collective_offers | INTEGER   | Total number of collective offers created.              |
