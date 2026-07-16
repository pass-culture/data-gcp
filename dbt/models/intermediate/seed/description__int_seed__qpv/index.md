The `QPV` table contains data on areas defined as priority neighborhoods under France's urban policy (QPV) from [INSEE's 2014 classification](https://www.data.gouv.fr/en/datasets/quartiers-prioritaires-de-la-politique-de-la-ville-qpv/) These neighborhoods are identified as some of the most socio-economically disadvantaged in the country, facing significant challenges such as high unemployment rates, lower income levels, and limited access to services.

## Table description

| name                | data_type | description                                                                                                |
| ------------------- | --------- | ---------------------------------------------------------------------------------------------------------- |
| qpv_department_code | STRING    | The official department code representing the administrative division.                                     |
| qpv_code            | STRING    | The official code for the priority urban district (quartier prioritaire de la politique de la ville, QPV). |
| qpv_municipality    | STRING    | Names of the municipalities associated with the QPV (priority urban district).                             |
| qpv_name            | STRING    | The name of the priority urban district (QPV).                                                             |
| qpv_department_name | STRING    | The official name of the department in which the QPV is located.                                           |
| insee_code          | STRING    | The INSEE code identifying the municipality or area.                                                       |
| qpv_geo_shape       | GEOGRAPHY | The geographical shape data representing the boundaries of the QPV as a GEOGRAPHY object.                  |
| qpv_min_longitude   | FLOAT     | The minimum longitude of the bounding box surrounding the QPV geographical shape.                          |
| qpv_max_longitude   | FLOAT     | The maximum longitude of the bounding box surrounding the QPV geographical shape.                          |
| qpv_min_latitude    | FLOAT     | The minimum latitude of the bounding box surrounding the QPV geographical shape.                           |
| qpv_max_latitude    | FLOAT     | The maximum latitude of the bounding box surrounding the QPV geographical shape.                           |
