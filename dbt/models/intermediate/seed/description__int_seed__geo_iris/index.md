# Table: Geographic IRIS

This model processes and enriches geographic IRIS (Îlots Regroupés pour l'Information Statistique) data. It contains comprehensive geographic and administrative information including:

- IRIS identifiers and labels
- Administrative hierarchy (city, district, sub-district, EPCI)
- Geographic boundaries (shape, centroid, bounding box coordinates)
- Administrative information (department, region, academy)
- Territorial characteristics (density levels, rural city type)
- Timezone information

## Table description

| name                        | data_type | description                                                                                                  |
| --------------------------- | --------- | ------------------------------------------------------------------------------------------------------------ |
| iris_code                   |           | The official INSEE code identifying the IRIS (Îlots Regroupés pour l'Information Statistique) area.          |
| iris_label                  |           | The descriptive name or label of the IRIS area.                                                              |
| city_code                   |           | The official INSEE code of the municipality (commune) containing the IRIS area.                              |
| city_label                  |           | The name of the municipality (commune) containing the IRIS area.                                             |
| territorial_authority_code  |           | The official code of the territorial authority (collectivité territoriale) containing the IRIS area.         |
| district_code               |           | The official code of the district (arrondissement) containing the IRIS area.                                 |
| sub_district_code           |           | The official code of the sub-district (canton) containing the IRIS area.                                     |
| epci_code                   |           | The official code of the EPCI (Établissement Public de Coopération Intercommunale) containing the IRIS area. |
| epci_label                  |           | The name of the EPCI containing the IRIS area.                                                               |
| sub_district_label          |           | The name of the sub-district (canton) containing the IRIS area.                                              |
| district_label              |           | The name of the district (arrondissement) containing the IRIS area.                                          |
| department_code             |           | The official code of the department (département) containing the IRIS area.                                  |
| department_name             |           | The official name of the department containing the IRIS area.                                                |
| region_code                 |           | The official code of the region (région) containing the IRIS area.                                           |
| region_name                 |           | The official name of the region containing the IRIS area.                                                    |
| timezone                    |           | The timezone of the IRIS area (e.g., 'Europe/Paris').                                                        |
| academy_name                |           | The name of the educational academy (académie) associated with the IRIS area.                                |
| territorial_authority_label |           | The name of the territorial authority (collectivité territoriale) containing the IRIS area.                  |
| density_level               |           | The numerical level indicating the population density of the IRIS area.                                      |
| density_label               |           | The descriptive label for the population density level of the IRIS area.                                     |
| density_macro_level         |           | The broader categorization of population density for the IRIS area.                                          |
| geo_code                    |           | The unique geographic code identifying the IRIS area.                                                        |
| rural_city_type             |           | The classification of the municipality type (rural, urban, etc.) containing the IRIS area.                   |
| iris_internal_id            |           | The internal identifier for the IRIS area within the system.                                                 |
| iris_centroid               |           | The geographic point representing the center of the IRIS area.                                               |
| iris_shape                  |           | The geographic shape data representing the boundaries of the IRIS area.                                      |
| min_longitude               |           | The minimum longitude of the bounding box surrounding the IRIS area.                                         |
| max_longitude               |           | The maximum longitude of the bounding box surrounding the IRIS area.                                         |
| min_latitude                |           | The minimum latitude of the bounding box surrounding the IRIS area.                                          |
| max_latitude                |           | The maximum latitude of the bounding box surrounding the IRIS area.                                          |
