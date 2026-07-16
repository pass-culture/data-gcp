# Table: Geographic IRIS

This model exports french geographic IRIS (Îlots Regroupés pour l'Information Statistique) data for ministry use. It contains information about:

- IRIS identifiers and labels
- Administrative hierarchy (city, district, sub-district)
- Department information (code and name)

## Table description

| name              | data_type | description                                                                     |
| ----------------- | --------- | ------------------------------------------------------------------------------- |
| iris_internal_id  |           | The internal identifier for the IRIS area within the system.                    |
| iris_label        |           | The descriptive name or label of the IRIS area.                                 |
| city_code         |           | The official INSEE code of the municipality (commune) containing the IRIS area. |
| city_label        |           | The name of the municipality (commune) containing the IRIS area.                |
| district_code     |           | The official code of the district (arrondissement) containing the IRIS area.    |
| sub_district_code |           | The official code of the sub-district (canton) containing the IRIS area.        |
| department_code   |           | The official code of the department (département) containing the IRIS area.     |
| department_name   |           | The official name of the department containing the IRIS area.                   |
| iris_centroid     |           | The geographic point representing the center of the IRIS area.                  |
| iris_shape        |           | The geographic shape data representing the boundaries of the IRIS area.         |
