# Table: User Address

This model exports beneficiary user address data for ministry use. It contains information about declarative user locations including: The address is the one declared by the beneficiary in their profile, and can be updated by the beneficiary.

- Geographic information (latitude, longitude, postal code)
- Address details (raw address, geocode type)
- Administrative information (academy name, department code, region name)
- QPV (Quartier Prioritaire de la Ville) information (code and name)
- IRIS internal ID (unique identifier of the IRIS) that can be used to join with the IRIS data provided.

## Table description

| name                      | data_type | description                                                                                                                                                                                                              |
| ------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| user_id                   |           | Unique identifier for a user.                                                                                                                                                                                            |
| user_address_geocode_type |           | Geocode precision type (street, municipality, etc) of the user's registered address.                                                                                                                                     |
| user_address_raw          |           | Raw address of the user's registered address.                                                                                                                                                                            |
| user_address_latitude     |           | Latitude of the user's registered address.                                                                                                                                                                               |
| user_address_longitude    |           | Longitude of the user's registered address.                                                                                                                                                                              |
| user_academy_name         |           | Academy name associated with the user's registered address.                                                                                                                                                              |
| user_department_code      |           | Department code associated with the user's registered address.                                                                                                                                                           |
| user_region_name          |           | Region name of the user's registered address.                                                                                                                                                                            |
| user_qpv_code             |           | The official code for the priority urban district (quartier prioritaire de la politique de la ville, QPV).                                                                                                               |
| user_qpv_name             |           | The name of the priority urban district (QPV).                                                                                                                                                                           |
| user_postal_code          |           | Postal code of the user's registered address.                                                                                                                                                                            |
| user_iris_internal_id     |           | Internal IRIS identifier associated with the user's registered address. IRIS (Ilots Regroupés pour l'Information Statistique) are small, standardized geographic units used for detailed statistical analysis in France. |
