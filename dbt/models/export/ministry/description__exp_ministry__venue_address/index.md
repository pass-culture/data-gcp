# Table: Venue Address

This model exports venue address data for ministry use. It contains information about administrative venue locations. Currently this concept should be preferred to the new address concept that is still under development.

- Basic address information (street, city, postal code)
- Geographic coordinates (latitude, longitude)
- Administrative information (department code, region name)
- QPV (Quartier Prioritaire de la Ville) information (code and name)
- IRIS internal ID (unique identifier of the IRIS) that can be used to join with the IRIS data provided.

## Table description

| name                   | data_type | description                                                                                                |
| ---------------------- | --------- | ---------------------------------------------------------------------------------------------------------- |
| venue_id               |           | Unique identifier for the venue.                                                                           |
| venue_street           |           | Street address of the venue.                                                                               |
| venue_latitude         |           | Latitude coordinate of the venue.                                                                          |
| venue_longitude        |           | Longitude coordinate of the venue.                                                                         |
| venue_department_code  |           | Department code of the venue.                                                                              |
| venue_postal_code      |           | Postal code of the venue.                                                                                  |
| venue_city             |           | City where the venue is located.                                                                           |
| venue_region_name      |           | Region name where the venue is located.                                                                    |
| qpv_code               |           | The official code for the priority urban district (quartier prioritaire de la politique de la ville, QPV). |
| qpv_name               |           | The name of the priority urban district (QPV).                                                             |
| venue_iris_internal_id |           | Internal IRIS identifier for the venue.                                                                    |
