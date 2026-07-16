# Table: User Beneficiary Information History

The `mrt_global__user_beneficiary_information_history` table tracks historical changes to user beneficiary information over time.

This table captures when users modify their profile information (activity status, address, city, postal code), allowing analysis of user behavior patterns while protecting privacy by excluding PII fields.

## Key Features

- Tracks activity changes (e.g., student to unemployed)
- Includes modification flags for each field type
- Maintains geographical aggregations (EPCI, IRIS, QPV) without exposing precise addresses
- Includes density and region information for demographic analysis
- Tracks user age at the time of information creation
- Supports temporal analysis with creation_timestamp and info_history_rank

## Privacy

This table excludes user_address, user_city, user_postal_code, and coordinates to protect PII.

## Table description

| name                             | data_type | description                                                                                                                                                                                                              |
| -------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| user_id                          | STRING    | Unique identifier for a user.                                                                                                                                                                                            |
| user_information_rank            | INT64     | Sequential rank of information changes for each user (0 = first record, 1 = second, etc.).                                                                                                                               |
| user_information_action_type     | STRING    | Type of action that triggered the user information record. Possible values: INFO_MODIFIED (user modified their profile), PROFILE_COMPLETION (initial profile completion during registration).                            |
| user_information_created_at      | TIMESTAMP | Timestamp when the user information was modified (partitioning key with daily granularity)                                                                                                                               |
| user_activity                    | STRING    | User's registered activity (student, apprentice, unemployed etc). Registered at first grant deposit and updated when the user applies for its GRANT_18.                                                                  |
| user_city                        | STRING    | City associated with the user's registered address.                                                                                                                                                                      |
| user_postal_code                 | STRING    | Postal code of the user's registered address.                                                                                                                                                                            |
| user_previous_activity           | STRING    | User's activity status before the current change.                                                                                                                                                                        |
| user_previous_city               | STRING    | User's city before the current change.                                                                                                                                                                                   |
| user_previous_postal_code        | STRING    | User's postal code before the current change.                                                                                                                                                                            |
| user_age_at_information_creation | INT64     | User's age in years at the time the information record was created.                                                                                                                                                      |
| user_has_confirmed_information   | BOOL      | Boolean flag indicating if the user confirmed their existing information (all fields remained the same compared to previous record).                                                                                     |
| user_has_modified_information    | BOOL      | Boolean flag indicating if the user modified any of their information (at least one field changed compared to previous record).                                                                                          |
| user_has_modified_activity       | BOOL      | Boolean flag indicating if user_activity changed compared to previous record.                                                                                                                                            |
| user_has_modified_address        | BOOL      | Boolean flag indicating if user address changed compared to previous record (normalized comparison).                                                                                                                     |
| user_has_modified_city           | BOOL      | Boolean flag indicating if user city changed compared to previous record.                                                                                                                                                |
| user_has_modified_postal_code    | BOOL      | Boolean flag indicating if user postal code changed compared to previous record.                                                                                                                                         |
| user_epci_code                   | STRING    | Code of the EPCI (Etablissement Public de Cooperation Intercommunale) where the user is located.                                                                                                                         |
| user_iris_internal_id            | STRING    | Internal IRIS identifier associated with the user's registered address. IRIS (Ilots Regroupés pour l'Information Statistique) are small, standardized geographic units used for detailed statistical analysis in France. |
| user_region_name                 | STRING    | Region name of the user's registered address.                                                                                                                                                                            |
| user_department_name             | STRING    | Department name associated with the user's registered address.                                                                                                                                                           |
| user_density_label               | STRING    | String column.Density label (urban, rural) of the user's registered address.                                                                                                                                             |
| user_density_macro_level         | STRING    | Macro-level urban density classification (Urban vs Rural). Aggregation of density_label into broader categories.                                                                                                         |
| user_qpv_code                    | STRING    | Code of the QPV (Quartier Prioritaire de la Ville) if the user is located in a priority neighborhood.                                                                                                                    |
| user_qpv_name                    | STRING    | Name of the QPV (Quartier Prioritaire de la Ville) if the user is located in a priority neighborhood.                                                                                                                    |
