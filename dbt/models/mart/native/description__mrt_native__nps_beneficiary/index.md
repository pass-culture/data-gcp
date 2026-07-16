The `mrt_native__nps_beneficiary` table captures Net Promoter Score (NPS) data from beneficiaries in the native application. It includes information about user responses, ratings, demographics, and engagement metrics like deposit type and booking history. This table is essential for analyzing user satisfaction, regional trends, and the relationship between user activity and NPS ratings.

## Table description

| name             | data_type | description                                                                                                                                             |
| ---------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| response_date    |           | The date of the response from the survey.                                                                                                               |
| user_id          |           | Unique identifier for a user.                                                                                                                           |
| response_id      |           | The identifier for the response from the survey.                                                                                                        |
| deposit_type     |           | Type of the deposit, can be GRANT_18, GRANT_15_17, GRANT_17_18, GRANT_FREE.                                                                             |
| user_civility    |           | Registered civility of the user (male, female). Information collected during registration.                                                              |
| user_region_name |           | Region name of the user's registered address.                                                                                                           |
| user_activity    |           | User's registered activity (student, apprentice, unemployed etc). Registered at first grant deposit and updated when the user applies for its GRANT_18. |
| user_is_in_qpv   |           | Boolean. Indicates if the user's registered address is in a priority neighborhood (QPV).                                                                |
| total_bookings   |           | Total number of bookings, both individual and collective.                                                                                               |
| response_rating  |           | The rating from the response from the survey.                                                                                                           |
| user_seniority   |           | Days between user account creation date and current date.                                                                                               |
