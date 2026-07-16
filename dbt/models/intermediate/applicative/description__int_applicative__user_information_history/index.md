The `int_applicative__user_information_history` table event-level data is a subset of the `action_history` table focusing on user modified informations. As of Q3 2024, users can update personal informations (postal code of residency, current activity) directly through their Profile section in the app. This enables pass Culture team to provide accurate analysis with up to date user information.

## Table description

| name             | data_type | description                                                                                                                                             |
| ---------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| user_id          | STRING    | Unique identifier for a user.                                                                                                                           |
| event_date       | DATE      | The date when the event occurred, used for partitioning the data.                                                                                       |
| user_modified_at | DATETIME  | Timestamp at which user has updated its informations.                                                                                                   |
| user_postal_code | STRING    | Postal code of the user's registered address.                                                                                                           |
| user_activity    | STRING    | User's registered activity (student, apprentice, unemployed etc). Registered at first grant deposit and updated when the user applies for its GRANT_18. |
