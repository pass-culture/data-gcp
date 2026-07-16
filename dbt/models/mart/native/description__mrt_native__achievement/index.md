The `mrt_native__achievement`table lists all achievements earned by users following their bookings. The achievements feature helps users try different types of cultural activities. By collecting achievements across various categories, users are motivated to explore new cultural experiences they might not have tried otherwise.

## Table description

| name                      | data_type | description                                                                     |
| ------------------------- | --------- | ------------------------------------------------------------------------------- |
| achievement_id            | STRING    | The unique identifier for the achievement.                                      |
| user_id                   | STRING    | Unique identifier for a user.                                                   |
| booking_id                | STRING    | Unique identifier for a booking.                                                |
| achievement_name          | STRING    | The name of the achievement.                                                    |
| achievement_unlocked_date | DATE      | The date when the achievement was unlocked.                                     |
| achievement_seen_date     | DATE      | The date when the achievement was seen by the user, after it has been unlocked. |
