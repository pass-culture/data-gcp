The `mrt_pro__visit` table captures detailed visit data from professional applications. It includes information about user sessions, interactions, and management activities. This table is essential for analyzing user engagement, session behavior, and the effectiveness of management actions within the app.

## Table description

| name                    | data_type | description                                                                                          |
| ----------------------- | --------- | ---------------------------------------------------------------------------------------------------- |
| user_id                 |           | Unique identifier for a user.                                                                        |
| user_pseudo_id          |           | A pseudo identifier for the user, used for tracking purposes.                                        |
| session_id              |           | Deprecated: use unique_session_id. The session identifier during which the event was recorded.       |
| unique_session_id       |           | A unique identifier for the session, ensuring no duplicates.                                         |
| session_number          |           | The number of the session during which the visit occurred, indicating the sequence of user sessions. |
| first_event_date        |           | The date of the first event in the session, used for partitioning the data.                          |
| first_event_timestamp   |           | The exact timestamp of the first event in the session.                                               |
| last_event_timestamp    |           | The exact timestamp of the last event in the session.                                                |
| visit_duration_seconds  |           | The duration of the visit in seconds, calculated from the first to the last event.                   |
| total_offerers          |           | The total number of distinct offerers interacted with during the visit.                              |
| total_partners          |           | The total number of distinct partners interacted with during the visit.                              |
| total_managed_offers    |           | Indicates whether any offers were managed during the visit.                                          |
| total_managed_tickets   |           | Indicates whether any tickets were managed during the visit.                                         |
| total_managed_bookings  |           | Indicates whether any bookings were managed during the visit.                                        |
| total_managed_finance   |           | Indicates whether any financial activities were managed during the visit.                            |
| total_managed_venues    |           | Indicates whether any venues were managed during the visit.                                          |
| total_managed_profiles  |           | Indicates whether any profiles were managed during the visit.                                        |
| total_stat_page_views   |           | The total number of statistic page views during the visit.                                           |
| total_consulted_help    |           | Indicates whether any help resources were consulted during the visit.                                |
| total_booking_downloads |           | Indicates whether any bookings were downloaded during the visit.                                     |
| total_consulted_offers  |           | Indicates whether any offer pages (on "Espace partenaire") were consulted during the visit.          |
