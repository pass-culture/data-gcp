# Table: Daily Venue Map Conversion

The `mrt_native__daily_venue_map_conversion` table provides detailed insights into user interactions with the venue map on a daily basis. It captures various user activities related to the venue map, including the consultations, bookings and diversity increments.

## Table description

| name                                          | data_type | description                                                                                  |
| --------------------------------------------- | --------- | -------------------------------------------------------------------------------------------- |
| user_id                                       |           | Unique identifier for a user.                                                                |
| unique_session_id                             |           | A unique identifier for the session, ensuring no duplicates.                                 |
| app_version                                   |           | The version of the application where the event was recorded.                                 |
| event_date                                    |           | The date when the event occurred, used for partitioning the data.                            |
| total_venue_map_consult                       |           | The total number of consultations from the venue map.                                        |
| total_venue_map_preview                       |           | The total number of previews of the venue map.                                               |
| total_consult_venue                           |           | The total number of venues consulted from the venue map.                                     |
| total_distinct_venue_consult_offer            |           | The total distinct number of venues which led to an offer consultation.                      |
| total_consult_offer                           |           | The total number of offer consultations.                                                     |
| total_bookings                                |           | Total number of bookings, both individual and collective.                                    |
| total_non_cancelled_bookings                  |           | Total number of bookings that were not cancelled, both individual and collective.            |
| total_diversification                         |           | Total du score de diversité incrémenté par les réservations associées.                       |
| total_session_venue_map_seen_duration_seconds |           | Durée totale en seconde pendant laquelle la venue map a été affichée au cours d’une session. |
