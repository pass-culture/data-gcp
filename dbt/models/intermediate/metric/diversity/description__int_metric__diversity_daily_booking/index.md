The `int_metric__diversity_daily_booking` table captures the diversification of cultural practices within the pass Culture application. Diversification is defined by booking an offer with different characteristics from those previously booked, indicating a cultural discovery by the user.

For each reservation, the analyzed characteristics include:

- **Diversity in category**: from book to cinema, from live performance to music.
- **Diversity in subcategory**: from comic book to detective novel, from drama to comedy.
- **Diversity in genre**: from science fiction to fantasy, from thriller to romance.
- **Diversity in place**: from an independent bookstore to a large network, from a cinema to a performance hall.
- **Diversity in type of place**: from a museum to a library, from a theater to a concert hall.

## Table description

| name                          | data_type | description                                                                                                                                        |
| ----------------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| booking_id                    |           | Unique identifier for a booking.                                                                                                                   |
| booking_created_at            |           | Timestamp when the booking was created.                                                                                                            |
| booking_creation_date         |           | Date when the booking was created.                                                                                                                 |
| user_id                       |           | Unique identifier for a user.                                                                                                                      |
| booking_rank                  |           | Rank of the booking in the user's booking history.                                                                                                 |
| diversity_booking_entity_rank |           | The rank of a booking entity for a user, determined by the order of booking creation.                                                              |
| diversity_booked_entity_type  |           | The type of entity booked, which can be one of several categories such as OFFER_CATEGORY, VENUE_TYPE, OFFER_SUBCATEGORY, VENUE, or EXTRA_CATEGORY. |
| diversity_booked_entity       |           | The specific entity booked, which can be an offer category ID, venue type label, offer subcategory ID, venue ID, or extra category.                |
| diversity_score               |           | A score assigned to a booking based on its rank and entity type, with a multiplier applied                                                         |
