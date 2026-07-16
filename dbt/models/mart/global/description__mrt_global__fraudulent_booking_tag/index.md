# Fraudulent booking tag Model

The `mrt_global__fraudulent_booking_tag` lists all bookings that have been tagged as fraudulent by the Fraud team.

**Business Rules**

- The column fraudulent_booking_tag_author_id corresponds to a user_id with the admin role in the back office.

## Table description

| name                                 | data_type | description                                                                                                                              |
| ------------------------------------ | --------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| fraudulent_booking_tag_id            |           | Unique identifier of a fraudulent booking tag.                                                                                           |
| fraudulent_booking_tag_creation_date |           | Date when the fraudulent booking tag was created.                                                                                        |
| fraudulent_booking_tag_author_id     |           | Unique identifier of the fraudulent booking tag author. This identifier corresponds to a user_id with the admin role in the back office. |
| booking_id                           |           | Unique identifier for a booking.                                                                                                         |
