# Table: Booking

This model exports booking data for ministry use. It contains information about beneficiary bookings including:

- Booking details (ID, creation date, quantity, amount)
- Booking status and cancellation information
- Related entities (user, deposit, stock, offer, venue, offerer)
- Usage information (rank, used date)

## Table description

| name                        | data_type | description                                                                                                     |
| --------------------------- | --------- | --------------------------------------------------------------------------------------------------------------- |
| booking_id                  | STRING    | Unique identifier for a booking.                                                                                |
| booking_creation_date       |           | Date when the booking was created.                                                                              |
| booking_quantity            | INT64     | Quantity of offer booked. Can be 1, or 2 if booking was made using the duo option.                              |
| booking_amount              |           | Total amount for the booking for one quantity.                                                                  |
| booking_status              | STRING    | Current status of the booking.                                                                                  |
| booking_cancellation_date   |           | Date when the booking was cancelled.                                                                            |
| booking_cancellation_reason |           | Reason for booking cancellation.                                                                                |
| user_id                     | STRING    | Unique identifier for a user.                                                                                   |
| deposit_id                  | STRING    | Unique identifier for the deposit.                                                                              |
| booking_intermediary_amount | NUMERIC   | The amount of the booking multiplied by the booking quantity. This field is used when we calculate the revenue. |
| booking_rank                |           | Rank of the booking in the user's booking history.                                                              |
| booking_used_date           |           | Date when the booking was used.                                                                                 |
| stock_id                    | STRING    | Unique identifier for the stock.                                                                                |
| offer_id                    | STRING    | Unique identifier for the offer.                                                                                |
| venue_id                    | STRING    | Unique identifier for the venue.                                                                                |
| offerer_id                  | STRING    | Unique identifier of the offerer.                                                                               |
