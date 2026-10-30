**booking_id**: Unique identifier for a booking.

**booking_creation_date**: Date when the booking was created.

**booking_created_at**: Date and time when the booking was created, in Paris local time, stored as DATETIME without time zone (not TIMESTAMP): compare it with a DATETIME or DATE.

**booking_quantity**: Quantity of offer booked. Can be 1, or 2 if booking was made using the duo option. Summing it counts seats, not bookings.

**booking_amount**: Unit price of the booked offer: the amount for one quantity. Multiply by the booking quantity for the booking total.

**booking_status**: Current status of the booking. For individual bookings, stored values are 'CONFIRMED' (booked, not yet used), 'USED' (used), 'REIMBURSED' (used and reimbursed to the offerer) and 'CANCELLED'.

**reimbursement_booking_status**: Current status of the booking (the collective booking status for collective rows). The table only keeps bookings with an invoiced pricing, so it does not cover every status of the booking lifecycle.

**booking_is_cancelled**: Boolean. TRUE when the booking is cancelled, i.e. booking_status = 'CANCELLED'.

**booking_is_used**: Boolean. TRUE when the booking has been used, whether or not it has been reimbursed since.

**booking_reimbursed**: Boolean. Indicates if the booking amount was reimbursed or not.

**booking_cancellation_date**: Date when the booking was cancelled.

**booking_cancellation_reason**: Reason for booking cancellation.

**booking_intermediary_amount**: The amount of the booking multiplied by the booking quantity. This field is used when we calculate the revenue.

**booking_rank**: Rank of the booking in the user's booking history.

**booking_used_date**: Date when the booking was used.

**booking_used_recredit_type**: The type of recredit associated with the booking, linking a booking to a specific user cohort.

**user_booking_rank**: Rank of the user booking.

**same_category_booking_rank**: Rank of the user booking within the same offer_category.

**first_individual_booking_date**: Date of the first individual booking.

**first_booking_date**: Date of the first booking.

**last_booking_date**: Date of the last booking.

**last_individual_booking_date**: Date of the last individual booking.
