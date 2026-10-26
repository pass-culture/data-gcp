{% docs column__booking_id %} Unique identifier for a booking. {% enddocs %}
{% docs column__booking_creation_date %} Date when the booking was created. {% enddocs %}
{% docs column__booking_created_at %} Date and time when the booking was created, in Paris local time, stored as DATETIME without time zone (not TIMESTAMP): compare it with a DATETIME or DATE. {% enddocs %}
{% docs column__booking_quantity %} Quantity of offer booked. Can be 1, or 2 if booking was made using the duo option. {% enddocs %}
{% docs column__booking_amount %} Total amount for the booking for one quantity. {% enddocs %}
{% docs column__booking_status %} Current status of the booking. For individual bookings, stored values are 'CONFIRMED' (booked, not yet used), 'USED' (used), 'REIMBURSED' (used and reimbursed to the offerer) and 'CANCELLED'. {% enddocs %}
{% docs column__booking_is_cancelled %} Boolean. TRUE when the booking is cancelled, i.e. booking_status = 'CANCELLED'. {% enddocs %}
{% docs column__booking_is_used %} Boolean. TRUE when the booking has been used: booking_status is 'USED', 'PENDING_REIMBURSEMENT' or 'REIMBURSED'. {% enddocs %}
{% docs column__booking_reimbursed %} Boolean. Indicates if the booking amount was reimbursed or not. {% enddocs %}
{% docs column__booking_cancellation_date %} Date when the booking was cancelled. {% enddocs %}
{% docs column__booking_cancellation_reason %} Reason for booking cancellation. {% enddocs %}
{% docs column__booking_intermediary_amount %} The amount of the booking multiplied by the booking quantity. This field is used when we calculate the revenue. {% enddocs %}
{% docs column__booking_rank %} Rank of the booking in the user's booking history. {% enddocs %}
{% docs column__booking_used_date %} Date when the booking was used. {% enddocs %}
{% docs column__booking_used_recredit_type %} The type of recredit associated with the booking, linking a booking to a specific user cohort. {% enddocs %}
{% docs column__user_booking_rank %} Rank of the user booking. {% enddocs %}
{% docs column__same_category_booking_rank %} Rank of the user booking within the same offer_category. {% enddocs %}
{% docs column__first_individual_booking_date %} Date of the first individual booking. {% enddocs %}
{% docs column__first_booking_date %} Date of the first booking. {% enddocs %}
{% docs column__last_booking_date %} Date of the last booking. {% enddocs %}
{% docs column__last_individual_booking_date %} Date of the last individual booking. {% enddocs %}
