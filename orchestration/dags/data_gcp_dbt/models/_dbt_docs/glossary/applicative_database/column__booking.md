{% docs column__booking_id %} Unique identifier for a booking. {% enddocs %}
{% docs column__booking_creation_date %} Date on which the user booked the offer. {% enddocs %}
{% docs column__booking_created_at %} Date and time the user made the booking. {% enddocs %}
{% docs column__booking_quantity %} Number of items booked for the offer. Values can be 1, or 2 if booking was made using the duo option (only for the events). {% enddocs %}
{% docs column__booking_amount %} Price for a single item (has to be multiply by the quantity to have the total price paid by the user). {% enddocs %}
{% docs column__booking_status %} Current status of the booking. Possible values for the field are: CANCELLED: The booking has been cancelled. / CONFIRMED: The booking is active and not cancelled, but has not yet been used. / USED: The user has either collected the physical good or attended the reserved event. The booking has been validated but not yet reimbursed to the cultural partner. / REIMBURSED: The booking has been reimbursed to the cultural partner according to the applicable reimbursement rules. {% enddocs %}
{% docs column__booking_is_cancelled %} Boolean. Indicates if the booking has been cancelled (status = CANCELLED). {% enddocs %}
{% docs column__booking_is_used %} Boolean. Indicates if the booking has been used (status = USED or REIMBURSED). {% enddocs %}
{% docs column__booking_reimbursed %} Boolean. Indicates if the booking has been reimbursed to the cultural partner (status = REIMBURSED). {% enddocs %}
{% docs column__booking_cancellation_date %} Date on which the booking has been cancelled. {% enddocs %}
{% docs column__booking_cancellation_reason %} Reason why the booking was cancelled. Main values are: BENEFICIARY (the booking was cancelled by the user) / EXPIRED (for digital offers only) / OFFERER (the booking was cancelled by the cultural partner - ex : the event was cancelled) / OFFERER_CLOSED (the cultural partner no longer exists) / BACKOFFICE (the booking was cancelled by the pass Culture team - ex : in case of fraud). {% enddocs %}
{% docs column__booking_intermediary_amount %} Total price paid by the user (booking_quantity * booking_amount). {% enddocs %}
{% docs column__booking_rank %} Rank of the booking in the user's booking history (ex : when the user makes his first booking, booking_rank = 1). {% enddocs %}
{% docs column__booking_used_date %} Date when the booking was used (physical good has been collected or event has occured). {% enddocs %}
{% docs column__booking_used_recredit_type %} The type of recredit associated with the booking, linking a booking to a specific user cohort (cf 'deposit' table). {% enddocs %}
{% docs column__user_booking_rank %} Rank of the user booking. {% enddocs %}
{% docs column__same_category_booking_rank %} Rank of the user booking within the same offer_category. {% enddocs %}
