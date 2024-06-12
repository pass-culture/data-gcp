{% snapshot collective_booking_history %}
    
{{
    config(
      strategy='check',
      unique_key='venue_id',
      check_cols=['collective_booking_id', 'booking_id', 'collective_booking_creation_date', 'collective_booking_used_date', 'collective_stock_id', 'venue_id', 'offerer_id', 'collective_booking_cancellation_date', 'collective_booking_cancellation_limit_date', 'collective_booking_cancellation_reason', 'collective_booking_status', 'collective_booking_reimbursement_date', 'educational_institution_id', 'educational_year_id', 'collective_booking_confirmation_date', 'collective_booking_confirmation_limit_date', 'educational_redactor_id']
    )
}}

SELECT
	collective_booking_id,
	booking_id,
	collective_booking_creation_date,
	collective_booking_used_date,
	collective_stock_id,
	venue_id,
	offerer_id,
	collective_booking_cancellation_date,
	collective_booking_cancellation_limit_date,
	collective_booking_cancellation_reason,
	collective_booking_status,
	collective_booking_reimbursement_date,
	educational_institution_id,
	educational_year_id,
	collective_booking_confirmation_date,
	collective_booking_confirmation_limit_date,
	educational_redactor_id
FROM {{ ref('collective_booking') }}

{% endsnapshot %}