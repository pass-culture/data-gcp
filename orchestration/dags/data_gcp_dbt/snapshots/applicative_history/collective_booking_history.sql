{% snapshot collective_booking_history %}
    
{{
    config(
      strategy='check',
      unique_key='collective_booking_id',
      check_cols=['booking_id', 'collective_booking_creation_date', 'collective_booking_used_date', 'collective_stock_id', 'venue_id', 'offerer_id', 'collective_booking_cancellation_date', 'collective_booking_cancellation_limit_date', 'collective_booking_cancellation_reason', 'collective_booking_status', 'collective_booking_reimbursement_date', 'educational_institution_id', 'educational_year_id', 'collective_booking_confirmation_date', 'collective_booking_confirmation_limit_date', 'educational_redactor_id']
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
{{ source('raw', 'applicative_database_collective_booking') }}

{% endsnapshot %}