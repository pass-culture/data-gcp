{% snapshot snapshot__applicative_collective_booking %}

{{
    config(
      strategy='check',
      unique_key='collective_booking_id',
      check_cols=['collective_booking_creation_date', 'collective_booking_used_date', 'collective_stock_id', 'venue_id', 'offerer_id', 'collective_booking_cancellation_date', 'collective_booking_cancellation_limit_date', 'collective_booking_cancellation_reason', 'collective_booking_status', 'collective_booking_reimbursement_date', 'educational_institution_id', 'educational_year_id', 'collective_booking_confirmation_date', 'collective_booking_confirmation_limit_date', 'educational_redactor_id']
    )
}}

    select
        collective_booking_id,
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
    from {{ source('raw', 'applicative_database_collective_booking') }}

{% endsnapshot %}
