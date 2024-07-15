{% snapshot booking_history_light_hash %}

{{
    config(
      strategy='check',
      unique_key='booking_id',
      check_cols=['booking_creation_date', 'stock_id', 'booking_quantity', 'user_id', 'booking_amount', 'booking_status', 'booking_is_cancelled', 'booking_is_used', 'booking_used_date', 'booking_cancellation_date', 'booking_cancellation_reason', 'booking_reimbursement_date']
    )
}}

with bookings as (
  select
    booking_id,
    DATE(booking_creation_date) AS booking_creation_date,
    stock_id,
    booking_quantity,
    user_id,
    booking_amount,
    booking_status,
    booking_is_cancelled,
    booking_is_used,
    booking_used_date,
    booking_cancellation_date,
    booking_cancellation_reason,
    booking_reimbursement_date
  from {{ source('raw', 'applicative_database_booking') }}
)

select * from bookings

{% endsnapshot %}