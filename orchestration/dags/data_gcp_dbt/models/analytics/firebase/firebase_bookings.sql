{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'booking_date', 'data_type': 'date'},
    )
}}

SELECT 
    user_id
    , user_pseudo_id
    , session_id
    , unique_session_id
    , offer_id
    , booking_id
    , event_date as booking_date
    , event_timestamp as booking_timestamp
    , user_location_type
FROM {{ ref('int_firebase__native_event') }}
WHERE event_name = "BookingConfirmation"
{% if is_incremental() %}
-- recalculate latest day's data + previous
AND date(event_date) BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 3 DAY) and DATE('{{ ds() }}')
{% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY booking_id ORDER BY event_timestamp ) = 1
