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
FROM {{ source('analytics', 'firebase_events') }}

{% if is_incremental() %}
-- recalculate latest day's data + previous
where date(event_date) >= date_sub(date(_dbt_max_partition), interval 1 day)
{% endif %}
