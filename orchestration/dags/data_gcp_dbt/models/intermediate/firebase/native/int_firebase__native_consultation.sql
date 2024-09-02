{{
    config(
        **custom_incremental_config(
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'consultation_date', 'data_type': 'date'}
    )
) }}

SELECT DISTINCT
    CONCAT(user_id, "-",DATE_FORMAT(event_timestamp, '%Y%m%d%H%i%s'), "-", offer_id) AS consultation_id,
    user_id,
    event_date AS consultation_date,
    event_timestamp AS consultation_timestamp,
    offer_id,
    origin,
    module_id,
    unique_session_id,
    event_name,
    venue_id,
    traffic_medium,
    traffic_campaign,
FROM {{ ref('int_firebase__native_event') }}
WHERE event_name = 'ConsultOffer'
    AND user_id IS NOT NULL
    AND offer_id IS NOT NULL
    {% if is_incremental() %}
    AND date(event_date) >= date_sub('{{ ds() }}', INTERVAL 3 day)
    {% endif %}
