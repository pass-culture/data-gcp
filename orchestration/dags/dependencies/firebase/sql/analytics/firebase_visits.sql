WITH base AS (
    SELECT
        event_name,
        event_timestamp,
        user_id,
        user_pseudo_id,
        user_first_touch_timestamp,
        device.category,
        device.mobile_brand_name,
        device.operating_system,
        device.operating_system_version,
        traffic_source.name,
        traffic_source.medium,
        traffic_source.source,
        (
            select
                event_params.value.int_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'ga_session_id'
        ) as session_id,
        (
            select
                event_params.value.int_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'ga_session_number'
        ) as session_number,
    FROM
        `{{ bigquery_clean_dataset }}.firebase_events_*`
    WHERE
        event_name NOT IN (
            'app_remove',
            'os_update',
            'batch_notification_open',
            'batch_notification_display',
            'batch_notification_dismiss',
            'app_update'
        )
)
SELECT
    session_id,
    user_pseudo_id,
    ANY_VALUE(session_number) AS session_number,
    ANY_VALUE(category) AS category,
    ANY_VALUE(mobile_brand_name) AS mobile_brand_name,
    ANY_VALUE(operating_system) AS operating_system,
    ANY_VALUE(operating_system_version) AS operating_system_version,
    ANY_VALUE(user_id) AS user_id,
    TIMESTAMP_SECONDS(CAST(MIN(event_timestamp) / 1000000 as INT64)) AS first_event_timestamp,
    ANY_VALUE(name) AS name,
    ANY_VALUE(medium) AS medium,
    ANY_VALUE(source) AS source,
    TIMESTAMP_SECONDS(CAST(MAX(event_timestamp) / 1000000 as INT64)) AS last_event_timestamp,
    COUNTIF(event_name = "ConsultOffer") AS nb_consult_offer,
    COUNTIF(event_name = "BookingConfirmation") AS nb_booking_confirmation,
    DATE_DIFF(
        TIMESTAMP_SECONDS(CAST(MAX(event_timestamp) / 1000000 as INT64)),
        TIMESTAMP_SECONDS(CAST(MIN(event_timestamp) / 1000000 as INT64)),
        SECOND
    ) AS visit_duration_seconds,
FROM
    base
GROUP BY
    session_id,
    user_pseudo_id;