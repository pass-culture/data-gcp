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
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'traffic_campaign'
        ) as traffic_campaign,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'traffic_medium'
        ) as traffic_medium,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'traffic_source'
        ) as traffic_source,
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
    ANY_VALUE(traffic_campaign) AS traffic_campaign,
    ANY_VALUE(traffic_medium) AS traffic_medium,
    ANY_VALUE(traffic_source) AS traffic_source,
    TIMESTAMP_SECONDS(CAST(MAX(event_timestamp) / 1000000 as INT64)) AS last_event_timestamp,
    COUNTIF(event_name = "ConsultOffer") AS nb_consult_offer,
    COUNTIF(event_name = "BookingConfirmation") AS nb_booking_confirmation,
    COUNTIF(event_name = "HasAddedOfferToFavorites") AS nb_add_to_favorites,
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