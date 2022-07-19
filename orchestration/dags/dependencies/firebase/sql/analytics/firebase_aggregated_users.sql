WITH events AS (
    SELECT
        user_id,
        event_name,
        device.mobile_model_name,
        (
            select
                event_params.value.int_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'ga_session_id'
        ) as session_id,
        user_pseudo_id,
        (
            select
                event_params.value.string_value
            from
                unnest(event_params) event_params
            where
                event_params.key = 'firebase_screen_class'
        ) as firebase_screen_class,
    FROM
        `{{ bigquery_clean_dataset }}.firebase_events_*` AS events
),
sessions AS (
    SELECT
        ROUND(
            (max(event_timestamp) - min(event_timestamp)) /(1000 * 1000),
            1
        ) AS total_time,
        event_params.value.int_value AS session_id,
        user_pseudo_id
    FROM
        `{{ bigquery_clean_dataset }}.firebase_events_*` AS events,
        events.event_params AS event_params
    WHERE
        event_params.key = 'ga_session_id'
        AND event_name NOT IN (
            'app_remove',
            'os_update',
            'batch_notification_open',
            'batch_notification_display',
            'batch_notification_dismiss',
            'app_update'
        )
    GROUP BY
        session_id,
        user_pseudo_id
),
first_and_last_co_date AS (
    SELECT
        user_id,
        MIN(event_timestamp) AS first_connexion_date,
        MAX(event_timestamp) AS last_connexion_date
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
    GROUP BY
        user_id
)
SELECT
    events.user_id,
    first_connexion_date,
    last_connexion_date,
    SUM(total_time) AS visit_total_time,
    AVG(total_time) AS visit_avg_time,
    COUNT(DISTINCT events.session_id) AS visit_count,
    COUNT(DISTINCT mobile_model_name) AS device_model_count,
    SUM(CAST(event_name = 'screen_view' AS INT64)) AS screen_view,
    SUM(
        CAST(
            event_name = 'screen_view'
            AND firebase_screen_class = 'Home' AS INT64
        )
    ) AS screen_view_home,
    SUM(
        CAST(
            event_name = 'screen_view'
            AND firebase_screen_class = 'Search' AS INT64
        )
    ) AS screen_view_search,
    SUM(
        CAST(
            event_name = 'screen_view'
            AND firebase_screen_class = 'Offer' AS INT64
        )
    ) AS screen_view_offer,
    SUM(
        CAST(
            event_name = 'screen_view'
            AND firebase_screen_class = 'Profile' AS INT64
        )
    ) AS screen_view_profile,
    SUM(
        CAST(
            event_name = 'screen_view'
            AND firebase_screen_class = 'Favorites' AS INT64
        )
    ) AS screen_view_favorites,
    SUM(CAST(event_name = 'user_engagement' AS INT64)) AS user_engagement,
    SUM(CAST(event_name = 'AllModulesSeen' AS INT64)) AS all_modules_seen,
    SUM(CAST(event_name = 'AllTilesSeen' AS INT64)) AS all_tiles_seen,
    SUM(CAST(event_name = 'Share' AS INT64)) AS share,
    SUM(
        CAST(event_name = 'HasAddedOfferToFavorites' AS INT64)
    ) AS has_added_offer_to_favorites,
    SUM(
        CAST(event_name = 'RecommendationModuleSeen' AS INT64)
    ) AS recommendation_module_seen,
    SUM(CAST(event_name = 'ConsultOffer' AS INT64)) AS consult_offer,
    SUM(CAST(event_name = 'ClickBookOffer' AS INT64)) AS click_book_offer,
FROM
    events
    LEFT JOIN sessions ON events.session_id = sessions.session_id
    AND sessions.user_pseudo_id = events.user_pseudo_id
    LEFT JOIN first_and_last_co_date ON first_and_last_co_date.user_id = events.user_id
WHERE
    events.user_id IS NOT NULL
GROUP BY
    events.user_id,
    first_connexion_date,
    last_connexion_date