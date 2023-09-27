SELECT
    session_id,
    user_pseudo_id,
    unique_session_id,
    ANY_VALUE(session_number) AS session_number,
    ANY_VALUE(user_id) AS user_id,
    MIN(event_timestamp) AS first_event_timestamp,
    ANY_VALUE(traffic_campaign) AS traffic_campaign,
    ANY_VALUE(traffic_medium) AS traffic_medium,
    ANY_VALUE(traffic_source) AS traffic_source,
    MAX(event_timestamp) AS last_event_timestamp,
    COUNTIF(event_name = "ConsultOffer") AS nb_consult_offer,
    COUNTIF(event_name = "BookingConfirmation") AS nb_booking_confirmation,
    COUNTIF(event_name = "HasAddedOfferToFavorites") AS nb_add_to_favorites,
    COUNTIF(event_name = "Share") AS nb_share,
    COUNTIF(event_name = "Screenshot") AS nb_screenshot,
    COUNTIF(event_name = "ConsultVideo") AS nb_consult_video,
    COUNTIF(event_name = 'screen_view') AS nb_screen_view,
    COUNTIF(event_name = 'screen_view' AND firebase_screen = 'Home') AS nb_screen_view_home,
    COUNTIF(event_name = 'screen_view' AND firebase_screen = 'Search') AS nb_screen_view_search,
    COUNTIF(event_name = 'screen_view' AND firebase_screen = 'Offer') AS nb_screen_view_offer,
    COUNTIF(event_name = 'screen_view' AND firebase_screen = 'Profile') AS nb_screen_view_profile,
    COUNTIF(event_name = 'screen_view' AND firebase_screen = 'Favorites') AS nb_screen_view_favorites,
    DATE_DIFF(MAX(event_timestamp),MIN(event_timestamp),SECOND) AS visit_duration_seconds,
FROM
        `{{ bigquery_analytics_dataset }}.firebase_events`
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
    session_id,
    user_pseudo_id,
    unique_session_id;
