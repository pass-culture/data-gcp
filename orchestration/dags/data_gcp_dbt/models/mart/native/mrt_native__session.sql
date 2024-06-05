{{
  config(
    materialized = "incremental",
    partition_by={
      "field": "first_event_date",
      "data_type": "date",
      "granularity": "day",
      "time_ingestion_partitioning": false
    },
    incremental_strategy = 'insert_overwrite',
    on_schema_change = "sync_all_columns"
  )
}}

WITH sessions AS (
    SELECT
        session_id,
        user_pseudo_id,
        unique_session_id,
        MAX(platform) AS platform,
        MAX(app_version) AS app_version,
        MAX(session_number) AS session_number,
        MAX(user_id) AS user_id,
        MIN(event_timestamp) AS first_event_timestamp,
        DATE(MIN(event_timestamp)) AS first_event_date,
        MAX(traffic_campaign) AS traffic_campaign,
        MAX(traffic_medium) AS traffic_medium,
        MAX(traffic_source) AS traffic_source,
        MAX(traffic_gen) AS traffic_gen,
        MAX(traffic_content) AS traffic_content,
        MAX(event_timestamp) AS last_event_timestamp,
        SUM(is_consult_offer) AS total_consulted_offers,
        SUM(is_booking_confirmation) AS total_booking_confirmations,
        SUM(is_add_to_favorites) AS total_add_to_favorites,
        SUM(is_share) AS total_shares,
        SUM(is_screenshot) AS total_screenshots,
        SUM(is_set_location) AS nb_set_location,
        SUM(is_consult_video) AS total_consulted_videos,
        SUM(is_consult_venue) AS total_consulted_venues,
        SUM(is_screen_view) AS total_screen_views,
        SUM(is_screen_view_home) AS total_home_screen_views,
        SUM(is_screen_view_search) AS total_search_screen_views,
        SUM(is_screen_view_offer) AS total_offer_screen_views,
        SUM(is_screen_view_profile) AS total_profile_screen_views,
        SUM(is_screen_view_favorites) AS total_favorite_screen_views,
        SUM(is_screen_view_bookings) AS total_booking_screen_views,
        SUM(is_signup_completed) AS total_completed_signup,
        SUM(is_benef_request_sent) AS total_sent_requests,
        COUNTIF(event_name = "login") AS total_login,
        DATE_DIFF(MAX(event_timestamp),MIN(event_timestamp),SECOND) AS visit_duration_seconds,
    FROM {{ ref('int_firebase__native_event') }}
    WHERE TRUE
        {% if is_incremental() %}
            -- lag in case session is between two days.
            AND event_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 3+1 DAY) and DATE('{{ ds() }}') AND
        {% endif %}
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
        user_pseudo_id,
        unique_session_id

)

SELECT * FROM sessions
-- incremental run only update partition of run day
{% if is_incremental() %}
    WHERE first_event_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 3 DAY) and DATE('{{ ds() }}')
{% endif %}
