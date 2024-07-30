{{
  config(
    **custom_incremental_config(
    partition_by={
      "field": "first_event_date",
      "data_type": "date",
      "granularity": "day",
      "time_ingestion_partitioning": false
    },
    incremental_strategy = 'insert_overwrite',
    on_schema_change = "sync_all_columns"
  )
) }}

with visits as (
    select
        session_id,
        user_pseudo_id,
        unique_session_id,
        platform,
        app_version,
        ANY_VALUE(session_number) as session_number,
        ANY_VALUE(user_id) as user_id,
        MIN(event_timestamp) as first_event_timestamp,
        DATE(MIN(event_timestamp)) as first_event_date,
        ANY_VALUE(traffic_campaign) as traffic_campaign,
        ANY_VALUE(traffic_medium) as traffic_medium,
        ANY_VALUE(traffic_source) as traffic_source,
        ANY_VALUE(traffic_gen) as traffic_gen,
        ANY_VALUE(traffic_content) as traffic_content,
        MAX(event_timestamp) as last_event_timestamp,
        COUNTIF(event_name = "ConsultOffer") as nb_consult_offer,
        COUNTIF(event_name = "BookingConfirmation") as nb_booking_confirmation,
        COUNTIF(event_name = "HasAddedOfferToFavorites") as nb_add_to_favorites,
        COUNTIF(event_name = "Share") as nb_share,
        COUNTIF(event_name = "Screenshot") as nb_screenshot,
        COUNTIF(event_name = "UserSetLocation") as nb_set_location,
        COUNTIF(event_name = "ConsultVideo") as nb_consult_video,
        COUNTIF(event_name = "ConsultVenue") as nb_consult_venue,
        COUNTIF(event_name = 'screen_view') as nb_screen_view,
        COUNTIF(event_name = 'screen_view' and firebase_screen = 'Home') as nb_screen_view_home,
        COUNTIF(event_name = 'screen_view' and firebase_screen = 'Search') as nb_screen_view_search,
        COUNTIF(event_name = 'screen_view' and firebase_screen = 'Offer') as nb_screen_view_offer,
        COUNTIF(event_name = 'screen_view' and firebase_screen = 'Profile') as nb_screen_view_profile,
        COUNTIF(event_name = 'screen_view' and firebase_screen = 'Favorites') as nb_screen_view_favorites,
        COUNTIF(event_name = 'screen_view' and firebase_screen in ('Bookings', 'BookingDetails')) as nb_screen_view_bookings,
        COUNTIF((firebase_screen = 'SignupConfirmationEmailSent' or event_name = 'ContinueCGU')) as nb_signup_completed,
        COUNTIF(firebase_screen in ('BeneficiaryRequestSent', 'UnderageAccountCreated', 'BeneficiaryAccountCreated')) as nb_benef_request_sent,
        COUNTIF(event_name = "login") as nb_login,
        DATE_DIFF(MAX(event_timestamp), MIN(event_timestamp), second) as visit_duration_seconds
    from
        {{ ref('int_firebase__native_event') }}

    where
        {% if is_incremental() %}
            -- lag in case session is between two days.
            event_date between DATE_SUB(DATE('{{ ds() }}'), interval 3 + 1 day) and DATE('{{ ds() }}') and
        {% endif %}

        event_name not in (
            'app_remove',
            'os_update',
            'batch_notification_open',
            'batch_notification_display',
            'batch_notification_dismiss',
            'app_update'
        )
    group by
        session_id,
        user_pseudo_id,
        unique_session_id,
        platform,
        app_version
)

select * from visits
-- incremental run only update partition of run day
{% if is_incremental() %}
    where first_event_date between DATE_SUB(DATE('{{ ds() }}'), interval 3 day) and DATE('{{ ds() }}')
{% endif %}
