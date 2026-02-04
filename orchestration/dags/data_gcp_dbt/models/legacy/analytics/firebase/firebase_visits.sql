{{
    config(
        **custom_incremental_config(
            partition_by={
                "field": "first_event_date",
                "data_type": "date",
                "granularity": "day",
                "time_ingestion_partitioning": false,
            },
            incremental_strategy="insert_overwrite",
            on_schema_change="append_new_columns",
        )
    )
}}

with
    visits as (
        select
            session_id,
            user_pseudo_id,
            unique_session_id,
            platform,
            app_version,
            any_value(session_number) as session_number,
            any_value(user_id) as user_id,
            min(event_timestamp) as first_event_timestamp,
            date(min(event_timestamp)) as first_event_date,
            any_value(traffic_campaign) as traffic_campaign,
            any_value(traffic_medium) as traffic_medium,
            any_value(traffic_source) as traffic_source,
            any_value(traffic_gen) as traffic_gen,
            any_value(traffic_content) as traffic_content,
            any_value(
                if(event_name = 'AppThemeStatus', theme_setting, null)
            ) as theme_setting,
            max(event_timestamp) as last_event_timestamp,
            countif(event_name = "ConsultOffer") as nb_consult_offer,
            countif(event_name = "BookingConfirmation") as nb_booking_confirmation,
            countif(event_name = "HasAddedOfferToFavorites") as nb_add_to_favorites,
            countif(event_name = "Share") as nb_share,
            countif(event_name = "Screenshot") as nb_screenshot,
            countif(event_name = "UserSetLocation") as nb_set_location,
            countif(event_name = "ConsultVideo") as nb_consult_video,
            countif(event_name = "ConsultVenue") as nb_consult_venue,
            countif(event_name = "ConsultVenueMap") as nb_consult_venue_map,
            countif(event_name = "ConsultArtist") as nb_consult_artist,
            countif(event_name = "screen_view") as nb_screen_view,
            countif(
                event_name = "screen_view" and firebase_screen = "Home"
            ) as nb_screen_view_home,
            countif(
                event_name = "screen_view" and firebase_screen = "Search"
            ) as nb_screen_view_search,
            countif(
                event_name = "screen_view" and firebase_screen = "Offer"
            ) as nb_screen_view_offer,
            countif(
                event_name = "screen_view" and firebase_screen = "Profile"
            ) as nb_screen_view_profile,
            countif(
                event_name = "screen_view" and firebase_screen = "Favorites"
            ) as nb_screen_view_favorites,
            countif(
                event_name = "screen_view"
                and firebase_screen in ("Bookings", "BookingDetails")
            ) as nb_screen_view_bookings,
            countif(
                (
                    firebase_screen = "SignupConfirmationEmailSent"
                    or event_name = "ContinueCGU"
                )
            ) as nb_signup_completed,
            countif(
                firebase_screen in (
                    "BeneficiaryRequestSent",
                    "UnderageAccountCreated",
                    "BeneficiaryAccountCreated"
                )
            ) as nb_benef_request_sent,
            countif(event_name = "login") as nb_login,
            date_diff(
                max(event_timestamp), min(event_timestamp), second
            ) as visit_duration_seconds
        from {{ ref("int_firebase__native_event") }}

        where
            {% if is_incremental() %}
                -- lag in case session is between two days.
                event_date
                between date_sub(date('{{ ds() }}'), interval 3 + 1 day) and date(
                    '{{ ds() }}'
                )
                and
            {% endif %}

            event_name not in (
                "app_remove",
                "os_update",
                "batch_notification_open",
                "batch_notification_display",
                "batch_notification_dismiss",
                "app_update"
            )
        group by session_id, user_pseudo_id, unique_session_id, platform, app_version
    )

select *
from visits
-- incremental run only update partition of run day
{% if is_incremental() %}
    where
        first_event_date
        between date_sub(date('{{ ds() }}'), interval 3 day) and date('{{ ds() }}')
{% endif %}
