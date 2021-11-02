def aggregate_firebase_offer_events(gcp_project, bigquery_raw_dataset):
    return f"""
        WITH events AS  (
            SELECT event_name, CAST(event_params.value.double_value AS STRING) AS double_offer_id ,
            event_params.value.string_value AS string_offer_id
            FROM `{gcp_project}.{bigquery_raw_dataset}.events_*` AS events, events.event_params AS event_params
            WHERE event_params.key = 'offerId'
        ),
        cleaned_events AS (
            SELECT * EXCEPT(double_offer_id, string_offer_id), 
            (CASE WHEN double_offer_id IS NULL THEN string_offer_id ELSE double_offer_id END) AS offer_id
            FROM events
        )
        
        SELECT offer_id, 
        SUM(CAST(event_name = 'ConsultOffer' AS INT64)) AS consult_offer,
        SUM(CAST(event_name = 'ConsultWholeOffer' AS INT64)) AS consult_whole_offer,
        SUM(CAST(event_name = 'ExclusivityBlockClicked' AS INT64)) AS exclusivity_block_clicked,
        SUM(CAST(event_name = 'ConsultDescriptionDetails' AS INT64)) AS consult_description_details,
        SUM(CAST(event_name = 'ClickBookOffer' AS INT64)) AS click_book_offer,
        SUM(CAST(event_name = 'ConsultAvailableDates' AS INT64)) AS consult_available_dates,
        SUM(CAST(event_name = 'Share' AS INT64)) AS share,
        SUM(CAST(event_name = 'ConsultAccessibilityModalities' AS INT64)) AS consult_accessibility_modalities,
        SUM(CAST(event_name = 'ConsultWithdrawalModalities' AS INT64)) AS consult_withdrawal_modalities,
        SUM(CAST(event_name = 'ConsultLocationItinerary' AS INT64)) AS consult_location_itinerary,
        SUM(CAST(event_name = 'HasAddedOfferToFavorites' AS INT64)) AS has_added_offer_to_favorites
        from cleaned_events 
        WHERE offer_id IS NOT NULL
        GROUP BY offer_id
    """


def aggregate_firebase_user_events(gcp_project, bigquery_raw_dataset):
    return f"""
                WITH events AS  (
            SELECT user_id, event_name, device.mobile_model_name,
            (select event_params.value.int_value
                from unnest(event_params) event_params
                where event_params.key = 'ga_session_id'
            ) as session_id,
            (select event_params.value.string_value
                from unnest(event_params) event_params
                where event_params.key = 'firebase_screen_class'
            ) as firebase_screen_class,
            FROM `{gcp_project}.{bigquery_raw_dataset}.events_*` AS events
        ),
        sessions AS ( 
            SELECT ROUND((max(event_timestamp) - min(event_timestamp))/(1000 * 1000), 1) AS total_time, 
            event_params.value.int_value AS session_id 
            FROM `{gcp_project}.{bigquery_raw_dataset}.events_*` AS events, events.event_params AS event_params
            WHERE event_params.key = 'ga_session_id'
            GROUP BY session_id
        )  
        SELECT user_id, 
        SUM(total_time) AS visit_total_time,
        AVG(total_time) AS visit_avg_time,
        COUNT(DISTINCT events.session_id) AS visit_count,
        COUNT(DISTINCT mobile_model_name) AS device_model_count,
        SUM(CAST(event_name = 'screen_view' AS INT64)) AS screen_view,
        SUM(CAST(event_name = 'screen_view' AND firebase_screen_class = 'Home' AS INT64)) AS screen_view_home,
        SUM(CAST(event_name = 'screen_view' AND firebase_screen_class = 'Search' AS INT64)) AS screen_view_search,
        SUM(CAST(event_name = 'screen_view' AND firebase_screen_class = 'Offer' AS INT64)) AS screen_view_offer,
        SUM(CAST(event_name = 'screen_view' AND firebase_screen_class = 'Profile' AS INT64)) AS screen_view_profile,
        SUM(CAST(event_name = 'screen_view' AND firebase_screen_class = 'Favorites' AS INT64)) AS screen_view_favorites,
        SUM(CAST(event_name = 'user_engagement' AS INT64)) AS user_engagement,
        SUM(CAST(event_name = 'AllModulesSeen' AS INT64)) AS all_modules_seen,
        SUM(CAST(event_name = 'AllTilesSeen' AS INT64)) AS all_tiles_seen,
        SUM(CAST(event_name = 'Share' AS INT64)) AS share,
        SUM(CAST(event_name = 'HasAddedOfferToFavorites' AS INT64)) AS has_added_offer_to_favorites,
        SUM(CAST(event_name = 'RecommendationModuleSeen' AS INT64)) AS recommendation_module_seen,
        SUM(CAST(event_name = 'ConsultOffer' AS INT64)) AS consult_offer,
        SUM(CAST(event_name = 'ClickBookOffer' AS INT64)) AS click_book_offer,
        FROM events LEFT JOIN sessions ON events.session_id = sessions.session_id
        WHERE user_id IS NOT NULL
        GROUP BY user_id
    """


def aggregate_firebase_visits(gcp_project, bigquery_raw_dataset):
    return f"""
    WITH base AS (
        SELECT
            event_name, event_timestamp, user_id,user_pseudo_id, user_first_touch_timestamp,
            device.category, device.mobile_brand_name, device.operating_system, device.operating_system_version,
            traffic_source.name, traffic_source.medium, traffic_source.source,
            (select event_params.value.int_value
                from unnest(event_params) event_params
                where event_params.key = 'ga_session_id'
            ) as session_id,
            (select event_params.value.int_value
                from unnest(event_params) event_params
                where event_params.key = 'ga_session_number'
            ) as session_number,
        FROM `{gcp_project}.{bigquery_raw_dataset}.events_*`
        WHERE event_name NOT IN ('app_remove', 'os_update', 'batch_notification_open','batch_notification_display', 'batch_notification_dismiss')
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
        TIMESTAMP_SECONDS(CAST(MIN(user_first_touch_timestamp)/1000000 as INT64)) AS user_first_touch_timestamp,
        ANY_VALUE(name) AS name,
        ANY_VALUE(medium) AS medium,
        ANY_VALUE(source) AS source,
        TIMESTAMP_SECONDS(CAST(MAX(event_timestamp)/1000000 as INT64)) AS last_event_timestamp,
        COUNTIF(event_name="ConsultOffer") AS nb_consult_offer,
        COUNTIF(event_name="BookingConfirmation") AS nb_booking_confirmation,
        DATE_DIFF(TIMESTAMP_SECONDS(CAST(MAX(event_timestamp)/1000000 as INT64)), TIMESTAMP_SECONDS(CAST(MIN(user_first_touch_timestamp)/1000000 as INT64)),SECOND) AS visit_duration_seconds,
    FROM base
    GROUP BY session_id,user_pseudo_id;
    """


def copy_table_to_analytics(gcp_project, bigquery_raw_dataset, execution_date):
    return f"""
    WITH temp_firebase_events AS (
        SELECT
            event_name, user_pseudo_id, user_id, platform,
            traffic_source.name,traffic_source.medium,traffic_source.source,
            PARSE_DATE("%Y%m%d", event_date) AS event_date,
            TIMESTAMP_SECONDS(CAST(CAST(event_timestamp as INT64)/1000000 as INT64)) AS event_timestamp,
            TIMESTAMP_SECONDS(CAST(CAST(event_previous_timestamp as INT64)/1000000 as INT64)) AS event_previous_timestamp,
            TIMESTAMP_SECONDS(CAST(CAST(event_timestamp as INT64)/1000000 as INT64)) AS user_first_touch_timestamp,
            (select event_params.value.string_value
                from unnest(event_params) event_params
                where event_params.key = 'firebase_screen'
            ) as firebase_screen,
            (select event_params.value.string_value
                from unnest(event_params) event_params
                where event_params.key = 'firebase_previous_screen'
            ) as firebase_previous_screen,
            (select event_params.value.int_value
                from unnest(event_params) event_params
                where event_params.key = 'ga_session_number'
            ) as session_number,
            (select event_params.value.int_value
                from unnest(event_params) event_params
                where event_params.key = 'ga_session_id'
            ) as session_id,
            (select event_params.value.string_value
                from unnest(event_params) event_params
                where event_params.key = 'pageName'
            ) as page_name,
            (select CAST(event_params.value.double_value AS STRING)
                from unnest(event_params) event_params
                where event_params.key = 'offerId'
            ) as double_offer_id,
            (select event_params.value.string_value
                from unnest(event_params) event_params
                where event_params.key = 'offerId'
            ) as string_offer_id,
            (select event_params.value.string_value
                from unnest(event_params) event_params
                where event_params.key = 'from'
            ) as origin,
            (select event_params.value.string_value
                from unnest(event_params) event_params
                where event_params.key = 'query'
            ) as query,
            (select event_params.value.string_value
                from unnest(event_params) event_params
                where event_params.key = 'moduleName'
            ) as module_name,
            (select event_params.value.string_value
                from unnest(event_params) event_params
                where event_params.key = 'traffic_campaign'
            ) as traffic_campaign,
            (select event_params.value.string_value
                from unnest(event_params) event_params
                where event_params.key = 'traffic_medium'
            ) as traffic_medium,
            (select event_params.value.string_value
                from unnest(event_params) event_params
                where event_params.key = 'traffic_source'
            ) as traffic_source
        FROM {gcp_project}.{bigquery_raw_dataset}.events_{execution_date}
    )
    SELECT * EXCEPT(double_offer_id, string_offer_id),
    (CASE WHEN double_offer_id IS NULL THEN string_offer_id ELSE double_offer_id END) AS offer_id
    FROM temp_firebase_events
    """
