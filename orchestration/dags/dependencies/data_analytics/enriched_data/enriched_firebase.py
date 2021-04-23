def aggregate_firebase_offer_events(gcp_project, bigquery_raw_dataset):
    return f"""
        WITH events AS  (
            SELECT event_name, CAST(CAST(event_params.value.double_value AS INT64) AS STRING) AS offer_id 
            FROM `{gcp_project}.{bigquery_raw_dataset}.events_*` AS events, events.event_params AS event_params
            WHERE event_params.key = 'offerId' AND CAST(event_params.value.double_value AS STRING) != 'nan'
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
        from events 
        WHERE offer_id IS NOT NULL
        GROUP BY offer_id
    """


def aggregate_firebase_user_events(gcp_project, bigquery_raw_dataset):
    return f"""
        WITH events AS  (
            SELECT user_id, event_name, device.mobile_model_name, event_params.value.int_value AS session_id 
            FROM `{gcp_project}.{bigquery_raw_dataset}.events_*` AS events, events.event_params AS event_params
            WHERE event_params.key = 'ga_session_id'
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
        SUM(CAST(event_name = 'screen_view_home' AS INT64)) AS screen_view_home,
        SUM(CAST(event_name = 'screen_view_search' AS INT64)) AS screen_view_search,
        SUM(CAST(event_name = 'screen_view_offer' AS INT64)) AS screen_view_offer,
        SUM(CAST(event_name = 'screen_view_profile' AS INT64)) AS screen_view_profile,
        SUM(CAST(event_name = 'screen_view_favorites' AS INT64)) AS screen_view_favorites,
        SUM(CAST(event_name = 'user_engagement' AS INT64)) AS user_engagement,
        SUM(CAST(event_name = 'AllModulesSeen' AS INT64)) AS all_modules_seen,
        SUM(CAST(event_name = 'AllTilesSeen' AS INT64)) AS all_tiles_seen,
        SUM(CAST(event_name = 'Share' AS INT64)) AS share,
        SUM(CAST(event_name = 'HasAddedOfferToFavorites' AS INT64)) AS has_added_offer_to_favorites,
        SUM(CAST(event_name = 'RecommendationModuleSeen' AS INT64)) AS recommendation_module_seen,
        SUM(CAST(event_name = 'ConsultOffer' AS INT64)) AS consult_offer,
        SUM(CAST(event_name = 'ClickBookOffer' AS INT64)) AS click_book_offer,
        from events LEFT JOIN sessions ON events.session_id = sessions.session_id
        WHERE user_id IS NOT NULL
        GROUP BY user_id
    """
