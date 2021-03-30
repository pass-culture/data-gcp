def aggregate_matomo_offer_events(
    gcp_project, bigquery_raw_dataset, bigquery_clean_dataset
):
    return f"""
        WITH offer_events AS (SELECT event_name,
            `{gcp_project}.{bigquery_raw_dataset}.dehumanize_id`(events.value) AS offer_id,
            FROM `{gcp_project}.{bigquery_clean_dataset}.matomo_events` AS matomo, matomo.event_params AS events
            WHERE events.key = 'offer_id'
        )
        select offer_id,
        SUM(CAST(event_name = 'ConsultOffer' AS INT64)) AS consult_offer,
        SUM(CAST(event_name = 'ConsultOffer_FromHomepage' AS INT64)) AS consult_offer_from_homepage,
        SUM(CAST(event_name = 'AddFavorite_FromHomepage' AS INT64)) AS add_favorite_from_homepage,
        SUM(CAST(event_name = 'BookOfferClick_FromHomepage' AS INT64)) AS book_offer_click_from_homepage,
        SUM(CAST(event_name = 'BookOfferSuccess_FromHomepage' AS INT64)) AS book_offer_success_from_homepage,
        FROM offer_events
        GROUP BY offer_id  
    """


def aggregate_matomo_user_events(gcp_project, bigquery_clean_dataset):
    return f"""
        WITH user_events AS (SELECT event_name, user_id_dehumanized, visits.visit_total_time AS total_time, visits.idvisit AS idvisit, config.device_model
            FROM `{gcp_project}.{bigquery_clean_dataset}.matomo_events` AS matomo, matomo.event_params AS events
            LEFT JOIN `{gcp_project}.{bigquery_clean_dataset}.matomo_visits` visits
            ON matomo.idvisit = visits.idvisit
            LEFT JOIN `{gcp_project}.{bigquery_clean_dataset}.matomo_visits_config` config
            ON matomo.idvisit = config.idvisit
            WHERE events.key = 'offer_id'
        )
        select user_id_dehumanized AS user_id,
        SUM(total_time) AS visit_total_time,
        AVG(total_time) AS visit_avg_time,
        COUNT(DISTINCT idvisit) AS visit_count,
        COUNT(DISTINCT device_model) AS device_model_count,
        SUM(CAST(event_name = 'ConsultOffer_FromHomepage' AS INT64)) AS consult_offer_from_homepage,
        SUM(CAST(event_name = 'ConsultOffer' AS INT64)) AS consult_offer,
        SUM(CAST(event_name = 'AddFavorite_FromHomepage' AS INT64)) AS add_favorite_from_homepage,
        SUM(CAST(event_name = 'BookOfferClick_FromHomepage' AS INT64)) AS book_offer_click_from_homepage,
        SUM(CAST(event_name = 'BookOfferSuccess_FromHomepage' AS INT64)) AS book_offer_success_from_homepage,
        FROM user_events
        GROUP BY user_id  
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
        SUM(CAST(event_name = 'Share' AS INT64)) AS consult_offer_from_homepage,
        SUM(CAST(event_name = 'HasAddedOfferToFavorites' AS INT64)) AS add_offer_to_favorites,
        SUM(CAST(event_name = 'RecommendationModuleSeen' AS INT64)) AS recommendation_module_seen,
        SUM(CAST(event_name = 'ConsultOffer' AS INT64)) AS consult_offer,
        SUM(CAST(event_name = 'ClickBookOffer' AS INT64)) AS click_book_offer,
        from events LEFT JOIN sessions ON events.session_id = sessions.session_id
        WHERE user_id IS NOT NULL
        GROUP BY user_id
    """
