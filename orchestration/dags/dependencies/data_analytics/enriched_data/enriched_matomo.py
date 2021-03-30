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
