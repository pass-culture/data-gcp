def aggregate_matomo_offer_events(
    gcp_project, bigquery_raw_dataset, bigquery_clean_dataset
):
    return f"""
        WITH offer_events as (SELECT event_name,
            `{gcp_project}.{bigquery_raw_dataset}.dehumanize_id`(events.value) as offer_id,
            FROM `{gcp_project}.{bigquery_clean_dataset}.matomo_events` as matomo, matomo.event_params as events
            where events.key = 'offer_id'
        )
        select offer_id,
        SUM(CAST(event_name = 'ConsultOffer' AS INT64)) as consult_offer,
        SUM(CAST(event_name = 'ConsultOffer_FromHomepage' AS INT64)) as consult_offer_from_homepage,
        SUM(CAST(event_name = 'AddToFavoritesFromHome' AS INT64)) as add_favorite_from_home,
        SUM(CAST(event_name = 'AddFavorite_FromHomepage' AS INT64)) as add_favorite_from_homepage,
        SUM(CAST(event_name = 'BookOffer_FromHomepage' AS INT64)) as book_offer_from_homepage,
        SUM(CAST(event_name = 'BookOfferClick_FromHomepage' AS INT64)) as book_offer_click_from_homepage,
        SUM(CAST(event_name = 'BookOfferSuccess_FromHomepage' AS INT64)) as book_offer_success_from_homepage,
        from offer_events
        group by offer_id  
    """
