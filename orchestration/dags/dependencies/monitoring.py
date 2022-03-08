from dependencies.config import (
    GCP_PROJECT,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    ENV_SHORT_NAME,
    APPLICATIVE_PREFIX,
    TABLE_AB_TESTING,
)

FIREBASE_EVENTS_TABLE = "firebase_events"
RECOMMENDATION_MODULE_SQL_STRING = '("Fais le plein de découvertes", "Nos recommandations pour toi")'  # Ce n'est pas une liste mais une string utilisée directement dans des requêtes


def get_last_event_time_request():
    return f"SELECT MAX(event_timestamp) FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.{FIREBASE_EVENTS_TABLE}_*`;"


def _define_recommendation_booking_funnel(start_date, end_date):
    return f"""
        WITH booking_events AS (
            SELECT event_name, event_timestamp, user_id, 
            MAX(CASE WHEN user_prop.key = "ga_session_id" THEN user_prop.value.int_value ELSE NULL END) AS session_id,
            MAX(CASE WHEN params.key = "moduleName" THEN params.value.string_value ELSE NULL END) AS module,
            MAX(CASE WHEN params.key = "offerId" THEN CAST(params.value.double_value AS INT64) ELSE NULL END) AS double_offer_id,
            MAX(CASE WHEN params.key = "offerId" THEN CAST(params.value.string_value AS INT64) ELSE NULL END) AS string_offer_id,
            MAX(CASE WHEN params.key = "firebase_screen" THEN params.value.string_value ELSE NULL END) AS firebase_screen,
            MAX(CASE WHEN params.key = "firebase_screen_class" THEN params.value.string_value ELSE NULL END) AS screen_view_event
            FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.{FIREBASE_EVENTS_TABLE}_*` events, 
            events.user_properties AS user_prop, events.event_params AS params
            WHERE event_name IN ("screen_view_bookingconfirmation", "ConsultOffer", "screen_view") AND user_id IS NOT NULL
            AND event_timestamp > {start_date}
            AND event_timestamp < {end_date}
            GROUP BY user_id, event_name, event_timestamp
        ),
        booking_funnel AS (
            SELECT *, 
            LEAD(event_name) OVER (PARTITION BY session_id, user_id ORDER BY event_timestamp ASC) AS next_event_name,
            LEAD(screen_view_event) OVER (PARTITION BY session_id, user_id ORDER BY event_timestamp ASC) AS next_screen_view_event,
            (CASE WHEN double_offer_id IS NULL THEN string_offer_id ELSE double_offer_id END) AS offer_id 
            FROM booking_events 
            WHERE event_name IN ("screen_view_bookingconfirmation", "ConsultOffer") or screen_view_event = "BookingConfirmation"
            ORDER BY user_id, session_id, event_timestamp
        ),
        booking_reco_origin AS
        (
            SELECT reco_origin,
            CAST(userid as STRING) as pr_user_id, offerid as pr_offer_id,
            TIMESTAMP_DIFF(TIMESTAMP_MICROS(bk.event_timestamp), CAST(date as TIMESTAMP), SECOND) as timediff, 
            ROW_NUMBER() OVER(PARTITION BY pr.userid, pr.offerid ORDER BY TIMESTAMP_DIFF(TIMESTAMP_MICROS(bk.event_timestamp), CAST(date as TIMESTAMP), SECOND)) AS row_nb,
            FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.past_recommended_offers` pr
            INNER JOIN booking_funnel as bk 
            ON bk.offer_id = pr.offerid and bk.user_id = CAST(pr.userid as STRING)
            WHERE TIMESTAMP_DIFF(TIMESTAMP_MICROS(bk.event_timestamp), CAST(date as TIMESTAMP), SECOND) >= 0
        ),
        recommendation_booking_funnel AS (
            SELECT event_name, event_timestamp, user_id, session_id, firebase_screen, module, booking_funnel.offer_id, next_event_name, groupid AS group_id, offer_subcategoryid,  pastreco.reco_origin,
            FROM booking_funnel
            LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{TABLE_AB_TESTING}` ab_testing
            ON booking_funnel.user_id = ab_testing.userid
            LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_offer` offers
            ON offers.offer_id = CAST(booking_funnel.offer_id AS STRING)
            LEFT JOIN(
                SELECT reco_origin,pr_user_id, pr_offer_id
                FROM booking_reco_origin
                WHERE row_nb=1
                GROUP BY pr_user_id,pr_offer_id,reco_origin
            ) pastreco 
            ON pr_offer_id  = booking_funnel.offer_id AND pr_user_id  = user_id
            WHERE (
                next_event_name = "screen_view_bookingconfirmation" OR (
                    next_event_name = "screen_view" AND next_screen_view_event = "BookingConfirmation"
                )
            ) and event_name = "ConsultOffer" 
        )
    """


def _define_clicks(start_date, end_date):
    return f"""
        WITH clicks AS (
            SELECT user_id, groupid as group_id, event_name, event_date, event_timestamp,  
            MAX(CASE WHEN params.key = "moduleName" THEN params.value.string_value ELSE NULL END) AS module,
            MAX(CASE WHEN params.key = "firebase_screen" THEN params.value.string_value ELSE NULL END) AS firebase_screen,
            MAX(CASE WHEN params.key = "reco_origin" THEN params.value.string_value ELSE NULL END) AS reco_origin,
            FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.{FIREBASE_EVENTS_TABLE}_*` events, events.event_params AS params
            LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{TABLE_AB_TESTING}` ab_testing ON events.user_id = ab_testing.userid
            WHERE event_timestamp > {start_date}
            AND event_timestamp < {end_date}
            GROUP BY event_timestamp, event_date, event_name, user_id, group_id,reco_origin
        )
    """


def _define_favorites(start_date, end_date):
    return f"""
        WITH favorite_events AS (
            SELECT event_name, event_timestamp, user_id, 
            MAX(CASE WHEN params.key = "moduleName" THEN params.value.string_value ELSE NULL END) AS module,
            MAX(CASE WHEN params.key = "offerId" THEN CAST(params.value.double_value AS INT64) ELSE NULL END) AS offer_id,
            MAX(CASE WHEN params.key = "from" THEN params.value.string_value ELSE NULL END) AS origin,
            MAX(CASE WHEN params.key = "reco_origin" THEN params.value.string_value ELSE NULL END) AS reco_origin,
            groupid AS group_id,
            FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.{FIREBASE_EVENTS_TABLE}_*` events, 
            events.user_properties AS user_prop, events.event_params AS params
            LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{TABLE_AB_TESTING}` ab_testing ON events.user_id = ab_testing.userid
            WHERE event_name = "HasAddedOfferToFavorites" 
            AND user_id IS NOT NULL
            AND event_timestamp > {start_date}
            AND event_timestamp < {end_date}
            GROUP BY user_id, event_name, event_timestamp,groupid,reco_origin
        )
    """


def get_favorite_request(start_date, end_date, group_id_list, reco_origin_list):
    group_id_list = sorted(group_id_list)

    return f"""
        {_define_favorites(start_date, end_date)}

        SELECT
        COUNT(*) AS favorites,
        SUM(CAST(origin = "home" AS INT64)) as home_favorites,
        {", ".join([f"SUM(CAST((module IN {RECOMMENDATION_MODULE_SQL_STRING} AND group_id = '{group_id}') AS INT64)) AS recommendation_favorites_{group_id}" for group_id in group_id_list])},
        {",".join([f'SUM(CAST((module IN {RECOMMENDATION_MODULE_SQL_STRING} AND group_id = "{group_id}" AND reco_origin="{reco_origin}") AS INT64)) AS recommendation_favorites_{reco_origin}_{group_id}'  for group_id in group_id_list for reco_origin in reco_origin_list])},
        FROM favorite_events
    """


def get_pertinence_bookings_request(
    start_date, end_date, group_id_list, reco_origin_list
):
    group_id_list = sorted(group_id_list)
    return f"""
        {_define_recommendation_booking_funnel(start_date, end_date)}

        SELECT
        COUNT(*) AS bookings,
        SUM(CAST(firebase_screen = "Home" AS INT64)) as home_bookings,
        SUM(CAST(module IN {RECOMMENDATION_MODULE_SQL_STRING} AS INT64)) AS total_recommendation_bookings,
        {", ".join([f"SUM(CAST((module IN {RECOMMENDATION_MODULE_SQL_STRING} AND group_id = '{group_id}') AS INT64)) AS recommendation_bookings_{group_id}" for group_id in group_id_list])},
        {",".join([f'SUM(CAST((module IN {RECOMMENDATION_MODULE_SQL_STRING} AND group_id = "{group_id}" AND reco_origin="{reco_origin}") AS INT64)) AS recommendation_bookings_{reco_origin}_{group_id}'  for group_id in group_id_list for reco_origin in reco_origin_list])},
        FROM recommendation_booking_funnel
    """


def get_pertinence_clicks_request(start_date, end_date, group_id_list, reco_origin_list):
    group_id_list = sorted(group_id_list)
    return f"""
        {_define_clicks(start_date, end_date)}
        
        SELECT
        COUNT(*) AS clicks,
        SUM(CAST(firebase_screen = "Home" AS INT64)) as home_clicks,
        {", ".join([f"SUM(CAST((module IN {RECOMMENDATION_MODULE_SQL_STRING} AND group_id = '{group_id}') AS INT64)) AS recommendation_clicks_{group_id}" for group_id in group_id_list])},
        {",".join([f'SUM(CAST((module IN {RECOMMENDATION_MODULE_SQL_STRING} AND group_id = "{group_id}" AND reco_origin="{reco_origin}") AS INT64)) AS recommendation_clicks_{reco_origin}_{group_id}'  for group_id in group_id_list for reco_origin in reco_origin_list])},
        FROM clicks
    """


def get_diversification_bookings_request(start_date, end_date):
    return f"""
        {_define_recommendation_booking_funnel(start_date, end_date)},
        
        diversification AS (
            SELECT COUNT(DISTINCT offer_subcategoryid) AS distinct_booking_offer_subcategory_id_count, COUNT(*) AS booking_offer_count, group_id , reco_origin
            FROM recommendation_booking_funnel
            WHERE module IN {RECOMMENDATION_MODULE_SQL_STRING}
            GROUP BY user_id, group_id, reco_origin
        )
        
        SELECT AVG(distinct_booking_offer_subcategory_id_count) as avg_distinct_booking_offer_subcategory_id_count,
        group_id , reco_origin
        FROM diversification 
        GROUP BY reco_origin,group_id
        ORDER BY reco_origin,group_id
    """


def get_recommendations_count(start_date, end_date, group_id_list, reco_origin_list):
    group_id_list = sorted(group_id_list)
    return f"""
        SELECT 
        {", ".join([f"SUM(CAST(groupid = '{group_id}' AS INT64)) AS recommendations_count_{group_id}" for group_id in group_id_list])},
        {",".join([f'SUM(CAST(group_id = "{group_id}" AND reco_origin="{reco_origin}" AS INT64)) AS recommendation_count_{reco_origin}_{group_id}'  for group_id in group_id_list for reco_origin in reco_origin_list])}
        FROM `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.past_recommended_offers` past_recommendations
        LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{TABLE_AB_TESTING}` ab_testing
        ON CAST(past_recommendations.userid AS STRING) = ab_testing.userid
        WHERE date > TIMESTAMP_MICROS({start_date}) AND date < TIMESTAMP_MICROS({end_date})
    """
