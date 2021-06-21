from dependencies.config import (
    GCP_PROJECT,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    ENV_SHORT_NAME,
    APPLICATIVE_PREFIX,
)

FIREBASE_EVENTS_TABLE = "firebase_events"
TABLE_AB_TESTING = "ab_testing_202104_v0_v0bis"
RECOMMENDATION_MODULE_TITLE = "Fais le plein de découvertes"


def get_last_event_time_request():
    return f"SELECT MAX(event_timestamp) FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.{FIREBASE_EVENTS_TABLE}_*`;"


def get_click_through_recommendation_module_request(start_date, end_date, group_id):
    return f"""
        SELECT COUNT(*) FROM (
            SELECT AS STRUCT event_timestamp, event_date, event_name, user_id, 
            (select event_params.value.string_value
                from unnest(event_params) event_params
                where event_params.key = 'moduleName'
            ) as module_name, 
            FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.{FIREBASE_EVENTS_TABLE}_*` events
            LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{TABLE_AB_TESTING}` ab_testing ON events.user_id = ab_testing.userid, 
            UNNEST(event_params) AS params
            WHERE event_name = 'ConsultOffer' 
            AND params.key = 'moduleName' 
            AND params.value.string_value = '{RECOMMENDATION_MODULE_TITLE}' 
            AND ab_testing.groupid = '{group_id}'
            AND event_timestamp > {start_date}
            AND event_timestamp < {end_date}
        );
        """


def get_average_booked_category_request(start_date, end_date):
    return f"""
        WITH booking_events AS (
        SELECT event_name, event_timestamp, user_id, 
        MAX(CASE WHEN user_prop.key = "ga_session_id" THEN user_prop.value.int_value ELSE NULL END) AS session_id,
        MAX(CASE WHEN params.key = "moduleName" THEN params.value.string_value ELSE NULL END) AS module,
        MAX(CASE WHEN params.key = "offerId" THEN CAST(params.value.double_value AS INT64) ELSE NULL END) AS offer_id,
        MAX(CASE WHEN params.key = "firebase_screen" THEN params.value.string_value ELSE NULL END) AS firebase_screen,
        FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.{FIREBASE_EVENTS_TABLE}_*` events, 
        events.user_properties AS user_prop, events.event_params AS params
        WHERE event_name IN ("screen_view_bookingconfirmation", "ConsultOffer") AND user_id IS NOT NULL
        AND event_timestamp > {start_date}
        AND event_timestamp < {end_date}
        GROUP BY user_id, event_name, event_timestamp
    ),
    booking_funnel AS (
        SELECT *, LEAD(event_name) OVER (PARTITION BY session_id ORDER BY event_timestamp ASC) AS next_event_name FROM booking_events 
        ORDER BY user_id, session_id, event_timestamp
    ),
    recommendation_booking_funnel AS (
        SELECT event_name, event_timestamp, user_id, session_id, firebase_screen, module, booking_funnel.offer_id, next_event_name, groupid AS group_id, offer_type 
        FROM booking_funnel
        LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.ab_testing_202104_v0_v0bis` ab_testing
        ON booking_funnel.user_id = ab_testing.userid
        LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.applicative_database_offer` offers
        ON offers.offer_id = CAST(booking_funnel.offer_id AS STRING)
        WHERE next_event_name = "screen_view_bookingconfirmation" and event_name = "ConsultOffer" 
        AND module = "{RECOMMENDATION_MODULE_TITLE}" 
    ),
    
    diversification AS (
        SELECT COUNT(DISTINCT offer_type) AS distinct_booking_offer_type_count, COUNT(*) AS booking_offer_count, group_id 
        FROM recommendation_booking_funnel
        GROUP BY user_id, group_id
    )
    
    SELECT AVG(distinct_booking_offer_type_count) as avg_distinct_booking_offer_type_count,
    group_id 
    FROM diversification 
    GROUP BY group_id
    ORDER BY group_id
    """


def get_average_booked_category_request(start_date):
    return f"""
        WITH booking_events AS (
            SELECT event_name, event_timestamp, user_id, 
            MAX(CASE WHEN user_prop.key = "ga_session_id" THEN user_prop.value.int_value ELSE NULL END) AS session_id,
            MAX(CASE WHEN params.key = "moduleName" THEN params.value.string_value ELSE NULL END) AS module,
            MAX(CASE WHEN params.key = "offerId" THEN CAST(params.value.double_value AS INT64) ELSE NULL END) AS offer_id,
            FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.{FIREBASE_EVENTS_TABLE}_*` events, events.user_properties AS user_prop, events.event_params AS params
            WHERE event_name IN ("screen_view_bookingconfirmation", "ConsultOffer") 
            AND user_id IS NOT NULL
            AND TIMESTAMP_SECONDS(CAST(CAST(event_timestamp as INT64)/1000000 as INT64)) > TIMESTAMP_SECONDS(CAST(CAST({start_date} as INT64)/1000000 as INT64))
            GROUP BY user_id, event_name, event_timestamp
        ),
        booking_funnel AS (
            SELECT *, LEAD(event_name) OVER (PARTITION BY session_id ORDER BY event_timestamp ASC) AS next_event_name FROM booking_events 
            ORDER BY user_id, session_id, event_timestamp
        ),
        recommendation_booking_funnel AS (
            SELECT event_name, event_timestamp, user_id, session_id, module, booking_funnel.offer_id, next_event_name, groupid AS group_id, offer_type 
            FROM booking_funnel
            LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{TABLE_AB_TESTING}` ab_testing
            ON booking_funnel.user_id = ab_testing.userid
            LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.{APPLICATIVE_PREFIX}offer` offers
            ON offers.offer_id = CAST(booking_funnel.offer_id AS STRING)
            WHERE next_event_name = "screen_view_bookingconfirmation" and event_name = "ConsultOffer" 
            AND module = "Fais le plein de découvertes" 
        ),

        diversification AS (
            SELECT COUNT(DISTINCT offer_type) AS distinct_booking_offer_type_count, COUNT(*) AS booking_offer_count, group_id 
            FROM recommendation_booking_funnel
            GROUP BY user_id, group_id
        )

        SELECT AVG(distinct_booking_offer_type_count), group_id FROM diversification 
        WHERE booking_offer_count > 1
        GROUP BY group_id
    """
