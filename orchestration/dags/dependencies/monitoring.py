from dependencies.config import (
    GCP_PROJECT,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    ENV_SHORT_NAME,
)

FIREBASE_EVENTS_TABLE = "firebase_events"
TABLE_AB_TESTING = "ab_testing_202104_v0_v0bis"


def get_last_event_time_request():
    return f"SELECT max(event_timestamp) FROM `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.{FIREBASE_EVENTS_TABLE}`;"


def get_request_click_through_reco_module(start_date, group_id):
    return f"""SELECT COUNT(*) FROM (
            SELECT AS STRUCT event_timestamp, event_date, event_name, user_id, 
                (select event_params.value.string_value
                    from unnest(event_params) event_params
                    where event_params.key = 'moduleName'
                ) as module_name, 
            FROM `{GCP_PROJECT}.{BIGQUERY_CLEAN_DATASET}.{FIREBASE_EVENTS_TABLE}_*` a 
            LEFT JOIN `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.{TABLE_AB_TESTING}` b on  a.user_id =  b.userid, 
            UNNEST(event_params) as ep 
            WHERE event_name = 'ConsultOffer' 
            AND ep.key = 'moduleName' 
            AND ep.value.string_value = 'Fais le plein de découvertes' 
            AND b.groupid = '{group_id}'
            AND TIMESTAMP_SECONDS(CAST(CAST(event_timestamp as INT64)/1000000 as INT64)) > TIMESTAMP_SECONDS(CAST(CAST({start_date} as INT64)/1000000 as INT64))
        );"""
