from dependencies.config import (
    GCP_PROJECT,
    BIGQUERY_RAW_DATASET,
    BIGQUERY_CLEAN_DATASET,
    BIGQUERY_ANALYTICS_DATASET,
    ENV_SHORT_NAME,
)

FIREBASE_EVENTS_TABLE = "firebase_events"
MONITORING_TABLE = "monitoring_data"
TABLE_AB_TESTING = "ab_testing_202104_v0_v0bis"

LAST_EVENT_TIME_KEY = "last_event_time_key"

metrics_to_compute = {
    "COUNT_CLICK_RECO": compute_click_through_reco_module,
}


def get_last_event_time_request():
    return f"SELECT max(event_timestamp) FROM `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.{FIREBASE_EVENTS_TABLE}`;"

def get_request_click_through_reco_module(start_date, group_id):
    print(f"group_id : {group_id}")
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


def get_insert_metric_request(ti, start_date):
    bigquery_query = f"""INSERT `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.{MONITORING_TABLE}` (compute_time, from_time, last_metric_time, metric_name, metric_value, algorithm_id, environment, group_id) 
    VALUES"""
    last_metric_time = ti.xcom_pull(key=LAST_EVENT_TIME_KEY)
    for metric_id, _ in metrics_to_compute.items():
        for group_id in groups:
            metric_query = f"""
            (   '{datetime.now()}', 
                '{start_date}', 
                TIMESTAMP_SECONDS(CAST(CAST({last_metric_time} as INT64)/1000000 as INT64)), 
                '{metric_id}', 
                {float(ti.xcom_pull(key=f"{metric_id}_{group_id}"))},
                'algo_v0',
                '{ENV_SHORT_NAME}',
                '{group_id}'
            ),"""
            bigquery_query += metric_query
    bigquery_query = f"{bigquery_query[:-1]};"
    return bigquery_query
