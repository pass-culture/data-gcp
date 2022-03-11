from datetime import datetime, timedelta
import pytz

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from dependencies.slack_alert import task_fail_slack_alert
from dependencies.bigquery_client import BigQueryClient
from dependencies.monitoring import (
    get_pertinence_clicks_request,
    get_last_event_time_request,
    get_diversification_bookings_request,
    get_pertinence_bookings_request,
    get_recommendations_count,
    get_favorite_request,
)

from dependencies.config import GCP_PROJECT, BIGQUERY_ANALYTICS_DATASET, ENV_SHORT_NAME

MONITORING_TABLE = "monitoring_data_v202203"
# A changer en fonction des dates de changement d'algo

START_DATE = datetime(2021, 11, 9, tzinfo=pytz.utc)  # expressed in UTC TimeZone
groups = ["A", "B", "C"]
reco_origin_list = {"cold_start", "algo"}
ALGO_A = "algo v1 + cold start"
ALGO_B = "algo v2 + Deep Reco"
ALGO_C = "algo v2 + Matrix Factorization"
# ALGO_B = "algo v0 + cold start"

LAST_EVENT_TIME_KEY = "last_event_time"


def convert_datetime_to_microseconds(date_time):
    epoch = datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
    return int((date_time - epoch).total_seconds() * 1000 * 1000)


def get_last_data_timestamp(ti, **kwargs):
    bigquery_client = BigQueryClient()
    bigquery_query = get_last_event_time_request()
    results = bigquery_client.query(bigquery_query)
    result = int(results.values[0][0])
    ti.xcom_push(key=LAST_EVENT_TIME_KEY, value=result)


def compute_click_pertinence_metrics(ti, **kwargs):
    start_date = convert_datetime_to_microseconds(START_DATE)
    end_date = ti.xcom_pull(key=LAST_EVENT_TIME_KEY)
    bigquery_client = BigQueryClient()
    results = bigquery_client.query(
        get_pertinence_clicks_request(start_date, end_date, groups, reco_origin_list)
    )
    for index, metric in enumerate(
        ["CLICKS", "HOME_CLICKS", "TOTAL_RECOMMENDATION_CLICKS"]
        + [f"RECOMMENDATION_CLICKS_{group_id}" for group_id in groups]
        + [
            f"RECOMMENDATION_CLICKS_{reco_origin}_{group_id}"
            for group_id in groups
            for reco_origin in reco_origin_list
        ]
    ):
        result = float(results.values[0][index])
        ti.xcom_push(key=metric, value=result)


def compute_booking_pertinence_metrics(ti, **kwargs):
    start_date = convert_datetime_to_microseconds(START_DATE)
    end_date = ti.xcom_pull(key=LAST_EVENT_TIME_KEY)
    bigquery_client = BigQueryClient()
    results = bigquery_client.query(
        get_pertinence_bookings_request(start_date, end_date, groups, reco_origin_list)
    )
    for index, metric in enumerate(
        ["BOOKINGS", "HOME_BOOKINGS", "TOTAL_RECOMMENDATION_BOOKINGS"]
        + [f"RECOMMENDATION_BOOKINGS_{group_id}" for group_id in groups]
        + [
            f"RECOMMENDATION_BOOKINGS_{reco_origin}_{group_id}"
            for group_id in groups
            for reco_origin in reco_origin_list
        ]
    ):
        result = float(results.values[0][index])
        ti.xcom_push(key=metric, value=result)


def compute_booking_diversification_metrics(ti, **kwargs):
    start_date = convert_datetime_to_microseconds(START_DATE)
    end_date = ti.xcom_pull(key=LAST_EVENT_TIME_KEY)
    bigquery_client = BigQueryClient()
    results = bigquery_client.query(
        get_diversification_bookings_request(start_date, end_date)
    )
    for index, metric in enumerate(
        [f"AVERAGE_CATEGORY_RECO_{group_id}" for group_id in groups]
        + [
            f"AVERAGE_CATEGORY_RECO_{reco_origin}_{group_id}"
            for group_id in groups
            for reco_origin in reco_origin_list
        ]
    ):
        result = None
        if len(results.values) > index:
            if len(results.values[index]) > 0:
                result = float(results.values[index][0])
        ti.xcom_push(key=metric, value=result)


def compute_recommendations_count_metrics(ti, **kwargs):
    start_date = convert_datetime_to_microseconds(START_DATE)
    end_date = ti.xcom_pull(key=LAST_EVENT_TIME_KEY)
    bigquery_client = BigQueryClient()
    results = bigquery_client.query(
        get_recommendations_count(start_date, end_date, groups, reco_origin_list)
    )
    for index, metric in enumerate(
        [f"RECOMMENDATIONS_COUNT_{group_id}" for group_id in groups]
        + [
            f"RECOMMENDATIONS_COUNT_{reco_origin}_{group_id}"
            for group_id in groups
            for reco_origin in reco_origin_list
        ]
    ):
        result = float(results.values[0][index])
        ti.xcom_push(key=metric, value=result)


def compute_favorites_metrics(ti, **kwargs):
    start_date = convert_datetime_to_microseconds(START_DATE)
    end_date = ti.xcom_pull(key=LAST_EVENT_TIME_KEY)
    bigquery_client = BigQueryClient()
    results = bigquery_client.query(
        get_favorite_request(start_date, end_date, groups, reco_origin_list)
    )
    for index, metric in enumerate(
        ["FAVORITES", "HOME_FAVORITES", "TOTAL_RECOMMENDATION_FAVORITES"]
        + [f"RECOMMENDATION_FAVORITES_{group_id}" for group_id in groups]
        + [
            f"RECOMMENDATION_FAVORITES_{reco_origin}_{group_id}"
            for group_id in groups
            for reco_origin in reco_origin_list
        ]
    ):
        result = float(results.values[0][index])
        ti.xcom_push(key=metric, value=result)


metric_groups_to_compute = {
    "PERTINENCE_CLICKS": {
        "function": compute_click_pertinence_metrics,
        "metric_list": [
            {"name": "CLICKS", "ab_testing": False},
            {"name": "HOME_CLICKS", "ab_testing": False},
            {"name": "TOTAL_RECOMMENDATION_CLICKS", "ab_testing": False},
            {"name": "RECOMMENDATION_CLICKS", "ab_testing": True},
            {"name": "RECOMMENDATION_CLICKS_cold_start", "ab_testing": True},
            {"name": "RECOMMENDATION_CLICKS_algo", "ab_testing": True},
        ],
    },
    "DIVERSIFICATION_BOOKING": {
        "function": compute_booking_diversification_metrics,
        "metric_list": [
            {"name": "AVERAGE_CATEGORY_RECO", "ab_testing": True},
            {"name": "AVERAGE_CATEGORY_RECO_cold_start", "ab_testing": True},
            {"name": "AVERAGE_CATEGORY_RECO_algo", "ab_testing": True},
        ],
    },
    "PERTINENCE_BOOKINGS": {
        "function": compute_booking_pertinence_metrics,
        "metric_list": [
            {"name": "BOOKINGS", "ab_testing": False},
            {"name": "HOME_BOOKINGS", "ab_testing": False},
            {"name": "TOTAL_RECOMMENDATION_BOOKINGS", "ab_testing": False},
            {"name": "RECOMMENDATION_BOOKINGS", "ab_testing": True},
            {"name": "RECOMMENDATION_BOOKINGS_cold_start", "ab_testing": True},
            {"name": "RECOMMENDATION_BOOKINGS_algo", "ab_testing": True},
        ],
    },
    "RECOMMENDATION_COUNT": {
        "function": compute_recommendations_count_metrics,
        "metric_list": [
            {"name": "RECOMMENDATIONS_COUNT", "ab_testing": True},
            {"name": "RECOMMENDATIONS_COUNT_cold_start", "ab_testing": True},
            {"name": "RECOMMENDATIONS_COUNT_algo", "ab_testing": True},
        ],
    },
    "FAVORITES": {
        "function": compute_favorites_metrics,
        "metric_list": [
            {"name": "FAVORITES", "ab_testing": False},
            {"name": "HOME_FAVORITES", "ab_testing": False},
            {"name": "TOTAL_RECOMMENDATION_FAVORITES", "ab_testing": False},
            {"name": "RECOMMENDATION_FAVORITES", "ab_testing": True},
            {"name": "RECOMMENDATION_FAVORITES_cold_start", "ab_testing": True},
            {"name": "RECOMMENDATION_FAVORITES_algo", "ab_testing": True},
        ],
    },
}


def get_insert_metric_request(ti, start_date):
    bigquery_query = f"""INSERT `{GCP_PROJECT}.{BIGQUERY_ANALYTICS_DATASET}.{MONITORING_TABLE}` (compute_time, from_time, last_metric_time, metric_name, metric_value, algorithm_id, environment, group_id) 
    VALUES"""
    last_metric_time = ti.xcom_pull(key=LAST_EVENT_TIME_KEY)
    for metric_group in list(metric_groups_to_compute.keys()):
        metric_list = metric_groups_to_compute[metric_group]["metric_list"]
        for metric in metric_list:
            if metric["ab_testing"]:
                for group_id in groups:
                    metric_value = ti.xcom_pull(key=f"{metric['name']}_{group_id}")
                    metric_query = f"""
                    (   '{datetime.now()}', 
                        '{start_date}', 
                        TIMESTAMP_MICROS({last_metric_time}), 
                        '{metric["name"]}_{group_id}', 
                        {float(metric_value) if metric_value and str(metric_value) != 'nan' else 'NULL'},
                        '{eval(f"ALGO_{group_id}")}',
                        '{ENV_SHORT_NAME}',
                        '{group_id}'
                    ),"""
                    bigquery_query += metric_query
            else:
                metric_value = ti.xcom_pull(key=metric["name"])
                metric_query = f"""
                (   '{datetime.now()}', 
                    '{start_date}', 
                    TIMESTAMP_MICROS({last_metric_time}), 
                    '{metric["name"]}', 
                    {float(metric_value) if metric_value and str(metric_value) != 'nan' else 'NULL'},
                    'NULL',
                    '{ENV_SHORT_NAME}',
                    'NULL'
                ),"""
                bigquery_query += metric_query
    bigquery_query = f"{bigquery_query[:-1]};"
    return bigquery_query


def insert_metric_bq(ti, **kwargs):
    bigquery_client = BigQueryClient()
    bigquery_query = get_insert_metric_request(ti, START_DATE)
    bigquery_client.query(bigquery_query)


default_args = {
    "start_date": datetime(2021, 5, 26),
    "on_failure_callback": task_fail_slack_alert,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "compute_monitoring",
    default_args=default_args,
    description="Compute monitoring metrics",
    schedule_interval="0 1 * * *",
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
) as dag:

    start = DummyOperator(task_id="start")

    init_dag = PythonOperator(
        task_id="get_last_data_timestamp",
        python_callable=get_last_data_timestamp,
        provide_context=True,
    )

    compute_metric_task = [
        PythonOperator(
            task_id=f"compute_{metric_group}",
            python_callable=metric_groups_to_compute[metric_group]["function"],
            provide_context=True,
        )
        for metric_group in list(metric_groups_to_compute.keys())
    ]

    insert_metric_bq = PythonOperator(
        task_id=f"insert_metric_bigquery",
        python_callable=insert_metric_bq,
        provide_context=True,
    )

    end = DummyOperator(task_id="end")


start >> init_dag >> compute_metric_task >> insert_metric_bq >> end
