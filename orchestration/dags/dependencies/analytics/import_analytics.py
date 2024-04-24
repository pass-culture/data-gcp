ANALYTICS_SQL_PATH = f"dependencies/analytics/sql/analytics"


def define_import_tables():
    return [
        "beneficiary_fraud_check",
        "booking",
        "collective_offer",
        "collective_offer_domain",
        "criterion",
        "deposit",
        "educational_domain",
        "educational_institution",
        "educational_year",
        "favorite",
        "offer",
        "offer_criterion",
        "offerer",
        "pricing",
        "stock",
        "user",
        "user_offerer",
        "venue",
        "venue_provider",
    ]


analytics_tables = {
    "venue_locations": {
        "sql": f"{ANALYTICS_SQL_PATH}/venue_locations.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        # see associated dependencies
    },
    "offer_moderation": {
        "sql": f"{ANALYTICS_SQL_PATH}/offer_moderation.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "offer_moderation",
    },
    "user_iris": {
        "sql": f"{ANALYTICS_SQL_PATH}/user_iris.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "user_iris",
        "dag_depends": ["export_cloudsql_tables_to_bigquery_v1"],
    },
    "diversification_raw": {
        "sql": f"{ANALYTICS_SQL_PATH}/diversification_raw.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "diversification_raw",
        "params": {
            "diversification_features": [
                "category",
                "sub_category",
                "format",
                "venue_id",
                "extra_category",
            ]
        },
    },
    "diversification_booking": {
        "sql": f"{ANALYTICS_SQL_PATH}/diversification_booking.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "diversification_booking",
        "depends": [
            "diversification_raw",
        ],
        "params": {
            "diversification_features": [
                "category",
                "sub_category",
                "format",
                "venue_id",
                "extra_category",
            ]
        },
    },
    "analytics_firebase_booking_origin": {
        "sql": f"{ANALYTICS_SQL_PATH}/firebase_booking_origin.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_booking_origin${{ yyyymmdd(add_days(ds, 0)) }}",
        "time_partitioning": {"field": "booking_date"},
        "dag_depends": ["import_intraday_firebase_data"],
        "params": {"from": -8, "to": 0},
    },
    "analytics_firebase_booking_origin_catchup": {
        "sql": f"{ANALYTICS_SQL_PATH}/firebase_booking_origin.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_booking_origin${{ yyyymmdd(add_days(ds, -2)) }}",
        "time_partitioning": {"field": "booking_date"},
        "params": {"from": -10, "to": -2},
    },
    "analytics_firebase_home_events_details": {
        "sql": f"{ANALYTICS_SQL_PATH}/firebase_home_events_details.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_home_events_details",
        "time_partitioning": {"field": "event_date"},
        "clustering_fields": {"fields": ["event_type"]},
        "depends": ["diversification_booking"],
        "dag_depends": [
            "import_intraday_firebase_data",
        ],  # computed once a day
    },
    "analytics_firebase_home_macro_conversion": {
        "sql": f"{ANALYTICS_SQL_PATH}/firebase_home_macro_conversion.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_home_macro_conversion",
        "time_partitioning": {"field": "module_displayed_date"},
        "depends": ["diversification_booking"],
        "dag_depends": [
            "import_intraday_firebase_data",
        ],
    },
    "analytics_firebase_home_micro_conversion": {
        "sql": f"{ANALYTICS_SQL_PATH}/firebase_home_micro_conversion.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_home_micro_conversion",
        "time_partitioning": {"field": "module_displayed_date"},
        "depends": ["diversification_booking"],
        "dag_depends": [
            "import_intraday_firebase_data",
        ],
    },
    "analytics_firebase_aggregated_similar_offer_events": {
        "sql": f"{ANALYTICS_SQL_PATH}/firebase_aggregated_similar_offer_events.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_aggregated_similar_offer_events",
        "time_partitioning": {"field": "event_date"},
        "depends": [
            "diversification_booking",
            "analytics_firebase_similar_offer_events",
        ],
    },
    "adage_involved_student": {
        "sql": f"{ANALYTICS_SQL_PATH}/adage_involved_student.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "adage_involved_student",
        "dag_depends": ["import_adage_v1"],
    },
    "adage_involved_institution": {
        "sql": f"{ANALYTICS_SQL_PATH}/adage_involved_institution.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "adage_involved_institution",
        "dag_depends": ["import_adage_v1"],
    },
    "analytics_firebase_recommendation_events": {
        "sql": f"{ANALYTICS_SQL_PATH}/firebase_recommendation_events.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_recommendation_events",
        "time_partitioning": {"field": "event_date"},
        "dag_depends": [
            "export_cloudsql_tables_to_bigquery_v1",
            "import_intraday_firebase_data",
        ],  # computed once a day
    },
    "analytics_firebase_aggregated_search_events": {
        "sql": f"{ANALYTICS_SQL_PATH}/firebase_aggregated_search_events.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_aggregated_search_events",
        "time_partitioning": {"field": "first_date"},
        "depends": ["diversification_booking"],
        "dag_depends": ["import_intraday_firebase_data"],
        "params": {"set_date": "2023-01-01"},
    },
    "bookable_venue_history": {
        "sql": f"{ANALYTICS_SQL_PATH}/bookable_venue_history.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "bookable_partner_history": {
        "sql": f"{ANALYTICS_SQL_PATH}/bookable_partner_history.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "retention_partner_history": {
        "sql": f"{ANALYTICS_SQL_PATH}/retention_partner_history.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": [
            "bookable_partner_history",
        ],
    },
    "funnel_subscription_beneficiary": {
        "sql": f"{ANALYTICS_SQL_PATH}/funnel_subscription_beneficiary.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "funnel_subscription_beneficiary",
        "dag_depends": ["import_intraday_firebase_data"],
    },
    "dms_pro": {
        "sql": f"{ANALYTICS_SQL_PATH}/dms_pro.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "collective_offer_domain_name": {
        "sql": f"{ANALYTICS_SQL_PATH}/collective_offer_domain_name.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
}

aggregated_tables = {
    "aggregated_weekly_user_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/aggregated_weekly_user_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": [
            "diversification_booking",
        ],
        "dag_depends": ["import_intraday_firebase_data"],
    },
    "enriched_partner_retention_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_partner_retention_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": [
            "partner_type_bookability_frequency",
            "enriched_venue_provider_data",
        ],
    },
}


export_tables = dict(analytics_tables, **aggregated_tables)
