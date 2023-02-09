from common.config import ENV_SHORT_NAME

ANALYTICS_SQL_PATH = f"dependencies/import_analytics/sql/analytics"


def define_import_tables():
    return [
        "action_history",
        "allocine_pivot",
        "allocine_venue_provider",
        "allocine_venue_provider_price_rule",
        "bank_information",
        "beneficiary_fraud_check",
        "beneficiary_fraud_review",
        "beneficiary_import",
        "beneficiary_import_status",
        "booking",
        "business_unit",
        "cashflow",
        "cashflow_batch",
        "cashflow_log",
        "cashflow_pricing",
        "cds_cinema_details",
        "cinema_provider_pivot",
        "collective_booking",
        "collective_offer",
        "collective_offer_domain",
        "collective_offer_template",
        "collective_offer_template_domain",
        "collective_stock",
        "criterion",
        "deposit",
        "educational_deposit",
        "educational_domain",
        "educational_domain_venue",
        "educational_institution",
        "educational_redactor",
        "educational_year",
        "favorite",
        "feature",
        "individual_booking",
        "internal_user",
        "invoice",
        "local_provider_event",
        "mediation",
        "offer",
        "offer_criterion",
        "offer_report",
        "offerer",
        "offerer_tag",
        "offerer_tag_mapping",
        "payment",
        "payment_message",
        "payment_status",
        "pricing",
        "pricing_line",
        "pricing_log",
        "product",
        "provider",
        "recredit",
        "stock",
        "transaction",
        "user",
        "user_offerer",
        "user_suspension",
        "venue",
        "venue_contact",
        "venue_educational_status",
        "venue_label",
        "venue_pricing_point_link",
        "venue_provider",
        "venue_reimbursement_point_link",
        "venue_type",
    ]


analytics_tables = {
    "available_stock_information": {
        "sql": f"{ANALYTICS_SQL_PATH}/available_stock_information.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "enriched_booking_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_booking_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "enriched_collective_booking_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_collective_booking_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "enriched_collective_offer_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_collective_offer_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "enriched_deposit_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_deposit_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "enriched_institution_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_institution_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": ["enriched_user_data"],
    },
    "enriched_offer_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_offer_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": [
            "enriched_stock_data",
            "isbn_editor",
            "offer_extracted_data",
            "offer_item_ids",
        ],
    },
    "enriched_offerer_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_offerer_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": ["enriched_offer_data"],
    },
    "enriched_stock_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_stock_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": ["stock_booking_information", "available_stock_information"],
    },
    "enriched_suivi_dms_adage": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_suivi_dms_adage.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": ["enriched_offerer_data", "enriched_venue_data"],
        "dag_depends": [
            "import_typeform_adage_reference_request",
            "import_adage_v1",
            "import_dms_subscriptions",
        ],
    },
    "enriched_user_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_user_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": ["enriched_deposit_data"],
    },
    "enriched_venue_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_venue_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": ["enriched_offer_data"],
    },
    "iris_venues_in_shape": {
        "sql": f"{ANALYTICS_SQL_PATH}/iris_venues_in_shape.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "iris_venues_at_radius": {
        "sql": f"{ANALYTICS_SQL_PATH}/iris_venues_at_radius.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "isbn_editor": {
        "sql": f"{ANALYTICS_SQL_PATH}/isbn_editor.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "offer_extracted_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/offer_extracted_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "stock_booking_information": {
        "sql": f"{ANALYTICS_SQL_PATH}/stock_booking_information.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "venue_locations": {
        "sql": f"{ANALYTICS_SQL_PATH}/venue_locations.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        # see associated dependencies
    },
    "offer_item_ids": {
        "sql": f"{ANALYTICS_SQL_PATH}/offer_item_ids.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "offer_item_ids",
    },
    "offer_with_mediation": {
        "sql": f"{ANALYTICS_SQL_PATH}/offer_with_mediation.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "offer_with_mediation",
    },
    "recommendable_offers_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/recommendable_offers_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "recommendable_offers_data",
        "depends": [
            "enriched_venue_data",
            "enriched_offer_data",
            "offer_with_mediation",
        ],
    },
    "top_items_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/top_items_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "top_items_data",
        "depends": ["recommendable_offers_data", "iris_venues_at_radius"],
    },
    "top_items_in_iris_shape": {
        "sql": f"{ANALYTICS_SQL_PATH}/top_items_in_iris_shape.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "top_items_in_iris_shape",
        "depends": ["top_items_data", "iris_venues_in_shape"],
    },
    "top_items_not_geolocated": {
        "sql": f"{ANALYTICS_SQL_PATH}/top_items_not_geolocated.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "top_items_not_geolocated",
        "depends": ["top_items_data"],
    },
    "top_items_out_iris_shape": {
        "sql": f"{ANALYTICS_SQL_PATH}/top_items_out_iris_shape.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "top_items_out_iris_shape",
        "depends": ["top_items_in_iris_shape"],
    },
    "recommendable_offers_per_iris_shape": {
        "sql": f"{ANALYTICS_SQL_PATH}/recommendable_offers_per_iris_shape.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "recommendable_offers_per_iris_shape",
        "depends": [
            "top_items_in_iris_shape",
            "top_items_out_iris_shape",
            "top_items_not_geolocated",
        ],
    },
    "non_recommendable_offers_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/non_recommendable_offers_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "non_recommendable_offers_data",
    },
    "venue_siren_offers": {
        "sql": f"{ANALYTICS_SQL_PATH}/venue_siren_offers.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "venue_siren_offers",
        "depends": [
            "enriched_offer_data",
            "enriched_collective_offer_data",
            "enriched_booking_data",
            "enriched_collective_booking_data",
            "enriched_venue_data",
            "enriched_offerer_data",
        ],
        "clustering_fields": {"fields": ["offerer_siren", "venue_id"]},
    },
    "user_penetration": {
        "sql": f"{ANALYTICS_SQL_PATH}/user_penetration.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "user_penetration",
        "depends": [
            "enriched_user_data",
            "aggregated_monthly_user_used_booking_activity",
        ],
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
        "depends": ["enriched_booking_data", "enriched_offer_data"],
        "dag_depends": ["import_qpi_answers_v1"],
    },
    "diversification_raw_v2": {
        "sql": f"{ANALYTICS_SQL_PATH}/diversification_raw_v2.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "diversification_raw_v2",
        "depends": ["enriched_booking_data", "enriched_offer_data"],
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
            "enriched_user_data",
            "enriched_booking_data",
            "enriched_offer_data",
        ],
    },
    "diversification_booking_v2": {
        "sql": f"{ANALYTICS_SQL_PATH}/diversification_booking_v2.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "diversification_booking_v2",
        "depends": [
            "diversification_raw_v2",
            "enriched_user_data",
            "enriched_booking_data",
            "enriched_offer_data",
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
    "analytics_firebase_home_events_details": {
        "sql": f"{ANALYTICS_SQL_PATH}/firebase_home_events_details.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_home_events_details",
        "time_partitioning": {"field": "event_date"},
        "clustering_fields": {"fields": ["event_type"]},
        "depends": ["diversification_booking", "enriched_user_data"],
        "dag_depends": [
            "import_intraday_firebase_data",
            "import_contentful",
        ],  # computed once a day
    },
    "adage_involved_student": {
        "sql": f"{ANALYTICS_SQL_PATH}/adage_involved_student.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "adage_involved_student",
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
}

aggregated_tables = {
    "aggregated_daily_used_booking": {
        "sql": f"{ANALYTICS_SQL_PATH}/aggregated_daily_used_booking.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": ["enriched_booking_data"],
    },
    "aggregated_daily_user_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/aggregated_daily_user_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": ["enriched_deposit_data", "enriched_user_data"],
    },
    "aggregated_daily_user_used_activity": {
        "sql": f"{ANALYTICS_SQL_PATH}/aggregated_daily_user_used_activity.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": ["enriched_deposit_data", "enriched_booking_data"],
    },
    "aggregated_monthly_user_used_booking_activity": {
        "sql": f"{ANALYTICS_SQL_PATH}/aggregated_monthly_user_used_booking_activity.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": ["aggregated_daily_user_used_activity"],
    },
    "aggregated_user_stats_reco": {
        "sql": f"{ANALYTICS_SQL_PATH}/aggregated_user_stats_reco.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": ["enriched_user_data"],
    },
    "aggregated_daily_offer_consultation_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/aggregated_daily_offer_consultation_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "aggregated_daily_offer_consultation_data",
        "depends": ["enriched_user_data", "enriched_offer_data"],
        "dag_depends": ["import_intraday_firebase_data"],  # computed once a day
    },
}


export_tables = dict(analytics_tables, **aggregated_tables)
