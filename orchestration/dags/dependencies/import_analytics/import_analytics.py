from common.config import ENV_SHORT_NAME

ANALYTICS_SQL_PATH = f"dependencies/import_analytics/sql/analytics"


def define_import_tables():
    return [
        "bank_information",
        "beneficiary_fraud_check",
        "booking",
        "cashflow",
        "collective_offer",
        "collective_offer_domain",
        "criterion",
        "deposit",
        "educational_deposit",
        "educational_domain",
        "educational_institution",
        "educational_year",
        "favorite",
        "offer",
        "offer_criterion",
        "offerer",
        "payment",
        "payment_status",
        "pricing",
        "pricing_line",
        "stock",
        "user",
        "user_offerer",
        "venue",
        "venue_provider",
        # add temporarly reimbursement tables
        "cashflow_batch",
        "cashflow_log",
        "cashflow_pricing",
        "invoice",
        "invoice_cashflow",
        "invoice_line",
        "venue_reimbursement_point_link",
    ]


analytics_tables = {
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
    },
    "enriched_offer_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_offer_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": [
            "isbn_rayon_editor",
        ],
    },
    "enriched_offer_metadata": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_offer_metadata.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "enriched_item_metadata": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_item_metadata.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": ["enriched_offer_metadata"],
    },
    "enriched_offerer_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_offerer_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": ["bookable_venue_history"],
    },
    "enriched_offerer_tags_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_offerer_tags_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "enriched_stock_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_stock_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": ["stock_booking_information"],
    },
    "enriched_suivi_dms_adage": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_suivi_dms_adage.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "dag_depends": [
            "import_adage_v1",
            "import_dms_subscriptions",
        ],
    },
    "enriched_user_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_user_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "enriched_user_offerer": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_user_offerer.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "enriched_venue_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_venue_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": ["bookable_venue_history"],
    },
    "enriched_venue_tags_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_venue_tags_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "enriched_reimbursement_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_reimbursement_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "enriched_venue_provider_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_venue_provider_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
    },
    "eple_aggregated": {
        "sql": f"{ANALYTICS_SQL_PATH}/eple_aggregated.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": [
            "enriched_institution_data",
            "enriched_collective_booking_data",
            "enriched_deposit_data",
            "enriched_booking_data",
        ],
    },
    "isbn_rayon_editor": {
        "sql": f"{ANALYTICS_SQL_PATH}/isbn_rayon_editor.sql",
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
    "offer_moderation": {
        "sql": f"{ANALYTICS_SQL_PATH}/offer_moderation.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "offer_moderation",
    },
    "offer_with_mediation": {
        "sql": f"{ANALYTICS_SQL_PATH}/offer_with_mediation.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "offer_with_mediation",
    },
    "forbidden_item_recommendation": {
        "sql": f"{ANALYTICS_SQL_PATH}/forbidden_item_recommendation.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "forbidden_item_recommendation",
        "depends": [
            "enriched_offer_data",
        ],
    },
    "recommendable_offers_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/recommendable_offers_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "recommendable_offers_data",
        "depends": [
            "enriched_venue_data",
            "enriched_offer_data",
            "offer_with_mediation",
            "enriched_item_metadata",
            "forbidden_item_recommendation",
        ],
    },
    "recommendable_offers_raw": {
        "sql": f"{ANALYTICS_SQL_PATH}/recommendable_offers_raw.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "recommendable_offers_raw",
        "depends": [
            "recommendable_offers_data",
        ],
    },
    "recommendable_items_raw": {
        "sql": f"{ANALYTICS_SQL_PATH}/recommendable_items_raw.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "recommendable_items_raw",
        "depends": [
            "recommendable_offers_raw",
        ],
    },
    "non_recommendable_offers_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/non_recommendable_offers_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "non_recommendable_offers_data",
    },
    "non_recommendable_items_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/non_recommendable_items_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "non_recommendable_items_data",
        "depends": [
            "enriched_booking_data",
        ],
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
    "user_penetration_cohorts": {
        "sql": f"{ANALYTICS_SQL_PATH}/user_penetration_cohorts.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "user_penetration_cohorts",
        "depends": [
            "user_penetration",
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
    "user_monthly_diversification_retention": {
        "sql": f"{ANALYTICS_SQL_PATH}/user_monthly_diversification_retention.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "user_monthly_diversification_retention",
        "depends": [
            "diversification_booking",
            "enriched_user_data",
        ],
    },
    "analytics_firebase_booking_origin": {
        "sql": f"{ANALYTICS_SQL_PATH}/firebase_booking_origin.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_booking_origin${{ yyyymmdd(add_days(ds, 0)) }}",
        "time_partitioning": {"field": "booking_date"},
        "dag_depends": ["import_intraday_firebase_data", "import_contentful"],
        "params": {"from": -8, "to": 0},
    },
    "analytics_firebase_booking_origin_catchup": {
        "sql": f"{ANALYTICS_SQL_PATH}/firebase_booking_origin.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_booking_origin${{ yyyymmdd(add_days(ds, -2)) }}",
        "time_partitioning": {"field": "booking_date"},
        "params": {"from": -10, "to": -2},
    },
    "analytics_firebase_similar_offer_events": {
        "sql": f"{ANALYTICS_SQL_PATH}/firebase_similar_offer_events.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_similar_offer_events",
        "time_partitioning": {"field": "event_date"},
        "dag_depends": ["import_intraday_firebase_data"],
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
    "analytics_firebase_home_macro_conversion": {
        "sql": f"{ANALYTICS_SQL_PATH}/firebase_home_macro_conversion.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_home_macro_conversion",
        "time_partitioning": {"field": "module_displayed_date"},
        "depends": ["diversification_booking"],
        "dag_depends": [
            "import_intraday_firebase_data",
            "import_contentful",
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
            "import_contentful",
        ],
    },
    "analytics_firebase_home_whole_path_conversion": {
        "sql": f"{ANALYTICS_SQL_PATH}/firebase_home_whole_path_conversion.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_home_whole_path_conversion",
        "time_partitioning": {"field": "module_displayed_date"},
        "depends": ["diversification_booking"],
        "dag_depends": [
            "import_intraday_firebase_data",
            "import_contentful",
        ],
    },
    "analytics_firebase_whole_home_conversion": {
        "sql": f"{ANALYTICS_SQL_PATH}/firebase_whole_home_conversion.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "firebase_whole_home_conversion",
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
            "enriched_user_data",
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
    "enriched_cultural_partner_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_cultural_partner_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": [
            "enriched_offerer_data",
            "enriched_venue_data",
            "enriched_collective_offer_data",
            "enriched_offer_data",
            "enriched_offerer_tags_data",
            "enriched_venue_tags_data",
        ],
    },
    "enriched_local_authority_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_local_authority_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": [
            "enriched_offerer_data",
            "enriched_venue_data",
            "bookable_venue_history",
        ],
    },
    "bookable_venue_history": {
        "sql": f"{ANALYTICS_SQL_PATH}/bookable_venue_history.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": [
            "enriched_offer_data",
            "enriched_collective_offer_data",
        ],
    },
    "bookable_partner_history": {
        "sql": f"{ANALYTICS_SQL_PATH}/bookable_partner_history.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": [
            "enriched_offer_data",
            "enriched_collective_offer_data",
            "enriched_venue_data",
            "enriched_offerer_data",
        ],
    },
    "cultural_sector_bookability_frequency": {
        "sql": f"{ANALYTICS_SQL_PATH}/cultural_sector_bookability_frequency.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": [
            "enriched_offer_data",
            "bookable_partner_history",
            "enriched_venue_data",
            "enriched_cultural_partner_data",
        ],
    },
    "retention_partner_history": {
        "sql": f"{ANALYTICS_SQL_PATH}/retention_partner_history.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": [
            "enriched_cultural_partner_data",
            "bookable_partner_history",
        ],
    },
    "funnel_subscription_beneficiary": {
        "sql": f"{ANALYTICS_SQL_PATH}/funnel_subscription_beneficiary.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "funnel_subscription_beneficiary",
        "depends": ["enriched_user_data"],
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
    "institution_locations": {
        "sql": f"{ANALYTICS_SQL_PATH}/institution_locations.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "institution_locations",
    },
    "user_iris_per_month": {
        "sql": f"{ANALYTICS_SQL_PATH}/user_iris_per_month.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "user_iris_per_month${{ yyyymmdd(current_month(ds)) }}",
        "time_partitioning": {"field": "month_log"},
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
        "dag_depends": [
            "import_intraday_firebase_data",
            "import_contentful",
        ],  # computed once a day
    },
    "aggregated_weekly_user_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/aggregated_weekly_user_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": [
            "enriched_deposit_data",
            "enriched_booking_data",
            "diversification_booking",
        ],
        "dag_depends": ["import_intraday_firebase_data"],
    },
    "partner_type_bookability_frequency": {
        "sql": f"{ANALYTICS_SQL_PATH}/partner_type_bookability_frequency.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": [
            "enriched_offer_data",
            "bookable_partner_history",
            "enriched_venue_data",
            "enriched_cultural_partner_data",
        ],
    },
    "enriched_partner_retention_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/enriched_partner_retention_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": [
            "enriched_cultural_partner_data",
            "partner_type_bookability_frequency",
            "enriched_offer_data",
            "enriched_booking_data",
            "enriched_collective_offer_data",
            "enriched_collective_booking_data",
            "enriched_venue_data",
            "enriched_offerer_data",
            "enriched_venue_provider_data",
        ],
    },
}


export_tables = dict(analytics_tables, **aggregated_tables)
