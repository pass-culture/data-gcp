from common.config import (
    ENV_SHORT_NAME,
)

ANALYTICS_SQL_PATH = f"dependencies/import_analytics/sql/analytics"


def define_import_tables():
    return [
        "user",
        "provider",
        "offerer",
        "offer",
        "product",
        "bank_information",
        "booking",
        "user_suspension",
        "individual_booking",
        "payment",
        "venue",
        "user_offerer",
        "offer_report",
        "stock",
        "favorite",
        "venue_type",
        "venue_label",
        "venue_contact",
        "payment_status",
        "cashflow",
        "cashflow_batch",
        "cashflow_log",
        "cashflow_pricing",
        "venue_pricing_point_link",
        "venue_reimbursement_point_link",
        "pricing",
        "pricing_line",
        "pricing_log",
        "business_unit",
        "transaction",
        "local_provider_event",
        "beneficiary_import_status",
        "deposit",
        "recredit",
        "beneficiary_import",
        "mediation",
        "offer_criterion",
        "allocine_pivot",
        "venue_provider",
        "allocine_venue_provider_price_rule",
        "allocine_venue_provider",
        "payment_message",
        "feature",
        "criterion",
        "beneficiary_fraud_review",
        "beneficiary_fraud_check",
        "educational_deposit",
        "educational_institution",
        "educational_redactor",
        "educational_year",
        "educational_domain",
        "educational_domain_venue",
        "collective_offer_domain",
        "collective_offer_template_domain",
        "venue_educational_status",
        "collective_booking",
        "collective_offer",
        "collective_offer_template",
        "collective_stock",
        "invoice",
        "offerer_tag",
        "offerer_tag_mapping",
        "action_history",
        "cds_cinema_details",
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
        "depends": [
            "enriched_offerer_data",
            "enriched_venue_data",
        ],  # add adage; dms_pro (?)
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
    },
    "diversification_raw": {
        "sql": f"{ANALYTICS_SQL_PATH}/diversification_raw.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "diversification_raw",
        "depends": [
            "enriched_booking_data",
            "enriched_offer_data",
        ],
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
}


export_tables = dict(analytics_tables, **aggregated_tables)
