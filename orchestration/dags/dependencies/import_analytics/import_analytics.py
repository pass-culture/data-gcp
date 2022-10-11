from common.config import (
    ENV_SHORT_NAME,
)

ANALYTICS_SQL_PATH = f"dependencies/import_analytics/sql/analytics"
CLEAN_SQL_PATH = f"dependencies/import_analytics/sql/clean"

clean_tables = {
    "clean_iris_venues": {
        "sql": f"{CLEAN_SQL_PATH}/iris_venues.sql",
        "destination_dataset": "{{ bigquery_clean_dataset }}",
        "destination_table": "iris_venues",
        "params": {"iris_distance": 50000 if ENV_SHORT_NAME != "dev" else 10000},
    }
}

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
    "iris_venues": {
        "sql": f"{ANALYTICS_SQL_PATH}/iris_venues.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "depends": ["clean_iris_venues"],
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
    "recommendable_offers_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/recommendable_offers_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "recommendable_offers_data",
        "depends": ["enriched_offer_data"],
    },
    "non_recommendable_offers_data": {
        "sql": f"{ANALYTICS_SQL_PATH}/non_recommendable_offers_data.sql",
        "destination_dataset": "{{ bigquery_analytics_dataset }}",
        "destination_table": "non_recommendable_offers_data",
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


export_tables = dict(clean_tables, **analytics_tables, **aggregated_tables)
