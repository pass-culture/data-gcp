BACKEND_SQL_PATH = f"dependencies/backend/sql"

create_tables = {
    "favorites_not_booked": {
        "sql": f"{BACKEND_SQL_PATH}/favorites_not_booked.sql",
        "destination_dataset": "{{ bigquery_backend_dataset }}",
        "destination_table": "favorites_not_booked${{ yyyymmdd(today()) }}",
        "time_partitioning": {"field": "execution_date"},
        "schedule_type": "weekly",
        "write_disposition": "WRITE_APPEND",
        "dag_depends": ["import_analytics_v7/end_import"],  # dag_id/task_id
    },
    "marketing_pro_email_churned_40_days_ago": {
        "sql": f"{BACKEND_SQL_PATH}/marketing_pro_email_churned_40_days_ago.sql",
        "destination_dataset": "{{ bigquery_backend_dataset }}",
        "destination_table": "marketing_pro_email_churned_40_days_ago${{ yyyymmdd(today()) }}",
        "time_partitioning": {"field": "execution_date"},
        "schedule_type": "daily",
        "write_disposition": "WRITE_APPEND",
        "dag_depends": ["import_analytics_v7/end_import"],  # dag_id/task_id
    },
    "marketing_pro_email_last_booking_40_days_ago": {
        "sql": f"{BACKEND_SQL_PATH}/marketing_pro_email_last_booking_40_days_ago.sql",
        "destination_dataset": "{{ bigquery_backend_dataset }}",
        "destination_table": "marketing_pro_email_last_booking_40_days_ago${{ yyyymmdd(today()) }}",
        "time_partitioning": {"field": "execution_date"},
        "schedule_type": "daily",
        "write_disposition": "WRITE_APPEND",
        "dag_depends": ["import_analytics_v7/end_import"],  # dag_id/task_id
    },
    "stats_display_top_3_most_consulted_offers_last_30_days": {
        "sql": f"{BACKEND_SQL_PATH}/stats_display_top_3_most_consulted_offers_last_30_days.sql",
        "destination_dataset": "{{ bigquery_backend_dataset }}",
        "destination_table": "stats_display_top_3_most_consulted_offers_last_30_days${{ yyyymmdd(today()) }}",
        "time_partitioning": {"field": "execution_date"},
        "schedule_type": "daily",
        "write_disposition": "WRITE_TRUNCATE",
        "dag_depends": ["import_analytics_v7/end_import"],  # dag_id/task_id
    },
    "stats_display_cum_daily_consult_per_offerer_last_180_days": {
        "sql": f"{BACKEND_SQL_PATH}/stats_display_cum_daily_consult_per_offerer_last_180_days.sql",
        "destination_dataset": "{{ bigquery_backend_dataset }}",
        "destination_table": "stats_display_cum_daily_consult_per_offerer_last_180_days",
        "schedule_type": "daily",
        "write_disposition": "WRITE_TRUNCATE",
        "dag_depends": ["import_analytics_v7/end_import"],  # dag_id/task_id
    },
    "adage_home_playlist_local_offerers": {
        "sql": f"{BACKEND_SQL_PATH}/adage_home_playlist_local_offerers.sql",
        "destination_dataset": "{{ bigquery_backend_dataset }}",
        "destination_table": "adage_home_playlist_local_offerers",
        "schedule_type": "daily",
        "write_disposition": "WRITE_TRUNCATE",
        "dag_depends": ["import_analytics_v7/end_import"],  # dag_id/task_id
        "clustering_fields": {"fields": ["institution_id"]},
    },
    "adage_home_playlist_new_template_offers": {
        "sql": f"{BACKEND_SQL_PATH}/adage_home_playlist_new_template_offers.sql",
        "destination_dataset": "{{ bigquery_backend_dataset }}",
        "destination_table": "adage_home_playlist_new_template_offers",
        "schedule_type": "daily",
        "write_disposition": "WRITE_TRUNCATE",
        "dag_depends": ["import_analytics_v7/end_import"],  # dag_id/task_id
        "clustering_fields": {"fields": ["institution_id"]},
    },
    "adage_home_playlist_new_venues": {
        "sql": f"{BACKEND_SQL_PATH}/adage_home_playlist_new_venues.sql",
        "destination_dataset": "{{ bigquery_backend_dataset }}",
        "destination_table": "adage_home_playlist_new_venues",
        "schedule_type": "daily",
        "write_disposition": "WRITE_TRUNCATE",
        "dag_depends": ["import_analytics_v7/end_import"],  # dag_id/task_id
        "clustering_fields": {"fields": ["institution_id"]},
    },
    "adage_home_playlist_moving_offerers": {
        "sql": f"{BACKEND_SQL_PATH}/adage_home_playlist_moving_offerers.sql",
        "destination_dataset": "{{ bigquery_backend_dataset }}",
        "destination_table": "adage_home_playlist_moving_offerers",
        "schedule_type": "daily",
        "write_disposition": "WRITE_TRUNCATE",
        "dag_depends": ["import_analytics_v7/end_import"],  # dag_id/task_id
        "clustering_fields": {"fields": ["institution_id"]},
    },
    "booking_per_ean_last_30_days": {
        "sql": f"{BACKEND_SQL_PATH}/booking_per_ean_last_30_days.sql",
        "destination_dataset": "{{ bigquery_backend_dataset }}",
        "destination_table": "booking_per_ean_last_30_days",
        "schedule_type": "daily",
        "write_disposition": "WRITE_TRUNCATE",
        "dag_depends": ["import_analytics_v7/end_import"],  # dag_id/task_id
    },


}
