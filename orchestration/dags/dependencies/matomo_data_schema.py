EXPORT_START_DATE = "2021-01-01 00:00:00.0"
# min_id and max_id are the minimum and maximal id of the table on the exported time period
# They are used to split the query in batch of <row_number_queried> rows.
PROD_TABLE_DATA = {
    "log_link_visit_action": {
        "id": "idlink_va",
        "min_id": 169419493,
        "max_id": 178167087,
        "columns": [
            {"name": "idlink_va", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idsite", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idvisitor", "type": "STRING", "mode": "REQUIRED"},  # BYTES
            {"name": "idvisit", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idaction_url_ref", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_name_ref", "type": "INT64", "mode": "NULLABLE"},
            {"name": "custom_float", "type": "STRING", "mode": "NULLABLE"},  # INT64
            {"name": "server_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "idpageview", "type": "STRING", "mode": "NULLABLE"},
            {"name": "interaction_position", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_name", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_url", "type": "INT64", "mode": "NULLABLE"},
            {"name": "time_spent_ref_action", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_event_action", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_event_category", "type": "INT64", "mode": "NULLABLE"},
            {
                "name": "idaction_content_interaction",
                "type": "INT64",
                "mode": "NULLABLE",
            },
            {"name": "idaction_content_name", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_content_piece", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_content_target", "type": "INT64", "mode": "NULLABLE"},
            {"name": "custom_var_k1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v5", "type": "STRING", "mode": "NULLABLE"},
        ],
        "row_number_queried": 300000,
        "query_filter": f"and server_time >= TIMESTAMP '{EXPORT_START_DATE}'",
    },
    "log_visit": {
        "id": "idvisit",
        "min_id": 8009709,
        "max_id": 8993487,
        "columns": [
            {"name": "idvisit", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idsite", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idvisitor", "type": "STRING", "mode": "REQUIRED"},  # BYTES
            {"name": "visit_last_action_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "config_id", "type": "STRING", "mode": "REQUIRED"},  # BYTES
            {"name": "location_ip", "type": "STRING", "mode": "REQUIRED"},  # BYTES
            {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
            {
                "name": "visit_first_action_time",
                "type": "TIMESTAMP",
                "mode": "REQUIRED",
            },
            {"name": "visit_goal_buyer", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_goal_converted", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_days_since_first", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_days_since_order", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_returning", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_count_visits", "type": "INT64", "mode": "REQUIRED"},
            {"name": "visit_entry_idaction_name", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_entry_idaction_url", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_exit_idaction_name", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_exit_idaction_url", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_total_actions", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_total_interactions", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_total_searches", "type": "INT64", "mode": "NULLABLE"},
            {"name": "referer_keyword", "type": "STRING", "mode": "NULLABLE"},
            {"name": "referer_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "referer_type", "type": "INT64", "mode": "NULLABLE"},
            {"name": "referer_url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_browser_lang", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_browser_engine", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_browser_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_browser_version", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_device_brand", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_device_model", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_device_type", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_os", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_os_version", "type": "STRING", "mode": "NULLABLE"},
            {"name": "visit_total_events", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_localtime", "type": "STRING", "mode": "NULLABLE"},  # TIME
            {"name": "visitor_days_since_last", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_resolution", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_cookie", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_director", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_flash", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_gears", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_java", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_pdf", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_quicktime", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_realplayer", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_silverlight", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_windowsmedia", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_total_time", "type": "INT64", "mode": "REQUIRED"},
            {"name": "location_city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_latitude", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "location_longitude", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "location_region", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_content", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_keyword", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_medium", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_provider", "type": "STRING", "mode": "NULLABLE"},
        ],
        "row_number_queried": 100000,
        "query_filter": f"and visit_first_action_time >= TIMESTAMP '{EXPORT_START_DATE}'",
    },
    "log_action": {
        "id": "idaction",
        "min_id": 1,
        "max_id": 7231362,
        "columns": [
            {"name": "idaction", "type": "INT64", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "hash", "type": "INT64", "mode": "REQUIRED"},
            {"name": "type", "type": "INT64", "mode": "NULLABLE"},
            {"name": "url_prefix", "type": "INT64", "mode": "NULLABLE"},
        ],
        "row_number_queried": 1000000,
        "query_filter": "",
    },
    "goal": {
        "id": "idgoal",
        "min_id": 1,
        "max_id": 50,
        "columns": [
            {"name": "idsite", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idgoal", "type": "INT64", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "description", "type": "STRING", "mode": "NULLABLE"},
            {"name": "match_attribute", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pattern", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pattern_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "case_sensitive", "type": "INT64", "mode": "NULLABLE"},
            {"name": "allow_multiple", "type": "INT64", "mode": "NULLABLE"},
            {"name": "revenue", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "deleted", "type": "INT64", "mode": "NULLABLE"},
            {"name": "event_value_as_revenue", "type": "INT64", "mode": "NULLABLE"},
        ],
        "row_number_queried": 50,
        "query_filter": "",
    },
    "log_conversion": {
        "id": "idvisit",
        "min_id": 8009709,
        "max_id": 8993485,
        "columns": [
            {"name": "idvisit", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idgoal", "type": "INT64", "mode": "REQUIRED"},
            {"name": "buster", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idsite", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idvisitor", "type": "STRING", "mode": "REQUIRED"},  # BYTES
            {"name": "server_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "idaction_url", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idlink_va", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idorder", "type": "INT64", "mode": "NULLABLE"},
            {"name": "items", "type": "INT64", "mode": "NULLABLE"},
            {"name": "url", "type": "STRING", "mode": "REQUIRED"},
            {"name": "visitor_days_since_first", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_count_visits", "type": "INT64", "mode": "REQUIRED"},
            {"name": "visitor_returning", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_days_since_order", "type": "INT64", "mode": "NULLABLE"},
            {"name": "referer_type", "type": "INT64", "mode": "NULLABLE"},
            {"name": "referer_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "referer_keyword", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_device_type", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_device_model", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_device_brand", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_region", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_longitude", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "location_latitude", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "location_country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "revenue_tax", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "revenue_subtotal", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "revenue_shipping", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "revenue_discount", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "revenue", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "custom_var_k1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_source", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_medium", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_keyword", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_content", "type": "STRING", "mode": "NULLABLE"},
        ],
        "row_number_queried": 100000,
        "query_filter": f"and server_time >= TIMESTAMP '{EXPORT_START_DATE}'",
    },
}

STAGING_TABLE_DATA = {
    "log_link_visit_action": {
        "id": "idlink_va",
        "min_id": 5958157,
        "max_id": 6127979,
        "columns": [
            {"name": "idlink_va", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idsite", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idvisitor", "type": "STRING", "mode": "NULLABLE"},  # BYTES
            {"name": "idvisit", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idaction_url_ref", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_name_ref", "type": "INT64", "mode": "NULLABLE"},
            {"name": "custom_float", "type": "STRING", "mode": "NULLABLE"},  # INT64
            {"name": "server_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "idpageview", "type": "STRING", "mode": "NULLABLE"},
            {"name": "interaction_position", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_name", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_url", "type": "INT64", "mode": "NULLABLE"},
            {"name": "time_spent_ref_action", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_event_action", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_event_category", "type": "INT64", "mode": "NULLABLE"},
            {
                "name": "idaction_content_interaction",
                "type": "INT64",
                "mode": "NULLABLE",
            },
            {"name": "idaction_content_name", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_content_piece", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idaction_content_target", "type": "INT64", "mode": "NULLABLE"},
            {"name": "custom_var_k1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v5", "type": "STRING", "mode": "NULLABLE"},
        ],
        "row_number_queried": 300000,
        "query_filter": f"and server_time >= TIMESTAMP '{EXPORT_START_DATE}'",
    },
    "log_visit": {
        "id": "idvisit",
        "min_id": 395630,
        "max_id": 418038,
        "columns": [
            {"name": "idvisit", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idsite", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idvisitor", "type": "STRING", "mode": "NULLABLE"},  # BYTES
            {"name": "visit_last_action_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
            {"name": "config_id", "type": "STRING", "mode": "NULLABLE"},  # BYTES
            {"name": "location_ip", "type": "STRING", "mode": "NULLABLE"},  # BYTES
            {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
            {
                "name": "visit_first_action_time",
                "type": "TIMESTAMP",
                "mode": "REQUIRED",
            },
            {"name": "visit_goal_buyer", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_goal_converted", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_days_since_first", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_days_since_order", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_returning", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_count_visits", "type": "INT64", "mode": "REQUIRED"},
            {"name": "visit_entry_idaction_name", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_entry_idaction_url", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_exit_idaction_name", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_exit_idaction_url", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_total_actions", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_total_interactions", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_total_searches", "type": "INT64", "mode": "NULLABLE"},
            {"name": "referer_keyword", "type": "STRING", "mode": "NULLABLE"},
            {"name": "referer_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "referer_type", "type": "INT64", "mode": "NULLABLE"},
            {"name": "referer_url", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_browser_lang", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_browser_engine", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_browser_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_browser_version", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_device_brand", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_device_model", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_device_type", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_os", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_os_version", "type": "STRING", "mode": "NULLABLE"},
            {"name": "visit_total_events", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_localtime", "type": "STRING", "mode": "NULLABLE"},  # TIME
            {"name": "visitor_days_since_last", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_resolution", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_cookie", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_director", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_flash", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_gears", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_java", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_pdf", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_quicktime", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_realplayer", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_silverlight", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_windowsmedia", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visit_total_time", "type": "INT64", "mode": "REQUIRED"},
            {"name": "location_city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_latitude", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "location_longitude", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "location_region", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_content", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_keyword", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_medium", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_source", "type": "STRING", "mode": "NULLABLE"},
        ],
        "row_number_queried": 100000,
        "query_filter": f"and visit_first_action_time >= TIMESTAMP '{EXPORT_START_DATE}'",
    },
    "log_action": {
        "id": "idaction",
        "min_id": 1,
        "max_id": 691393,
        "columns": [
            {"name": "idaction", "type": "INT64", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "hash", "type": "INT64", "mode": "REQUIRED"},
            {"name": "type", "type": "INT64", "mode": "NULLABLE"},
            {"name": "url_prefix", "type": "INT64", "mode": "NULLABLE"},
        ],
        "row_number_queried": 1000000,
        "query_filter": "",
    },
    "goal": {
        "id": "idgoal",
        "min_id": 1,
        "max_id": 5,
        "columns": [
            {"name": "idsite", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idgoal", "type": "INT64", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "description", "type": "STRING", "mode": "NULLABLE"},
            {"name": "match_attribute", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pattern", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pattern_type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "case_sensitive", "type": "INT64", "mode": "NULLABLE"},
            {"name": "allow_multiple", "type": "INT64", "mode": "NULLABLE"},
            {"name": "revenue", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "deleted", "type": "INT64", "mode": "NULLABLE"},
            {"name": "event_value_as_revenue", "type": "INT64", "mode": "NULLABLE"},
        ],
        "row_number_queried": 5,
        "query_filter": "",
    },
    "log_conversion": {
        "id": "idvisit",
        "min_id": 14475,
        "max_id": 399028,
        "columns": [
            {"name": "idvisit", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idsite", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idvisitor", "type": "STRING", "mode": "NULLABLE"},  # BYTES
            {"name": "server_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "idaction_url", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idlink_va", "type": "INT64", "mode": "NULLABLE"},
            {"name": "idgoal", "type": "INT64", "mode": "REQUIRED"},
            {"name": "buster", "type": "INT64", "mode": "REQUIRED"},
            {"name": "idorder", "type": "INT64", "mode": "NULLABLE"},
            {"name": "items", "type": "INT64", "mode": "NULLABLE"},
            {"name": "url", "type": "STRING", "mode": "REQUIRED"},
            {"name": "visitor_days_since_first", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_days_since_order", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_returning", "type": "INT64", "mode": "NULLABLE"},
            {"name": "visitor_count_visits", "type": "INT64", "mode": "REQUIRED"},
            {"name": "referer_keyword", "type": "STRING", "mode": "NULLABLE"},
            {"name": "referer_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "referer_type", "type": "INT64", "mode": "NULLABLE"},
            {"name": "config_device_brand", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_device_model", "type": "STRING", "mode": "NULLABLE"},
            {"name": "config_device_type", "type": "INT64", "mode": "NULLABLE"},
            {"name": "location_city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_country", "type": "STRING", "mode": "NULLABLE"},
            {"name": "location_latitude", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "location_longitude", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "location_region", "type": "STRING", "mode": "NULLABLE"},
            {"name": "revenue", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "revenue_discount", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "revenue_shipping", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "revenue_subtotal", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "revenue_tax", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "custom_var_k1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_k5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "custom_var_v5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_content", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_keyword", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_medium", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "campaign_source", "type": "STRING", "mode": "NULLABLE"},
        ],
        "row_number_queried": 100000,
        "query_filter": f"and server_time >= TIMESTAMP '{EXPORT_START_DATE}'",
    },
}
