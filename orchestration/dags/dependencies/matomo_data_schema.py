TABLE_DATA = {
    "log_link_visit_action": {
        "id": "idlink_va",
        "conversions": """
            CAST(idvisit AS INT64) as idvisit,
            CAST(idlink_va AS INT64) as idlink_va,
            CAST(server_time AS TIMESTAMP) AS server_time,
            TO_BASE64(idvisitor) as idvisitor,
            CAST(custom_float AS STRING) AS custom_float,
            CAST(bandwidth AS INT64) as bandwidth,
            * except(idvisit, idlink_va, server_time, idvisitor, custom_float, bandwidth)
        """,
    },
    "log_visit": {
        "id": "idvisit",
        "time_column": "visit_last_action_time",
        "conversions": """
            CAST(idvisit AS INT64) as idvisit,
            TO_BASE64(idvisitor) as idvisitor,
            CAST(visit_last_action_time AS TIMESTAMP) AS visit_last_action_time,
            TO_BASE64(config_id) as config_id,
            TO_BASE64(location_ip) as location_ip,
            CAST(visit_first_action_time AS TIMESTAMP) AS visit_first_action_time,
            CAST(visitor_localtime AS STRING) AS visitor_localtime,
            CAST(location_latitude AS FLOAT64) AS location_latitude,
            CAST(location_longitude AS FLOAT64) AS location_longitude,
            CAST(last_idlink_va AS INT64) as last_idlink_va,
            * EXCEPT(idvisit, idvisitor, visit_last_action_time, config_id, location_ip, visit_first_action_time, visitor_localtime, location_latitude, location_longitude, last_idlink_va),
        """,
    },
    "log_action": {
        "id": "idaction",
        "conversions": "`hash` AS _hash, * EXCEPT(`hash`)",
    },
    "goal": {
        "id": "idgoal",
        "conversions": "*",
    },
    "log_conversion": {
        "id": "idvisit",
        "time_column": "server_time",
        "conversions": """
            CAST(idvisit AS INT64) as idvisit,
            CAST(idlink_va AS INT64) as idlink_va,
            CAST(server_time AS TIMESTAMP) AS server_time,
            TO_BASE64(idvisitor) as idvisitor,
            CAST(idorder AS INT64) as idorder,
            CAST(location_latitude AS FLOAT64) AS location_latitude,
            CAST(location_longitude AS FLOAT64) AS location_longitude,
            * except(idvisit, idlink_va, server_time, idvisitor, idorder, location_latitude, location_longitude)
        """,
    },
}
