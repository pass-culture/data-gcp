def preprocess_log_visit_query(gcp_project, bigquery_raw_dataset):
    return f"""
    SELECT
        idvisit,
        idsite,
        idvisitor,
        user_id,
        visit_first_action_time,
        visit_last_action_time,
        visit_goal_buyer,
        visit_goal_converted,
        visit_entry_idaction_name,
        visit_entry_idaction_url,
        visit_exit_idaction_name,
        visit_exit_idaction_url,
        visit_total_actions,
        visit_total_interactions,
        visit_total_searches,
        visit_total_events,
        visit_total_time,
        visitor_days_since_first,
        visitor_days_since_last,
        visitor_days_since_order,
        visitor_returning,
        visitor_count_visits,
        visitor_localtime,
        CASE
            WHEN REGEXP_CONTAINS(user_id, r"^ANONYMOUS ")
                THEN NULL
            WHEN REGEXP_CONTAINS(user_id, r"^[0-9]{{2,}} ")
                THEN REGEXP_EXTRACT(user_id, r"^[0-9]{{2,}}")
            WHEN REGEXP_CONTAINS(user_id, r"^[A-Z0-9]{{2,}} ")
                THEN `{gcp_project}`.{bigquery_raw_dataset}.dehumanize_id(REGEXP_EXTRACT(user_id, r"^[A-Z0-9]{{2,}}"))
            ELSE NULL
        END AS user_id_dehumanized
    FROM
        `{gcp_project}.{bigquery_raw_dataset}.log_visit`
"""


def preprocess_log_visit_referer_query(gcp_project, bigquery_raw_dataset):
    return f"""
    SELECT
        idvisit,
        referer_keyword as keyword,
        referer_name as name,
        referer_type as type,
        referer_url as url
    FROM
        {gcp_project}.{bigquery_raw_dataset}.log_visit
    """


def preprocess_log_visit_config_query(gcp_project, bigquery_raw_dataset):
    return f"""
    SELECT
        idvisit,
        config_id,
        config_browser_engine as browser_engine,
        config_browser_name as browser_name,
        config_browser_version as browser_version,
        config_device_brand as device_brand,
        config_device_model as device_model,
        config_device_type as device_type,
        config_os as os,
        config_os_version as os_version,
        config_resolution as resolution,
        config_cookie as cookie,
        config_director as director,
        config_flash as flash,
        config_gears as gears,
        config_java as java,
        config_pdf as pdf,
        config_quicktime as quicktime,
        config_realplayer as realplayer,
        config_silverlight as silverlight,
        config_windowsmedia as windowsmedia
    FROM
        {gcp_project}.{bigquery_raw_dataset}.log_visit
"""


def preprocess_log_visit_location_query(gcp_project, bigquery_raw_dataset, env):
    additional_column = ", location_provider as provider" if env != "dev" else ""

    return f"""
    SELECT
        idvisit,
        location_ip as ip,
        location_city as city,
        location_country as country,
        location_latitude as latitude,
        location_longitude as longitude,
        location_region as region,
        location_browser_lang as browser_lang{additional_column}
    FROM
        {gcp_project}.{bigquery_raw_dataset}.log_visit
    """


def preprocess_log_visit_campaign_query(gcp_project, bigquery_raw_dataset):
    return f"""
    SELECT
        idvisit,
        campaign_content as content,
        campaign_id as id,
        campaign_keyword as keyword,
        campaign_medium as medium,
        campaign_name as name,
        campaign_source as source
    FROM
        {gcp_project}.{bigquery_raw_dataset}.log_visit
    """


def preprocess_log_visit_custom_var_query(gcp_project, bigquery_raw_dataset):
    return f"""
    SELECT
        idvisit,
        custom_var_k1,
        custom_var_v1,
    FROM
        {gcp_project}.{bigquery_raw_dataset}.log_visit
    """


# Transform log_link_visit_action into matomo_events
def transform_matomo_events_query(gcp_project, bigquery_raw_dataset):
    return f"""
    WITH base AS (
        SELECT DISTINCT
            idlink_va,
            idvisit,
            server_time,
            la1.name as last_page_url,
            la4.name as url,
            la3.name AS event_params_raw, # infos Module name, offer id etc
            la5.name AS event_name,
            la6.name AS event_page # page sur laquelle on est
        FROM `{gcp_project}.{bigquery_raw_dataset}.log_link_visit_action`
        LEFT JOIN `{gcp_project}.{bigquery_raw_dataset}.log_action` la1
        ON idaction_url_ref = la1.idaction
        JOIN `{gcp_project}.{bigquery_raw_dataset}.log_action` la3
        ON idaction_name = la3.idaction
        JOIN `{gcp_project}.{bigquery_raw_dataset}.log_action` la4
        ON idaction_url = la4.idaction
        JOIN `{gcp_project}.{bigquery_raw_dataset}.log_action` la5
        ON idaction_event_action = la5.idaction
        JOIN `{gcp_project}.{bigquery_raw_dataset}.log_action` la6
        ON idaction_event_category = la6.idaction
        WHERE
            idaction_event_action IS NOT NULL
        order by server_time DESC
    ), params_raw AS (
        SELECT
            idlink_va,
            "params_raw" as key,
            event_params_raw AS value
        FROM base
    ), module_name AS (
        SELECT
            idlink_va,
            "module_name" as key,
            REGEXP_EXTRACT(event_params_raw, r"Module name: (.*) -") AS value
        FROM base
    ), number_tiles AS (
        SELECT
            idlink_va,
            "number_tiles" as key,
            REGEXP_EXTRACT(event_params_raw, r"Number of tiles: ([0-9]*)") AS value
        FROM base
    ), number_modules AS (
        SELECT
            idlink_va,
            "number_modules" as key,
            REGEXP_EXTRACT(event_params_raw, r"Number of modules: ([0-9]*)") AS value
        FROM base
    ), offer_id AS (
        SELECT
            idlink_va,
            "offer_id" as key,
            REGEXP_EXTRACT(event_params_raw, r"Offer id: ([A-Z0-9]*)") AS value
        FROM base
    ), object_id AS (
        SELECT
            idlink_va,
            "object_id" as key,
            REGEXP_EXTRACT(event_params_raw, r"^[A-Z0-9]*$") AS value
        FROM base
    ), unioned AS (
        SELECT * FROM params_raw WHERE params_raw.value is not null
        UNION ALL
        SELECT * FROM module_name WHERE module_name.value is not null
        UNION ALL
        SELECT * FROM number_tiles WHERE number_tiles.value is not null
        UNION ALL
        SELECT * FROM offer_id WHERE offer_id.value is not null
        UNION ALL
        SELECT * FROM number_modules WHERE number_modules.value is not null
        UNION ALL
        SELECT * FROM object_id WHERE object_id.value is not null
    ), aggregate AS (
        SELECT
            idlink_va,
            ARRAY_AGG(STRUCT(key, value)) as event_params
        FROM unioned
        GROUP BY idlink_va
    )
    SELECT
        base.idvisit,
        base.server_time as event_timestamp,
        base.event_name,
        base.url as page_url,
        base.last_page_url,
        aggregate.event_params
    FROM aggregate
    RIGHT OUTER JOIN base on base.idlink_va = aggregate.idlink_va
    """


# Create screen_view events from matomo log_link_visit_action
def add_screen_view_matomo_events_query(gcp_project, bigquery_raw_dataset):
    return f"""
    WITH pages AS (
        SELECT
            idaction_url,
            idaction_url_ref,
            idvisit,
            idaction_event_action,
            idaction_name,
            LAG(idaction_url) OVER (PARTITION BY idvisit ORDER BY server_time ASC) AS previous_idaction_url,
            server_time
        FROM `{gcp_project}.{bigquery_raw_dataset}.log_link_visit_action` llva
        JOIN `{gcp_project}.{bigquery_raw_dataset}.log_action` la
        ON llva.idaction_url = la.idaction
        WHERE la.type = 1
        ORDER by idvisit, server_time
    )
    SELECT
        idvisit,
        server_time AS  event_timestamp,
        'screen_view' AS event_name,
        idaction_url.name AS page_url,
        previous_idaction_url.name AS last_page_url,
        (SELECT [
            struct(SPLIT(idaction_url.name, '/')[OFFSET(1)] AS value, "page_path" AS key),
            struct(SPLIT(previous_idaction_url.name, '/')[OFFSET(1)] AS value, "last_page_path" AS key),
            struct(idaction_name.name AS value, 'page_name' AS key),
            struct(previous_idaction_name.name AS value, 'previous_page_name' AS key)
        ]) AS event_params
    FROM pages
    JOIN `{gcp_project}.{bigquery_raw_dataset}.log_action` idaction_url
    ON pages.idaction_url = idaction_url.idaction
    LEFT JOIN `{gcp_project}.{bigquery_raw_dataset}.log_action` previous_idaction_url
    ON pages.idaction_url_ref = previous_idaction_url.idaction
    LEFT JOIN `{gcp_project}.{bigquery_raw_dataset}.log_action` idaction_name
    ON pages.idaction_name = idaction_name.idaction
    LEFT JOIN `{gcp_project}.{bigquery_raw_dataset}.log_action` previous_idaction_name
    ON pages.idaction_name = previous_idaction_name.idaction
    WHERE idaction_url != previous_idaction_url OR previous_idaction_url IS NULL
    """
