{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "event_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="append_new_columns",
            cluster_by="event_name",
            require_partition_filter=true,
        )
    )
}}

select
    event_date,
    event_name,
    user_pseudo_id,
    user_id,
    platform,
    device_category,
    device_operating_system,
    device_operating_system_version,
    device_browser,
    device_browser_version,
    device_hostname,
    geo_country,
    geo_city,
    user_traffic_campaign,
    user_traffic_medium,
    user_traffic_source,
    event_timestamp,
    user_first_touch_timestamp,
    ga_session_id as session_id,
    ga_session_number as session_number,
    engagement_time_msec,
    entrances,
    percent_scrolled,
    engaged_session_event,
    page_location,
    page_title,
    page_referrer,
    origin,
    source,
    medium,
    campaign,
    term,
    ignore_referrer,
    link_url,
    link_domain,
    link_text,
    link_classes,
    outbound,
    file_name,
    file_extension,
    privacy_consent_type,
    privacy_consent_value,
    session_engaged,
    concat(user_pseudo_id, "-", ga_session_id) as unique_session_id,
    nullif(geo_region, "") as geo_region
from {{ ref("int_firebase__institutional_event_flattened") }}
where
    device_hostname = "pass.culture.fr"
    {% if is_incremental() %}
        and event_date between date_sub(
            date("{{ ds() }}"), interval {{ var("lookback_days", 3) }} day
        ) and date("{{ ds() }}")
    {% endif %}
