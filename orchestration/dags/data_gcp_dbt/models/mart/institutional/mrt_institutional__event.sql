{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="append_new_columns",
            require_partition_filter=true,
        )
    )
}}

select
    event_date,
    event_timestamp,
    user_pseudo_id,
    session_id,
    unique_session_id,
    session_number,
    event_name,
    page_title,
    engagement_time_msec,
    percent_scrolled,
    session_engaged,
    source,
    medium,
    campaign,
    user_traffic_source,
    user_traffic_medium,
    file_name,
    file_extension,
    link_text,
    link_url,
    link_domain,
    outbound,
    device_category,
    geo_country,
    geo_region
from {{ ref("int_firebase__institutional_event") }}
{% if is_incremental() %}
    where
        event_date between date_sub(
            date("{{ ds() }}"), interval {{ var("lookback_days", 3) }} day
        ) and date("{{ ds() }}")
{% endif %}
