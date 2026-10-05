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

with
    institutional_events as (
        select *
        from {{ source("raw", "firebase_institutional_events") }}
        {% if is_incremental() %}
            where
                event_date between date_sub(
                    date("{{ ds() }}"), interval {{ var("lookback_days", 3) }} day
                ) and date("{{ ds() }}")
        {% endif %}
    )

select
    event_date,
    event_name,
    user_pseudo_id,
    user_id,
    platform,
    timestamp_micros(event_timestamp) as event_timestamp,
    timestamp_micros(user_first_touch_timestamp) as user_first_touch_timestamp,
    device.category as device_category,
    device.operating_system as device_operating_system,
    device.operating_system_version as device_operating_system_version,
    device.web_info.browser as device_browser,
    device.web_info.browser_version as device_browser_version,
    device.web_info.hostname as device_hostname,
    geo.country as geo_country,
    geo.region as geo_region,
    geo.city as geo_city,
    traffic_source.name as user_traffic_campaign,
    traffic_source.medium as user_traffic_medium,
    traffic_source.source as user_traffic_source,
    {{
        extract_params_int_value(
            [
                "ga_session_id",
                "ga_session_number",
                "engagement_time_msec",
                "entrances",
                "percent_scrolled",
                "engaged_session_event",
            ]
        )
    }},
    {{
        extract_params_string_value(
            [
                "page_location",
                "page_title",
                "page_referrer",
                "origin",
                "source",
                "medium",
                "campaign",
                "term",
                "ignore_referrer",
                "link_url",
                "link_domain",
                "link_text",
                "link_classes",
                "outbound",
                "file_name",
                "file_extension",
                "privacy_consent_type",
                "privacy_consent_value",
            ]
        )
    }},
    -- session_engaged is mostly sent as a string, sometimes as an int
    (
        select
            coalesce(
                event_params.value.string_value,
                cast(event_params.value.int_value as string)
            ) as session_engaged
        from unnest(event_params) as event_params
        where event_params.key = 'session_engaged'
    ) as session_engaged
from institutional_events
