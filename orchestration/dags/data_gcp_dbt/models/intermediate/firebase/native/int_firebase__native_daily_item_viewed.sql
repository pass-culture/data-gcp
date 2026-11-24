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

{% set tracking_versions = {
    "v2": "int_firebase__native_daily_item_viewed_v2",
    "legacy": "int_firebase__native_daily_item_viewed_legacy",
} %}

{% for tracking_version, model_name in tracking_versions.items() %}
    select
        event_date,
        origin,
        traffic_source,
        traffic_medium,
        traffic_campaign,
        module_id,
        home_entry_id,
        item_type,
        unique_session_id,
        user_id,
        item_index,
        viewed_item_id,
        playlist_index,
        origin_id,
        reco_call_id,
        item_displayed_timestamp,
        "{{ tracking_version }}" as tracking_version
    from {{ ref(model_name) }}
    {% if is_incremental() %}
        where date(event_date) >= date_sub("{{ ds() }}", interval 3 day)
    {% else %} where date(event_date) >= date("2025-06-02")
    {% endif %}
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}
