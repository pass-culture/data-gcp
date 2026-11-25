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

-- Current tracking: one ViewItem event per viewed item
with
    single_item_events as (
        select
            event_date,
            origin,
            traffic_source,
            traffic_medium,
            traffic_campaign,
            item_type,
            unique_session_id,
            user_id,
            module_index as item_index,
            item_id as viewed_item_id,
            playlist_index,
            origin_id,
            reco_call_id,
            event_timestamp as item_displayed_timestamp,
            coalesce(playlist_id, module_id) as module_id,
            coalesce(if(origin = "home", origin_id, null), entry_id) as home_entry_id
        from {{ ref("int_firebase__native_event") }} as native_event
        where
            native_event.event_name = "ViewItem" and native_event.item_id is not null
            {% if is_incremental() %}
                and date(native_event.event_date)
                >= date_sub("{{ ds() }}", interval 3 day)
            {% else %} and date(native_event.event_date) >= date("2025-06-02")
            {% endif %}
    )

-- The app sends ViewItem again when the user scrolls back to an item already
-- seen: keep only the last display of the same item at the same place
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
    item_displayed_timestamp
from single_item_events
qualify
    row_number() over (
        partition by
            event_date,
            unique_session_id,
            origin,
            origin_id,
            module_id,
            playlist_index,
            item_index,
            item_type,
            viewed_item_id
        order by item_displayed_timestamp desc
    )
    = 1
