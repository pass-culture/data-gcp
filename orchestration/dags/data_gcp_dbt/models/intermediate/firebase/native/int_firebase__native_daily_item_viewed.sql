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
    source as (
        select
            event_date,
            origin,
            traffic_source,
            traffic_medium,
            traffic_campaign,
            module_id,
            entry_id as home_entry_id,
            search_id,
            item_type,
            unique_session_id,
            item_index_list,
            item_id,
            module_index,
            playlist_id,
            playlist_index,
            origin_id,
            reco_call_id,
            event_timestamp as item_displayed_timestamp,
            user_id
        from {{ ref("int_firebase__native_event") }} as native_event
        where
            1 = 1 and native_event.event_name = "ViewItem"
            {% if is_incremental() %}
                and date(native_event.event_date)
                >= date_sub("{{ ds() }}", interval 3 day)
            {% else %} and date(native_event.event_date) >= date("2025-06-02")
            {% endif %}
    ),

    -- Current tracking: one event per viewed item
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
            item_displayed_timestamp,
            coalesce(playlist_id, module_id) as module_id,
            coalesce(
                if(origin = "home", origin_id, null), home_entry_id
            ) as home_entry_id
        from source
        where item_id is not null
    ),

    -- The app sends ViewItem again when the user scrolls back to an item already
    -- seen: keep only the last display of the same item at the same place
    deduplicated_single_item_events as (
        select *
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
    ),

    -- Legacy tracking: one event per playlist with an "index:item_id" list
    legacy_split_items as (
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
            reco_call_id,
            safe_cast(module_index as int64) as playlist_index,
            case
                origin when "home" then home_entry_id when "search" then search_id
            end as origin_id,
            split(item_index_list, ",") as item_pairs
        from source
        where item_id is null and item_index_list is not null
    ),

    legacy_item_events as (
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
            cast(split(item_pair, ":")[offset(0)] as string) as item_index,
            cast(split(item_pair, ":")[offset(1)] as string) as viewed_item_id,
            playlist_index,
            origin_id,
            reco_call_id,
            cast(null as timestamp) as item_displayed_timestamp
        from legacy_split_items, unnest(item_pairs) as item_pair
    )

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
from deduplicated_single_item_events
union all
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
from legacy_item_events
