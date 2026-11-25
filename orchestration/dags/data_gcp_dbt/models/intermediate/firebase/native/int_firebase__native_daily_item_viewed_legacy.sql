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

-- Legacy tracking: one ViewItem event per playlist with an "index:item_id" list
with
    legacy_split_items as (
        select
            event_date,
            origin,
            traffic_source,
            traffic_medium,
            traffic_campaign,
            module_id,
            entry_id as home_entry_id,
            item_type,
            unique_session_id,
            user_id,
            reco_call_id,
            safe_cast(module_index as int64) as playlist_index,
            case
                origin when "home" then entry_id when "search" then search_id
            end as origin_id,
            split(item_index_list, ",") as item_pairs
        from {{ ref("int_firebase__native_event") }} as native_event
        where
            native_event.event_name = "ViewItem"
            and native_event.item_id is null
            and native_event.item_index_list is not null
            {% if is_incremental() %}
                and date(native_event.event_date)
                >= date_sub("{{ ds() }}", interval 3 day)
            {% else %} and date(native_event.event_date) >= date("2025-06-02")
            {% endif %}
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
    cast(split(item_pair, ":")[offset(0)] as string) as item_index,
    cast(split(item_pair, ":")[offset(1)] as string) as viewed_item_id,
    playlist_index,
    origin_id,
    reco_call_id,
    cast(null as timestamp) as item_displayed_timestamp
from legacy_split_items, unnest(item_pairs) as item_pair
