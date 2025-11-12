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
            module_id,
            entry_id as home_entry_id,
            item_type,
            unique_session_id,
            item_index_list,
            user_id
        from {{ ref("int_firebase__native_event") }} as native_event
        where
            1 = 1 and native_event.event_name = "ViewItem"
            {% if is_incremental() %}
                and date(native_event.event_date)
                >= date_sub('{{ ds() }}', interval 3 day)
            {% else %} and date(native_event.event_date) >= date("2025-06-02")
            {% endif %}
    ),

    -- sépare chaque pair index:item_id
    split_items as (
        select
            event_date,
            origin,
            module_id,
            home_entry_id,
            item_type,
            unique_session_id,
            user_id,
            split(item_index_list, ",") as item_pairs
        from source
    ),

    -- explose la liste
    unnested as (
        select
            event_date,
            origin,
            module_id,
            home_entry_id,
            item_type,
            unique_session_id,
            item_pair,
            user_id
        from split_items, unnest(item_pairs) as item_pair
    ),

select
    event_date,
    origin,
    module_id,
    home_entry_id,
    item_type,
    unique_session_id,
    user_id,
    cast(split(item_pair, ":")[offset(0)] as string) as item_index,
    cast(split(item_pair, ":")[offset(1)] as string) as viewed_item_id
from unnested
