{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "view_date", "data_type": "date"},
            on_schema_change="append_new_columns",
            require_partition_filter=true,
        )
    )
}}

with source as (
select
    event_date,
    origin,
    module_id,
    entry_id as home_entry_id,
    item_type,
    unique_session_id,
    item_index_list
from
    {{ ref("int_firebase__native_event") }} as native_event
    -- Jointure contentful pour récup les infos de la home/playlist
where
    1 = 1
    and native_event.event_name = "ViewItem"
    {% if is_incremental() %}
        and date(native_event.event_date) >= date_sub('{{ ds() }}', interval 3 day)
    {% else %}
        and date(native_event.event_date)
        >= date_sub('{{ ds() }}', interval {{ var("full_refresh_lookback") }})
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
        item_pair
    from split_items,
    unnest(item_pairs) as item_pair
),

-- Extraction des index et item_id
parsed as (
    select
        event_date,
        origin,
        module_id,
        home_entry_id,
        item_type,
        unique_session_id,
        cast(split(item_pair, ":")[offset(0)] as int64) as item_index,
        cast(split(item_pair, ":")[offset(1)] as int64) as item_id
    from unnested
),

-- Crée les buckets d’index
bucketed as (
    select
        event_date,
        origin,
        module_id,
        home_entry_id,
        item_type,
        unique_session_id,
        case
            when item_index between 1 and 3 then "bucket_1_3"
            when item_index between 4 and 6 then "bucket_4_6"
            when item_index between 7 and 9 then "bucket_7_9"
            else "bucket_10_plus"
        end as index_bucket
    from parsed
),

-- Agrège les sessions distinctes
aggregated as (
    select
        event_date,
        origin,
        module_id,
        home_entry_id,
        item_type,
        item_bucket,
        count(distinct unique_session_id) as unique_sessions_count
    from bucketed
    group by 1, 2, 3
)

-- Pivot des buckets en colonnes
select
    event_date,
    origin,
    module_id,
    home_entry_id,
    item_type,
    coalesce(bucket_1_3, 0) as sessions_bucket_1_3,
    coalesce(bucket_4_6, 0) as sessions_bucket_4_6,
    coalesce(bucket_7_9, 0) as sessions_bucket_7_9,
    coalesce(bucket_10_plus, 0) as sessions_bucket_10_plus
from aggregated
pivot (
    sum(unique_sessions_count) for index_bucket in ("bucket_1_3", "bucket_4_6", "bucket_7_9", "bucket_10_plus")
)
