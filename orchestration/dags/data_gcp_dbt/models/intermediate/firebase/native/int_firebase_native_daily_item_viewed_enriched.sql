    -- Crée les buckets d’index
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

with  bucketed as (
        select
            event_date,
            origin,
            module_id,
            home_entry_id,
            item_type,
            unique_session_id,
            user_id,
            viewed_item_id,
            case
                when item_index between 1 and 3
                then "bucket_1_3"
                when item_index between 4 and 6
                then "bucket_4_6"
                when item_index between 7 and 9
                then "bucket_7_9"
                else "bucket_10_plus"
            end as index_bucket
        from ref("int_firebase__native_daily_item_viewed_raw")
    ),

    -- Agrège les sessions distinctes
    aggregated as (
        select
            event_date,
            origin,
            module_id,
            home_entry_id,
            item_type,
            viewed_item_id,
            index_bucket,
            count(distinct unique_session_id) as total_sessions_item_viewed,
            count(distinct user_id) as total_user_item_viewed
        from bucketed
        group by event_date,
            origin,
            module_id,
            home_entry_id,
            item_type,
            index_bucket,
            viewed_item_id
    )

-- Pivot des buckets en colonnes
select
    event_date,
    origin,
    module_id,
    home_entry_id,
    item_type, -- offer/venue/artist    
    item_id, -- displayed_item_id? Renommer ce champ pour distinguer avec l'item_id coté DS
    -- item_category ? -> A mettre dans un second temps (modèle mrt filtré sur item_type)
    -- partner_id -> Dans modèle mrt filtré sur item_type
    -- partner_geographic dimensions -> Dans modèle mrt filtré sur item_type
    total_user_item_viewed,
    -- user_role + autres dimensions à rajouter pour segmenter les users
    -- user_geographic dimensions
    -- user_is_rural
    coalesce(bucket_1_3, 0) as total_user_item_viewed_bucket_1_3,
    coalesce(bucket_4_6, 0) as sessions_bucket_4_6,
    coalesce(bucket_7_9, 0) as sessions_bucket_7_9,
    coalesce(bucket_10_plus, 0) as sessions_bucket_10_plus 
from
    aggregated pivot (
        sum(total_user_item_viewed) for index_bucket
        in ("bucket_1_3", "bucket_4_6", "bucket_7_9", "bucket_10_plus")
    )