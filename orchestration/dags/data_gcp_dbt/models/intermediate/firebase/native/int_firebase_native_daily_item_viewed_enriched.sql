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

with
    bucketed as (
        select
            ivr.event_date,
            ivr.origin,
            ivr.module_id,
            ivr.home_entry_id,
            ivr.item_type,
            ivr.unique_session_id,
            ivr.user_id,
            ivr.viewed_item_id,
            user.user_role,
            user.user_is_priority_public,
            user.user_age,
            case
                when ivr.item_index between 1 and 3
                then "bucket_1_3"
                when ivr.item_index between 4 and 6
                then "bucket_4_6"
                when ivr.item_index between 7 and 9
                then "bucket_7_9"
                else "bucket_10_plus"
            end as index_bucket
        from ref("int_firebase__native_daily_item_viewed_raw") as ivr
        left join ref("int_global__user") as user on ivr.user_id = user.user_id
        {% if is_incremental() %}
            where date(event_date)
            = date_sub('{{ ds() }}', interval 3 day)
        {% else %} where date(event_date) >= "2025-08-01"
        {% endif %}
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
            user_role,
            user_is_priority_public,
            user_age,
            count(distinct unique_session_id) as total_sessions_item_viewed,
            count(distinct user_id) as total_user_item_viewed
        from bucketed
        group by
            event_date,
            origin,
            module_id,
            home_entry_id,
            item_type,
            index_bucket,
            viewed_item_id,
            user_role,
            user_is_priority_public,
            user_age
    )

-- Pivot des buckets en colonnes
select
    event_date,
    origin,
    module_id,
    home_entry_id,
    item_type,  -- offer/venue/artist
    viewed_item_id, -- offer_id/venue_id/artist_id
    user_role,
    user_is_priority_public,
    user_age,
    total_user_item_viewed,
    coalesce(bucket_1_3, 0) as total_user_item_viewed_bucket_1_3,
    coalesce(bucket_4_6, 0) as total_user_item_viewed_bucket_4_6,
    coalesce(bucket_7_9, 0) as total_user_item_viewed_bucket_7_9,
    coalesce(bucket_10_plus, 0) as total_user_item_viewed_bucket_10_plus
from
    aggregated pivot (
        sum(total_user_item_viewed) for index_bucket
        in ("bucket_1_3", "bucket_4_6", "bucket_7_9", "bucket_10_plus")
    )
