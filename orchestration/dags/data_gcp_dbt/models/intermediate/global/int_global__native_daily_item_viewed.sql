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
            ivr.traffic_source,
            ivr.traffic_medium,
            ivr.traffic_campaign,
            ivr.module_id,
            ivr.home_entry_id,
            home_tag.home_name,
            home_tag.home_type,
            home_tag.home_audience,
            home_tag.user_lifecycle_home,
            playlist_tag.bloc_name as playlist_name,
            playlist_tag.playlist_type,
            playlist_tag.offer_category as playlist_offer_category,
            playlist_tag.playlist_reach,
            playlist_tag.playlist_recurrence,
            ivr.item_type,
            ivr.unique_session_id,
            ivr.user_id,
            ivr.viewed_item_id,
            user.user_role,
            user.user_age,
            case
                when cast(ivr.item_index as int64) between 0 and 2
                then "bucket_1_3"
                when cast(ivr.item_index as int64) between 3 and 5
                then "bucket_4_6"
                when cast(ivr.item_index as int64) between 6 and 8
                then "bucket_7_9"
                else "bucket_10_plus"
            end as index_bucket
        from {{ ref("int_firebase__native_daily_item_viewed") }} as ivr
        left join
            {{ ref("int_applicative__user") }} as user on ivr.user_id = user.user_id
        left join
            {{ ref("int_contentful__home_tag") }} as home_tag
            on ivr.home_entry_id = home_tag.entry_id
        left join
            {{ ref("int_contentful__playlist_tag") }} as playlist_tag
            on ivr.module_id = playlist_tag.entry_id
        {% if is_incremental() %}
            where date(event_date) = date_sub('{{ ds() }}', interval 3 day)
        {% else %} where date(event_date) >= "2025-06-02"
        {% endif %}
    ),

    -- Agrège les sessions distinctes
    aggregated as (
        select
            event_date,
            origin,
            traffic_source,
            traffic_medium,
            traffic_campaign,
            module_id,
            home_entry_id,
            home_name,
            home_type,
            home_audience,
            user_lifecycle_home,
            playlist_name,
            playlist_type,
            playlist_offer_category,
            playlist_reach,
            playlist_recurrence,
            item_type,
            viewed_item_id,
            index_bucket,
            user_role,
            user_age,
            count(distinct unique_session_id) as total_sessions_item_viewed,
            count(distinct user_id) as total_user_item_viewed
        from bucketed
        group by
            event_date,
            origin,
            traffic_source,
            traffic_medium,
            traffic_campaign,
            module_id,
            home_entry_id,
            home_name,
            home_type,
            home_audience,
            user_lifecycle_home,
            playlist_name,
            playlist_type,
            playlist_offer_category,
            playlist_reach,
            playlist_recurrence,
            item_type,
            index_bucket,
            viewed_item_id,
            user_role,
            user_age
    )

-- Pivot des buckets en colonnes
select
    event_date,
    origin,
    traffic_source,
    traffic_medium,
    traffic_campaign,
    module_id,
    home_entry_id,
    home_name,
    home_type,
    home_audience,
    user_lifecycle_home,
    playlist_name,
    playlist_type,
    playlist_offer_category,
    playlist_reach,
    playlist_recurrence,
    item_type,  -- offer/venue/artist
    viewed_item_id,  -- offer_id/venue_id/artist_id
    user_role,
    user_age,
    coalesce(bucket_1_3, 0)
    + coalesce(bucket_4_6, 0)
    + coalesce(bucket_7_9, 0)
    + coalesce(bucket_10_plus, 0) as total_user_item_viewed,
    coalesce(bucket_1_3, 0) as total_user_item_viewed_bucket_1_3,
    coalesce(bucket_4_6, 0) as total_user_item_viewed_bucket_4_6,
    coalesce(bucket_7_9, 0) as total_user_item_viewed_bucket_7_9,
    coalesce(bucket_10_plus, 0) as total_user_item_viewed_bucket_10_plus
from
    aggregated pivot (
        sum(total_user_item_viewed) for index_bucket
        in ("bucket_1_3", "bucket_4_6", "bucket_7_9", "bucket_10_plus")
    )
