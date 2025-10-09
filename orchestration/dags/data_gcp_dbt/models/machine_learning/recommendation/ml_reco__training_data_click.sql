with
    events as (
        select
            native_events.user_id,
            native_events.offer_id,
            native_events.event_date,
            extract(hour from native_events.event_timestamp) as event_hour,
            extract(dayofweek from native_events.event_timestamp) as event_day,
            extract(month from native_events.event_timestamp) as event_month
        from {{ ref("int_firebase__native_event") }} as native_events
        where
            native_events.event_name = "ConsultOffer"
            and native_events.event_date
            >= date_sub(date("{{ ds() }}"), interval 6 month)
            and native_events.user_id is not null
            and native_events.offer_id is not null
            and native_events.offer_id != "NaN"
    ),

    clicks as (
        select
            events.event_date,
            events.user_id,
            offers.item_id,
            events.event_hour,
            events.event_day,
            events.event_month
        from events
        inner join
            {{ ref("int_global__offer") }} as offers
            on events.offer_id = offers.offer_id
        left join
            {{ ref("int_global__user_beneficiary") }} as users
            on events.user_id = users.user_id
    )

select distinct
    clicks.event_date,
    clicks.user_id,
    clicks.item_id,
    clicks.event_hour,
    clicks.event_day,
    clicks.event_month,
    user_features.consult_offer,
    user_features.booking_cnt,
    user_features.user_theoretical_remaining_credit,
    user_features.has_added_offer_to_favorites,
    user_features.user_queries,
    user_features.qpi_subcategory_ids,
    item_features.offer_subcategory_id,
    item_features.offer_category_id,
    item_features.item_image_embedding,
    item_features.item_semantic_content_hybrid_embedding,
    item_features.item_names,
    item_features.item_descriptions,
    item_features.item_rayons,
    item_features.item_author,
    item_features.item_performer,
    item_features.item_mean_stock_price,
    item_features.item_booking_cnt,
    item_features.item_favourite_cnt
from clicks
inner join  -- Could be a left join if ml_reco__training_data_user_feature would not remove users
    {{ ref("ml_reco__training_data_user_feature") }} as user_features
    on clicks.user_id = user_features.user_id
inner join  -- Could be a left join if ml_reco__training_data_item_feature would not remove users
    {{ ref("ml_reco__training_data_item_feature") }} as item_features
    on clicks.item_id = item_features.item_id
