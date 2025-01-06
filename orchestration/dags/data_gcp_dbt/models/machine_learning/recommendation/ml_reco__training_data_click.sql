with
    events as (
        select
            native_event.user_id,
            native_event.offer_id,
            native_event.event_date,
            extract(hour from native_event.event_timestamp) as event_hour,
            extract(dayofweek from native_event.event_timestamp) as event_day,
            extract(month from native_event.event_timestamp) as event_month
        from {{ ref("int_firebase__native_event") }} as native_event
        where
            native_event.event_name = "ConsultOffer"
            and native_event.event_date
            >= date_sub(date("{{ ds() }}"), interval 6 month)
            and native_event.user_id is not null
            and native_event.offer_id is not null
            and native_event.offer_id != "NaN"
    ),

    click as (
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
            {{ ref("int_global__user") }} as users on events.user_id = users.user_id
    )

select distinct
    click.event_date,
    click.user_id,
    click.item_id,
    click.event_hour,
    click.event_day,
    click.event_month,
    user_feature.consult_offer,
    user_feature.booking_cnt,
    user_feature.user_theoretical_remaining_credit,
    user_feature.has_added_offer_to_favorites,
    user_feature.user_queries,
    user_feature.qpi_subcategory_ids,
    item_feature.offer_subcategory_id,
    item_feature.offer_category_id,
    item_feature.item_image_embedding,
    item_feature.item_semantic_content_hybrid_embedding,
    item_feature.item_names,
    item_feature.item_descriptions,
    item_feature.item_rayons,
    item_feature.item_author,
    item_feature.item_performer,
    item_feature.item_mean_stock_price,
    item_feature.item_booking_cnt,
    item_feature.item_favourite_cnt
from click
inner join
    {{ ref("ml_reco__training_data_user_feature") }} as user_feature
    on click.user_id = user_feature.user_id
inner join
    {{ ref("ml_reco__training_data_item_feature") }} as item_feature
    on click.item_id = item_feature.item_id
