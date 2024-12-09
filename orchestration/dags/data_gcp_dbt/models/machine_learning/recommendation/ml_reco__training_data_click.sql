with
    events as (
        select
            user_id,
            offer_id,
            event_date,
            extract(hour from event_timestamp) as event_hour,
            extract(dayofweek from event_timestamp) as event_day,
            extract(month from event_timestamp) as event_month
        from {{ ref("int_firebase__native_event") }}
        where
            event_name = "ConsultOffer"
            and event_date >= date_sub(date('{{ ds() }}'), interval 6 month)
            and user_id is not null
            and offer_id is not null
            and offer_id != 'NaN'
    ),
    click as (
        select
            event_date,
            events.user_id as user_id,
            offer.item_id as item_id,
            event_hour,
            event_day,
            event_month,
        from events
        join {{ ref("int_global__offer") }} offer on offer.offer_id = events.offer_id
        left join {{ ref("int_global__user") }} user on user.user_id = events.user_id
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
left join
    {{ ref("ml_reco__training_data_user_feature") }} as user_feature
    on user_feature.user_id = click.user_id
left join
    {{ ref("ml_reco__training_data_item_feature") }} as item_feature
    on item_feature.item_id = click.item_id
