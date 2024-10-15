with
    events as (
        select
            user_id,
            offer_id,
            event_date,
            extract(hour from event_timestamp) as event_hour,
            extract(dayofweek from event_timestamp) as event_day,
            extract(month from event_timestamp) as event_month
        from `{{ bigquery_int_firebase_dataset }}`.`native_event`
        where
            event_name = "ConsultOffer"
            and event_date >= date_sub(date("{{ ds }}"), interval 6 month)
            and event_date < date("{{ ds }}")
            and user_id is not null
            and offer_id is not null
            and offer_id != 'NaN'
    )
select
    events.user_id,
    coalesce(cast(enruser.user_age as int64), 0) as user_age,
    "CLICK" as event_type,
    event_date,
    event_hour,
    event_day,
    event_month,
    offer_item_id.item_id as item_id,
    offer.offer_subcategory_id as offer_subcategory_id,
    subcategories.category_id as offer_category_id,
    enroffer.genres,
    enroffer.rayon,
    enroffer.type,
    enroffer.venue_id,
    enroffer.venue_name
from events
join
    `{{ bigquery_clean_dataset }}`.`applicative_database_offer` offer
    on offer.offer_id = events.offer_id
inner join
    `{{ bigquery_raw_dataset }}`.`subcategories` subcategories
    on offer.offer_subcategory_id = subcategories.id
inner join
    `{{ bigquery_analytics_dataset }}`.`global_offer` enroffer
    on enroffer.offer_id = offer.offer_id
inner join
    `{{ bigquery_int_applicative_dataset }}`.`offer_item_id` offer_item_id
    on offer_item_id.offer_id = offer.offer_id
left join
    `{{ bigquery_analytics_dataset }}`.`global_user` enruser
    on enruser.user_id = events.user_id
