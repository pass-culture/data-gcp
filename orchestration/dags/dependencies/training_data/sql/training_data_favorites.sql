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
            event_name = "HasAddedOfferToFavorites"
            and event_date >= date_sub(date("{{ ds }}"), interval 6 month)
            and user_id is not null
            and offer_id is not null
            and offer_id != 'NaN'
    )
select
    events.user_id,
    cast(enruser.user_age as int64) as user_age,
    "FAVORITE" as event_type,
    event_date,
    event_hour,
    event_day,
    event_month,
    cast(unix_seconds(timestamp(events.event_date)) as int64) as timestamp,
    enroffer.item_id as item_id,
    enroffer.offer_subcategory_id as offer_subcategory_id,
    enroffer.offer_category_id as offer_category_id,
    enroffer.genres,
    enroffer.rayon,
    enroffer.type,
    enroffer.venue_id,
    enroffer.venue_name,
from events
join
    `{{ bigquery_analytics_dataset }}`.`global_offer` enroffer
    on enroffer.offer_id = events.offer_id
inner join
    `{{ bigquery_analytics_dataset }}`.`global_user` enruser
    on enruser.user_id = events.user_id
