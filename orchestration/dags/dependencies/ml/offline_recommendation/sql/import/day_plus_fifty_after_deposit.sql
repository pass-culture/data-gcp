with 
    user_with_first_deposit as (
        select
            user_id,
            user_postal_code,
            first_deposit_creation_date,
            current_date() as current_date
        from `{{ bigquery_analytics_dataset }}.global_user`
        where date(first_deposit_creation_date) = date_sub(current_date(), interval 50 day)
    ),
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
            and event_date >= date_sub(date("{{ ds }}"), interval 50 day)
            and user_id is not null
            and offer_id is not null
            and offer_id != 'NaN'
    )
select
    e.user_id,
    uob.user_postal_code,
    e.event_date,
    e.event_hour,
    e.event_day,
    e.event_month,
    e.offer_id,
    e.item_id,
    eom.offer_subcategory_id,
    eom.search_group_name
from events e
join user_with_first_deposit uob on e.user_id = uob.user_id
join
    `{{ bigquery_int_applicative_dataset }}.offer_metadata` eom
    on eom.offer_id = e.offer_id
