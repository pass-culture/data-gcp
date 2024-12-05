{{ config(**custom_table_config(materialized="view")) }}

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
            event_name = "HasAddedOfferToFavorites"
            and event_date >= date_sub(date("{{ ds() }}"), interval 6 month)
            and user_id is not null
            and offer_id is not null
            and offer_id != 'NaN'
    )
select
    events.user_id,
    cast(user.user_age as int64) as user_age,
    "FAVORITE" as event_type,
    event_date,
    event_hour,
    event_day,
    event_month,
    offer.item_id as item_id,
    offer.offer_subcategory_id as offer_subcategory_id,
    offer.offer_category_id as offer_category_id,
    offer.genres,
    offer.rayon,
    offer.type,
    offer.venue_id,
    offer.venue_name,
from events
join {{ ref("int_global__offer") }} offer on offer.offer_id = events.offer_id
inner join {{ ref("int_global__user") }} user on user.user_id = events.user_id
