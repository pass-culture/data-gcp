-- noqa: disable=all
{{ config(**custom_table_config(materialized="view")) }}

with
    events as (
        select
            user_id,
            offer_id,
            event_date,
            extract(hour from event_timestamp) as event_hour,
            extract(dayofweek from event_timestamp) as event_day,
            extract(month from  event_timestamp) as event_month
        from {{ ref("int_firebase__native_event") }}
        where
            event_name = "HasAddedOfferToFavorites"
            and event_date >= date_sub(date("{{ ds() }}"), interval 6 month)
            and user_id is not null
            and offer_id is not null
            and offer_id != "NaN"
    )

select
    events.user_id,
    cast(user.user_age as int64) as user_age,
    "FAVORITE" as event_type,
    events.event_date,
    events.event_hour,
    events.event_day,
    events.event_month,
    offer.item_id,
    offer.offer_subcategory_id,
    offer.offer_category_id,
    offer.genres,
    offer.rayon,
    offer.type,
    offer.venue_id,
    offer.venue_name
from events
inner join {{ ref("mrt_global__offer") }} as offer on events.offer_id = offer.offer_id
inner join {{ ref("mrt_global__user_beneficiary") }} as user on events.user_id = user.user_id  -- noqa: RF04
