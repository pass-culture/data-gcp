with
    user_with_one_booking as (
        select
            user_id,
            user_postal_code,
            first_booking_date as first_booking_date,
            current_date() as current_date
        from `{{ bigquery_analytics_dataset }}.global_user`
        where date(first_booking_date) = date_sub(current_date(), interval 2 day)
    )
select
    ebd.user_id,
    uob.user_postal_code,
    evd.venue_latitude,
    evd.venue_longitude,
    uob.first_booking_date,
    ebd.booking_created_at as booking_creation_date,
    uob.current_date,
    ebd.offer_id,
    eom.subcategory_id,
    eom.search_group_name
from `{{ bigquery_analytics_dataset }}.global_booking` ebd
join user_with_one_booking uob on ebd.user_id = uob.user_id
join
    `{{ bigquery_clean_dataset }}.int_applicative__offer_metadata` eom
    on eom.offer_id = ebd.offer_id
join `{{ bigquery_analytics_dataset }}.global_venue` evd on evd.venue_id = ebd.venue_id
where uob.first_booking_date = ebd.booking_creation_date
