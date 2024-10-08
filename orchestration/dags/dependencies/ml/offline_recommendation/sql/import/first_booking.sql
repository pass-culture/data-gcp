with
    user_with_one_booking as (
        select user_id, last_booking_date, first_booking_date, user_postal_code
        from `{{ bigquery_analytics_dataset }}.global_user`
        where
            last_booking_date = first_booking_date
            and last_booking_date >= date_sub(current_date(), interval 7 day)
    )
select
    ebd.user_id,
    uob.user_postal_code,
    evd.venue_latitude,
    evd.venue_longitude,
    ebd.offer_id,
    eom.subcategory_id,
    eom.search_group_name
from `{{ bigquery_analytics_dataset }}.global_booking` ebd
join user_with_one_booking uob on ebd.user_id = uob.user_id
join
    `{{ bigquery_clean_dataset }}.int_applicative__offer_metadata` eom
    on eom.offer_id = ebd.offer_id
join `{{ bigquery_analytics_dataset }}.global_venue` evd on evd.venue_id = ebd.venue_id
