with
    base as (
        select *
        from `{{ bigquery_raw_dataset }}`.`training_data_bookings`
        union all
        select *
        from `{{ bigquery_raw_dataset }}`.`training_data_clicks`
        union all
        select *
        from `{{ bigquery_raw_dataset }}`.`training_data_favorites`
        order by item_id
    )
select
    item_id,
    user_id,
    event_type,
    offer_subcategory_id,
    event_date,
    event_hour,
    event_day,
    event_month,
    count(*) as count
from base
group by
    item_id,
    user_id,
    event_type,
    offer_subcategory_id,
    event_date,
    event_hour,
    event_day,
    event_month
