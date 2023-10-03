with base as(
    select
        *
    from
        `{{ bigquery_raw_dataset }}`.`training_data_bookings`
    UNION
    ALL
    select
        *
    from
        `{{ bigquery_raw_dataset }}`.`training_data_clicks`
    UNION
    ALL
    select
        *
    from
        `{{ bigquery_raw_dataset }}`.`training_data_favorites`
    order by
        user_id
)
select
    user_id,
    item_id,
    event_type,
    offer_subcategoryid,
    event_date,
    event_hour,
    event_day,
    event_month,
    count(*) as count
from
    base
group by
    user_id,
    item_id,
    event_type,
    offer_subcategoryid,
    event_date,
    event_hour,
    event_day,
    event_month