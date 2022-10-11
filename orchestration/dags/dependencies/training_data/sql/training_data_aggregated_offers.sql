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
        item_id
)
select
    item_id,
    user_id,
    event_type,
    offer_subcategoryid,
    event_date,
    count(*) as count
from
    base
group by
    item_id,
    user_id,
    event_type,
    offer_subcategoryid,
    event_date