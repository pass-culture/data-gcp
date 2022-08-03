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
    `{{ bigquery_raw_dataset }}`.`training_data_clics`
UNION
ALL
select
    *
from
    `{{ bigquery_raw_dataset }}`.`training_data_favorites`
order by
    offer_id
)
select
    offer_id,
    user_id,
    event_type,
    offer_subcategoryid,
    event_date,
    count(*) as count
from
    base
group by
    offer_id,
    user_id,
    event_type,
    offer_subcategoryid,
    event_date