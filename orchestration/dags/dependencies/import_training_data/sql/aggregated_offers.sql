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