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
        user_id
) 
select user_id,event_type,offer_subcategoryid,count(*) as count
from base 
group by user_id,event_type,offer_subcategoryid