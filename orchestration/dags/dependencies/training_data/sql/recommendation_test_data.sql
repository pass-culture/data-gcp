select *
from `{{ bigquery_raw_dataset }}`.`training_data_{{ params.input_type }}`
where
    event_date >= date_sub(date("{{ ds }}"), interval {{ params.event_day_number }} day)
    and user_id in (
        select distinct user_id
        from `{{ bigquery_tmp_dataset }}`.`{{ ts_nodash }}_recommendation_training_data`
    )
    and item_id in (
        select distinct item_id
        from `{{ bigquery_tmp_dataset }}`.`{{ ts_nodash }}_recommendation_training_data`
    )
    and (user_id, item_id) not in (
        select (user_id, item_id)
        from `{{ bigquery_tmp_dataset }}`.`{{ ts_nodash }}_recommendation_training_data`
    )
    and mod(abs(farm_fingerprint(concat(user_id, item_id, date("{{ ds }}")))), 2) = 1
