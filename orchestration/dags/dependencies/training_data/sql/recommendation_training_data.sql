select *
from
    (
        select
            *,
            row_number() over (partition by user_id order by event_date) as row_index,
            count(*) over (partition by user_id) as total
        from `{{ bigquery_raw_dataset }}`.`training_data_{{ params.input_type }}`
        where
            event_date
            >= date_sub(date("{{ ds }}"), interval {{ params.event_day_number }} day)
    )
where row_index < {{ params.train_set_size }} * total or total = 1
