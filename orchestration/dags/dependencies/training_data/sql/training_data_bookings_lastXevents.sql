with
    ranked_events as (
        with
            ranked_events as (
                select
                    user_id,
                    offer_id,
                    event_date,
                    extract(hour from event_timestamp) as event_hour,
                    extract(dayofweek from event_timestamp) as event_day,
                    extract(month from event_timestamp) as event_month,
                    row_number() over (
                        partition by user_id order by event_timestamp desc
                    ) as row_num
                from `{{ bigquery_int_firebase_dataset }}`.`native_event`
                where
                    event_name = "ConsultOffer"
                    and event_date >= date_sub(date("{{ ds }}"), interval 6 month)
                    and event_date < date("{{ ds }}")
                    and user_id is not null
                    and offer_id is not null
                    and offer_id != 'NaN'
            )
        select user_id, offer_id, event_date, event_hour, event_day, event_month
        from ranked_events
        where row_num <= x  -- Replace X with the number of last events you want per user
;
