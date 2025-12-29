{{
    config(
        **custom_incremental_config(
            partition_by={
                "field": "first_event_date",
                "data_type": "date",
                "granularity": "day",
                "time_ingestion_partitioning": false,
            },
            incremental_strategy="insert_overwrite",
        )
    )
}}

select distinct
    user_pseudo_id,
    session_id,
    unique_session_id,
    first_value(event_date) over (
        partition by unique_session_id
        order by event_timestamp
        rows between unbounded preceding and unbounded following
    ) as first_event_date,
    last_value(traffic_campaign) over (
        partition by unique_session_id
        order by event_timestamp
        rows between unbounded preceding and unbounded following
    ) as traffic_campaign,
    last_value(traffic_source) over (
        partition by unique_session_id
        order by event_timestamp
        rows between unbounded preceding and unbounded following
    ) as traffic_source,
    last_value(traffic_medium) over (
        partition by unique_session_id
        order by event_timestamp
        rows between unbounded preceding and unbounded following
    ) as traffic_medium,
    last_value(traffic_gen) over (
        partition by unique_session_id
        order by event_timestamp
        rows between unbounded preceding and unbounded following
    ) as traffic_gen,
    last_value(traffic_content) over (
        partition by unique_session_id
        order by event_timestamp
        rows between unbounded preceding and unbounded following
    ) as traffic_content,
    last_value(user_location_type ignore nulls) over (
        partition by unique_session_id
        order by event_timestamp
        rows between unbounded preceding and unbounded following
    ) as last_user_location_type
from {{ ref("int_firebase__native_event") }}
where
    session_id is not null
    and event_name not in (
        'app_remove',
        'os_update',
        'batch_notification_open',
        'batch_notification_display',
        'batch_notification_dismiss',
        'app_update'
    )
    {% if is_incremental() %}
        and event_date
        between date_sub(date('{{ ds() }}'), interval 3 day) and date('{{ ds() }}')
    {% endif %}
