{{
  config(
    **custom_incremental_config(
    partition_by={
      "field": "first_event_date",
      "data_type": "date",
      "granularity": "day",
      "time_ingestion_partitioning": false
    },
    incremental_strategy = 'insert_overwrite'
  )
) }}

select distinct
    user_pseudo_id,
    session_id,
    unique_session_id,
    FIRST_VALUE(event_date) over (partition by unique_session_id order by event_timestamp rows between unbounded preceding and unbounded following) as first_event_date,
    LAST_VALUE(traffic_campaign) over (partition by unique_session_id order by event_timestamp rows between unbounded preceding and unbounded following) as traffic_campaign,
    LAST_VALUE(traffic_source) over (partition by unique_session_id order by event_timestamp rows between unbounded preceding and unbounded following) as traffic_source,
    LAST_VALUE(traffic_medium) over (partition by unique_session_id order by event_timestamp rows between unbounded preceding and unbounded following) as traffic_medium,
    LAST_VALUE(traffic_gen) over (partition by unique_session_id order by event_timestamp rows between unbounded preceding and unbounded following) as traffic_gen,
    LAST_VALUE(traffic_content) over (partition by unique_session_id order by event_timestamp rows between unbounded preceding and unbounded following) as traffic_content,
    LAST_VALUE(user_location_type ignore nulls) over (partition by unique_session_id order by event_timestamp rows between unbounded preceding and unbounded following) as last_user_location_type
from {{ ref('int_firebase__native_event') }} as firebase_events
where
    session_id is not NULL
    and event_name not in (
        'app_remove',
        'os_update',
        'batch_notification_open',
        'batch_notification_display',
        'batch_notification_dismiss',
        'app_update'
    )
    {% if is_incremental() %}
        and event_date between DATE_SUB(DATE('{{ ds() }}'), interval 3 day) and DATE('{{ ds() }}')
    {% endif %}
