{{
  config(
    materialized = "incremental",
    partition_by={
      "field": "first_event_date",
      "data_type": "date",
      "granularity": "day",
      "time_ingestion_partitioning": false
    },
    incremental_strategy = 'insert_overwrite'
  )
}}

SELECT DISTINCT
    user_pseudo_id
    , session_id
    , unique_session_id
    , FIRST_VALUE(event_date)OVER(PARTITION BY unique_session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_date
    , LAST_VALUE(traffic_campaign) OVER(PARTITION BY unique_session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS traffic_campaign
    , LAST_VALUE(traffic_source) OVER(PARTITION BY unique_session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS traffic_source
    , LAST_VALUE(traffic_medium) OVER(PARTITION BY unique_session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS traffic_medium
    , LAST_VALUE(traffic_gen) OVER(PARTITION BY unique_session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS traffic_gen
    , LAST_VALUE(traffic_content) OVER(PARTITION BY unique_session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS traffic_content
    , LAST_VALUE(user_location_type IGNORE NULLS) OVER(PARTITION BY unique_session_id ORDER BY event_timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_user_location_type
FROM {{ ref('int_firebase__native_event') }} AS firebase_events
WHERE  session_id IS NOT NULL
AND event_name NOT IN (
            'app_remove',
            'os_update',
            'batch_notification_open',
            'batch_notification_display',
            'batch_notification_dismiss',
            'app_update'
        )
{% if is_incremental() %}
    AND event_date BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 1 DAY) and DATE('{{ ds() }}')
{% endif %}