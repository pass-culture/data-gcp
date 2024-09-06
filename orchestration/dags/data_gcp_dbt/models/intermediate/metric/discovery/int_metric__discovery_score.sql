SELECT consultation_id,
    consulted_entity,
    type
FROM {{ ref('int_metric__discovery_daily_consultation') }}
QUALIFY ROW_NUMBER() OVER(PARTITION BY user_id, consulted_entity, type ORDER BY consultation_timestamp ASC) = 1
