SELECT DISTINCT
        event_date,
        user_pseudo_id,
        user_id,
        user_properties.key as experiment_name,
        user_properties.value.string_value as experiment_value
FROM {{ source('raw','firebase_events') }},
UNNEST(user_properties) AS user_properties
where user_properties.key like "%firebase_exp%"
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id,user_id,experiment_name ORDER BY event_date DESC) = 1
