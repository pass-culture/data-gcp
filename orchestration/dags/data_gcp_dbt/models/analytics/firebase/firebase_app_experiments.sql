SELECT 
        user_pseudo_id, 
        user_id, 
        experiment_name,
        experiment_value,
FROM
    {{ source('clean', 'firebase_app_experiments') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY user_pseudo_id,user_id,experiment_name ORDER BY event_date DESC) = 1
