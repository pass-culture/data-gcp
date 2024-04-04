SELECT 
DISTINCT 
        event_date,
        user_pseudo_id, 
        user_id, 
        user_properties.key as experiment_name,
        user_properties.value.string_value as experiment_value
FROM
    `{{ bigquery_raw_dataset }}.firebase_events`
, 
UNNEST(user_properties) AS user_properties
where user_properties.key like "%firebase_exp%"
AND 
    {% if params.dag_type == 'intraday' %}
    event_date = DATE('{{ ds }}')
    {% else %}
    event_date = DATE('{{ add_days(ds, -1) }}')
    {% endif %}