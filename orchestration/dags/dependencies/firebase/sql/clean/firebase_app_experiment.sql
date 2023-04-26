SELECT 
DISTINCT 
        event_date, 
        user_pseudo_id, 
        user_id, 
        user_properties.key as experiment_name,
        user_properties.value.string_value as experiment_value
FROM
    {% if params.dag_type == 'intraday' %}
    `{{ bigquery_raw_dataset }}.{{ params.table_name }}_{{ yyyymmdd(ds) }}`
    {% else %}
    `{{ bigquery_raw_dataset }}.{{ params.table_name }}_{{ yyyymmdd(add_days(ds, -1)) }}`
    {% endif %}
, 
UNNEST(user_properties) AS user_properties
where user_properties.key like "%firebase_exp%" 