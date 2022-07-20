SELECT
    *
FROM
    {% if params.dag_type == 'intraday' %}
    `{{ params.gcp_project_native_env }}.{{ params.firebase_raw_dataset }}.events{{ params.prefix }}{{ yyyymmdd(ds) }}`
    {% else %}
    `{{ params.gcp_project_native_env }}.{{ params.firebase_raw_dataset }}.events{{ params.prefix }}{{ yyyymmdd(add_days(ds, -1)) }}`
    {% endif %}
WHERE
    app_info.id IN (
        '{{ params.app_info_ids | join(',') }}'
    )
    OR app_info.id is NULL