SELECT
    *
FROM
    {% if params.dag_type == 'intraday' %}
    `{{ params.gcp_project_native_env }}.{{ params.firebase_raw_dataset }}.events{{ params.prefix }}{{ yyyymmdd(ds) }}`
    {% else %}
    `{{ params.gcp_project_native_env }}.{{ params.firebase_raw_dataset }}.events{{ params.prefix }}{{ yyyymmdd(add_days(ds, -1)) }}`
    {% endif %}
