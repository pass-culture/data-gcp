select
    event_timestamp,
    app_display_version,
    app_build_version,
    os_version,
    device_name,
    country,
    carrier,
    radio_type,
    custom_attributes,
    event_type,
    event_name,
    parent_trace_name,
    trace_info,
    network_info,
    timestamp_trunc(event_timestamp, day) as event_date
from `{{ params.gcp_project_env }}.app_passculture_{{ params.suffix }}`
{% if params.dag_type == "intraday" %}
where timestamp_trunc(event_timestamp, day) = timestamp("{{ ds }}")
{% else %}
where timestamp_trunc(event_timestamp, day) = timestamp("{{ add_days(ds, -1) }}")
{% endif %}
