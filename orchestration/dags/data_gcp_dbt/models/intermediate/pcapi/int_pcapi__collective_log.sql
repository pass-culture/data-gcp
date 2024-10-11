{{
    config(
        **custom_incremental_config(
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'partition_date', 'data_type': 'date'},
        on_schema_change = "sync_all_columns",
        require_partition_filter = true
    )
) }}

select
    partition_date,
    log_timestamp,
    url_path,
    status_code,
    method,
    source_ip,
    duration
from
    {{ ref("int_pcapi__log") }}
where
log_timestamp >= DATE_SUB(CURRENT_TIMESTAMP(), interval 365 day)
AND  url_path like "/collective/%"
{% if is_incremental() %}
and DATE(timestamp) = "{{ ds() }}"
{% endif %}
