{{
    config(
        **custom_incremental_config(
        incremental_strategy='insert_overwrite',
        partition_by={'field': 'partition_date', 'data_type': 'date'},
    )
) }}

select
    DATE(timestamp) as partition_date,
    timestamp,
    jsonpayload.extra.path as path,
    jsonpayload.extra.statuscode as status_code,
    jsonpayload.extra.method as method,
    jsonpayload.extra.sourceip as source_ip,
    jsonpayload.extra.duration as duration
from
    {{ source("raw","stdout") }}
where
    1 = 1
    {% if is_incremental() %}
        and DATE(timestamp) = "{{ ds() }}"
    {% endif %}
    and jsonpayload.extra.path like "/collective/%"
