{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_date", "data_type": "date"},
            on_schema_change="append_new_columns",
            require_partition_filter=true,
        )
    )
}}

select
    partition_date,
    log_timestamp,
    technical_message_id,
    technical_origin as origin,
    technical_feature as feature,
    technical_action as action,
    technical_cron as cron_name,
    severity,
    message,
    exception_message,
    user_id,
    booking_id,
    offer_id,
    stock_id,
    venue_id,
    trace,
    extra_json,
    cast(provider_id as int64) as provider_id,
    -- explicit success flag when logged (e.g. users.login), log level
    -- otherwise (failures are logged as WARNING or ERROR)
    coalesce(success, severity in ('DEFAULT', 'DEBUG', 'INFO', 'NOTICE')) as is_success,
    coalesce(exception_class, exception_type) as exception_class
from {{ ref("int_pcapi__log") }}
where
    technical_origin is not null
    {% if is_incremental() %}
        and partition_date
        between date_sub(date("{{ ds() }}"), interval 1 day) and date("{{ ds() }}")
    {% else %} and partition_date >= '2026-09-17'  -- technical_origin deployment date
    {% endif %}
