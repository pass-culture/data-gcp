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

with
    source_logs as (
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
            reason,
            cast(provider_id as int64) as provider_id,
            user_id,
            booking_id,
            offer_id,
            stock_id,
            venue_id,
            trace,
            extra_json,
            -- explicit success flag when logged (e.g. users.login), log level
            -- otherwise (failures are logged as WARNING or ERROR)
            coalesce(
                success, severity in ('DEFAULT', 'DEBUG', 'INFO', 'NOTICE')
            ) as is_success,
            coalesce(exception_class, exception_type) as exception_class
        from {{ ref("int_pcapi__log") }}
        where
            technical_origin is not null
            {% if is_incremental() %}
                and partition_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% else %} and partition_date >= '2026-09-17'  -- technical_origin deployment date
            {% endif %}
    )

select
    partition_date,
    log_timestamp,
    technical_message_id,
    origin,
    cron_name,
    severity,
    is_success,
    message,
    exception_class,
    exception_message,
    provider_id,
    user_id,
    booking_id,
    offer_id,
    stock_id,
    venue_id,
    trace,
    extra_json,
    -- successful cancellations from the app are only logged as booking.cancelled
    -- (native.bookings.cancel logs failures only): map them to the same action
    case
        when
            technical_message_id = 'booking.cancelled'
            and origin = 'native'
            and reason = 'BookingCancellationReasons.BENEFICIARY'
        then 'bookings'
        else feature
    end as feature,
    case
        when
            technical_message_id = 'booking.cancelled'
            and origin = 'native'
            and reason = 'BookingCancellationReasons.BENEFICIARY'
        then 'cancel'
        else action
    end as action
from source_logs
