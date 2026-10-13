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
            severity,
            message,
            exception_class,
            exception_message,
            user_id,
            booking_id,
            offer_id,
            stock_id,
            venue_id,
            provider_id,
            booking_quantity,
            trace,
            extra_json
        from {{ ref("int_pcapi__log") }}
        where
            (
                -- any booking action logged by the native API (book, cancel and
                -- future ones), successes as INFO and failures as WARNING/ERROR
                technical_message_id like 'native.bookings.%'
                -- successful cancellations from the app are only logged as
                -- booking.cancelled (native.bookings.cancel logs failures only)
                or (
                    technical_message_id = 'booking.cancelled'
                    and k8s_pod_role = 'native'
                    and reason = 'BookingCancellationReasons.BENEFICIARY'
                )
            )
            {% if is_incremental() %}
                and partition_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% else %} and partition_date >= '2026-07-02'  -- feature deployment date
            {% endif %}
    )

select
    partition_date,
    log_timestamp,
    technical_message_id,
    severity,
    message,
    exception_class,
    exception_message,
    user_id,
    booking_id,
    offer_id,
    stock_id,
    venue_id,
    provider_id,
    booking_quantity,
    trace,
    extra_json,
    case
        when technical_message_id = 'booking.cancelled'
        then 'cancel'
        else regexp_extract(technical_message_id, r'^native\.bookings\.(.+)$')
    end as booking_action,
    severity in ('DEFAULT', 'DEBUG', 'INFO', 'NOTICE') as is_success,
    regexp_extract(message, r'^Could not book offer: (.*)$') as failure_reason,
    case
        when severity in ('DEFAULT', 'DEBUG', 'INFO', 'NOTICE')
        then null
        -- expected refusals (business rules): the feature works as designed
        when
            exception_class in (
                'OfferIsAlreadyBooked',
                'OfferCategoryNotBookableByUser',
                'StockIsNotBookable',
                'UserHasInsufficientFunds',
                'DigitalExpenseLimitHasBeenReached',
                'ShowSoldOutException',
                'ShowRemovedException',
                'BookingIsAlreadyUsed',
                'CannotCancelConfirmedBooking'
            )
        then 'business'
        -- anything else (provider API errors, timeouts, unknown exceptions)
        else 'technical'
    end as failure_type
from source_logs
