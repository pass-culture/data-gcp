{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

-- One row per action outcome, not per log line: a booking cancellation logs
-- several lines for the same booking (a start marker, the core outcome, the
-- route outcome), so counting lines inflates successes and failures unevenly.
-- Lines of the same request (trace) and booking are one event; a line without
-- booking_id joins its request's booking when that request has exactly one.
-- Lines with no booking to attach to stay one event each (bulk actions log one
-- line per stock / offer).
with
    feature_log as (
        select
            partition_date,
            origin,
            feature,
            action,
            severity,
            exception_class,
            provider_id,
            booking_id,
            trace,
            is_success
        from {{ ref("int_pcapi__feature_log") }}
        where
            {% if is_incremental() %}
                partition_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% else %} partition_date >= '2026-09-17'
            {% endif %}
    ),

    single_booking_trace as (
        select origin, feature, action, trace, any_value(booking_id) as trace_booking_id
        from feature_log
        where trace is not null and booking_id is not null
        group by origin, feature, action, trace
        having count(distinct booking_id) = 1
    ),

    keyed_log as (
        select
            feature_log.*,
            case
                when
                    feature_log.trace is not null
                    and coalesce(
                        feature_log.booking_id, single_booking_trace.trace_booking_id
                    )
                    is not null
                then
                    concat(
                        feature_log.trace,
                        "|",
                        coalesce(
                            feature_log.booking_id,
                            single_booking_trace.trace_booking_id
                        )
                    )
                else generate_uuid()
            end as event_key
        from feature_log
        left join
            single_booking_trace
            on feature_log.origin = single_booking_trace.origin
            and feature_log.feature = single_booking_trace.feature
            and feature_log.action = single_booking_trace.action
            and feature_log.trace = single_booking_trace.trace
    ),

    action_event as (
        select
            origin,
            feature,
            action,
            min(partition_date) as partition_date,
            -- an event fails when any of its lines fails; its failure lines
            -- carry the severity and exception
            logical_and(is_success) as is_success,
            coalesce(
                max(if(not is_success, severity, null)), max(severity)
            ) as severity,
            max(if(not is_success, exception_class, null)) as exception_class,
            max(provider_id) as provider_id,
            count(*) as log_line_count
        from keyed_log
        group by event_key, origin, feature, action
    )

select
    partition_date,
    origin,
    feature,
    action,
    severity,
    exception_class,
    provider_id,
    count(*) as total_count,
    countif(is_success) as success_count,
    countif(not is_success) as error_count,
    sum(log_line_count) as log_line_count
from action_event
group by partition_date, origin, feature, action, severity, exception_class, provider_id
