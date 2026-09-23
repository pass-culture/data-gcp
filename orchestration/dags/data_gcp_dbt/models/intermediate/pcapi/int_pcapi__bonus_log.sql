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
        select partition_date, log_started_at, log_ended_at, counters_json
        from {{ ref("int_pcapi__log") }}
        where
            technical_message_id = 'bonus_credit.statistics.counters'
            {% if is_incremental() %}
                and partition_date
                between date_sub(date("{{ ds() }}"), interval 1 day) and date(
                    "{{ ds() }}"
                )
            {% else %} and partition_date >= '2026-09-01'  -- feature deployment date
            {% endif %}
    ),

    parsed_payloads as (
        select
            partition_date,
            log_started_at,
            log_ended_at,
            safe.parse_json(counters_json) as counters_json_obj
        from source_logs
    ),

    -- 1. Extraction des succès (grants)
    unnested_grants as (
        select
            payload.partition_date,
            payload.log_started_at,
            payload.log_ended_at,
            bonus_key as bonus_type,
            'grant' as status,
            cast(null as string) as error_reason,
            cast(null as int64) as attempts_count,
            cast(
                json_value(payload.counters_json_obj.grants[bonus_key]) as int64
            ) as total_requests
        from parsed_payloads as payload
        cross join unnest(json_keys(payload.counters_json_obj.grants, 1)) as bonus_key
        where payload.counters_json_obj.grants is not null
    ),

    -- 2. Extraction des erreurs (errors)
    unnested_errors as (
        select
            payload.partition_date,
            payload.log_started_at,
            payload.log_ended_at,
            bonus_key as bonus_type,
            'error' as status,
            error_key as error_reason,
            cast(null as int64) as attempts_count,
            cast(
                json_value(
                    payload.counters_json_obj.errors[bonus_key][error_key]
                ) as int64
            ) as total_requests
        from parsed_payloads as payload
        cross join unnest(json_keys(payload.counters_json_obj.errors, 1)) as bonus_key
        cross join
            unnest(
                json_keys(payload.counters_json_obj.errors[bonus_key], 1)
            ) as error_key
        where payload.counters_json_obj.errors is not null
    ),

    -- 3. Extraction de la distribution des tentatives (attempts_until_grant)
    unnested_attempts as (
        select
            payload.partition_date,
            payload.log_started_at,
            payload.log_ended_at,
            cast(null as string) as bonus_type,
            'attempt' as status,
            cast(null as string) as error_reason,
            cast(attempt_key as int64) as attempts_count,
            cast(
                json_value(
                    payload.counters_json_obj.attempts_until_grant[attempt_key]
                ) as int64
            ) as total_requests
        from parsed_payloads as payload
        cross join
            unnest(
                json_keys(payload.counters_json_obj.attempts_until_grant, 1)
            ) as attempt_key
        where payload.counters_json_obj.attempts_until_grant is not null
    ),

    unified_events as (
        select
            partition_date,
            log_started_at,
            log_ended_at,
            bonus_type,
            status,
            error_reason,
            attempts_count,
            total_requests
        from unnested_grants

        union all

        select
            partition_date,
            log_started_at,
            log_ended_at,
            bonus_type,
            status,
            error_reason,
            attempts_count,
            total_requests
        from unnested_errors

        union all

        select
            partition_date,
            log_started_at,
            log_ended_at,
            bonus_type,
            status,
            error_reason,
            attempts_count,
            total_requests
        from unnested_attempts
    )

select
    partition_date,
    log_started_at,
    log_ended_at,
    bonus_type,
    status,
    error_reason,
    attempts_count,
    total_requests
from unified_events
