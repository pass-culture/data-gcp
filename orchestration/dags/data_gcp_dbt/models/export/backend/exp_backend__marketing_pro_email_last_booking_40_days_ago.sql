{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "execution_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
}}

SELECT
     DATE('{{ ds() }}') as execution_date
    ,venue_id
    ,venue_booking_email
FROM  {{ ref('mrt_global__venue') }}
WHERE venue_is_permanent
AND DATE_DIFF(current_date, last_booking_date, DAY) = 40
