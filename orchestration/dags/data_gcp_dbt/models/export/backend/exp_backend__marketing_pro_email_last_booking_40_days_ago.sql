{{
    config(
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "execution_date", "data_type": "date", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
) }}

select
    DATE('{{ ds() }}') as execution_date,
    venue_id,
    venue_booking_email
from {{ ref('mrt_global__venue') }}
where venue_is_permanent
    and DATE_DIFF(CURRENT_DATE, last_booking_date, day) = 40
