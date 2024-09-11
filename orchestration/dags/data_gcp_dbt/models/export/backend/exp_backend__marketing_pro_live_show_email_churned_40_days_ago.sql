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
from {{ ref('mrt_global__venue') }} venue
join {{ ref('mrt_global__offerer') }} offerer ON venue.venue_managing_offerer_id=offerer.offerer_id
    and DATE_DIFF(CURRENT_DATE, offerer.last_bookable_offer_date, day) = 40
where venue_is_permanent
    and venue_type_label = "Spectacle vivant"
    and venue_booking_email is not NULL
    