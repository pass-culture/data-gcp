{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "execution_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="append_new_columns",
        )
    )
}}

select date('{{ ds() }}') as execution_date, venue_id, venue_booking_email
from {{ ref("mrt_global__venue") }}
where
    venue_is_open_to_public
    and venue_booking_email is not null
    and date_diff(current_date, last_bookable_offer_date, day) = 40
