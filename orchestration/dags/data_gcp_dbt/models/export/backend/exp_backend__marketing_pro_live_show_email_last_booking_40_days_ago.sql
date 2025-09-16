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

select date('{{ ds() }}') as execution_date, venue_id, venue_booking_email, 'test' as dbt_test_field
from {{ ref("mrt_global__venue") }} as venue
inner join
    {{ ref("mrt_global__offerer") }} as offerer
    on venue.offerer_id = offerer.offerer_id
    and date_diff(date('{{ ds() }}'), offerer.last_booking_date, day) >= 40
where venue_is_open_to_public and venue_type_label = 'Spectacle vivant'
