{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "active_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="sync_all_columns",
            require_partition_filter=true,
        )
    )
}}

with
    -- Regenerate last 7 days
    days as (
        select *
        from
            unnest(
                generate_date_array(
                    {% if is_incremental() %}date_sub('{{ ds() }}', interval 7 day),
                    {% else %}'2019-02-11',
                    {% endif %}
                    '{{ ds() }}',
                    interval 1 day
                )
            ) as day
    ),

select
    days.day as user_snapshot_date,
    mrt_global__deposit.user_id,
    mrt_global__deposit.user_department_code,
    mrt_global__deposit.user_region_name,
    mrt_global__deposit.user_birth_date,
    if(
        extract(dayofyear from days.day) < extract(dayofyear from days.day),
        date_diff(days.day, mrt_global__deposit.user_birth_date, year) - 1,
        date_diff(days.day, mrt_global__deposit.user_birth_date, year)
    ) as user_age,
    mrt_global__deposit.deposit_id,
    mrt_global__deposit.deposit_amount,
    mrt_global__deposit.deposit_creation_date,
    mrt_global__deposit.deposit_type,

from {{ ref("mrt_global__deposit") }} as mrt_global__deposit
join
    days
    on days.day between date(mrt_global__deposit.deposit_creation_date) and date(
        mrt_global__deposit.deposit_expiration_date
    )
