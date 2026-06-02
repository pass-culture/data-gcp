{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "deposit_active_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="append_new_columns",
            require_partition_filter=true,
        )
    )
}}

with
    -- Regenerate last 7 days
    __days as (
        select *
        from
            unnest(
                generate_date_array(
                    {% if is_incremental() %}date_sub('{{ ds() }}', interval 7 day),
                    {% else %}'2019-02-11',
                    {% endif %}
                    current_date,
                    interval 1 day
                )
            ) as day
    )

select
    __days.day as deposit_active_date,
    u.user_id,
    u.user_department_code,
    u.user_region_name,
    u.user_birth_date,
    int_global__deposit.deposit_id,
    int_global__deposit.deposit_amount,
    int_global__deposit.deposit_creation_date,
    int_global__deposit.deposit_type,
    {{ calculate_exact_age("__days.day", "u.user_birth_date") }} as user_age
from {{ ref("int_global__deposit") }} as int_global__deposit
inner join
    {{ ref("int_global__user_beneficiary") }} as u
    on int_global__deposit.user_id = u.user_id
inner join
    __days
    on __days.day between date(int_global__deposit.deposit_creation_date) and date(
        int_global__deposit.deposit_expiration_date
    )
left join
    {{ ref("int_applicative__action_history") }} as ah
    on int_global__deposit.user_id = ah.user_id
    and ah.action_history_rk = 1
where (u.user_is_active or ah.action_history_reason = 'upon user request')
