{{
    config(
        materialized='ephemeral'
    )
}}

{% set dimensions =
        [
            {"name": "NAT", "value_expr": "'NAT'"},
            {"name": "REG", "value_expr": "region_name"},
            {"name": "DEP", "value_expr": "dep_name"}
        ]
    %}

-- noqa: disable=all
with
    last_day_of_month as (
        select
            date_trunc(deposit_active_date, month) as execution_date,
            date("{{ ds }}") as update_date,
            max(deposit_active_date) as last_active_date
        from ref("mrt_native__daily_user_deposit")
        where deposit_active_date > date('2021-01-01')
        group by date_trunc(deposit_active_date, month)
    ),

    user_amount_spent_per_day as (
        select
            uua.deposit_active_date,
            uua.user_id,
            uua.deposit_amount,
            coalesce(sum(booking_intermediary_amount), 0) as amount_spent,
            case
                when uua.deposit_type = "GRANT_17_18" and uua.user_age <= 17
                then "GRANT_15_17"
                when uua.deposit_type = "GRANT_17_18" and uua.user_age >= 18
                then "GRANT_18"
                else uua.deposit_type
            end as deposit_type
        from ref("mrt_native__daily_user_deposit") uua
        left join
            ref("mrt_global__booking") ebd
            on ebd.deposit_id = uua.deposit_id
            and uua.deposit_active_date = date(booking_used_date)
            and booking_is_used
        where deposit_active_date > date('2021-01-01')
        group by deposit_active_date, user_id, deposit_type, deposit_amount
    ),

    user_cumulative_amount_spent as (
        select
            deposit_active_date,
            user_id,
            deposit_type,
            deposit_amount as initial_deposit_amount,
            sum(amount_spent) over (
                partition by user_id, deposit_type order by deposit_active_date asc
            ) as cumulative_amount_spent,
        from user_amount_spent_per_day
    ),

    aggregated_active_beneficiary as (

    {% for dim in dimensions %}
        {% if not loop.first %}UNION ALL{% endif %}
            select
            execution_date,
            update_date,
            '{{ dim.name }}' as dimension_name,
            {{ dim.value_expr }} as dimension_value,
            deposit_type as user_type,
            'beneficiaire_actuel' as indicator,
            count(distinct uua.user_id) as numerator,
            1 as denominator
        from user_cumulative_amount_spent uua
        inner join last_day_of_month ldm
            on ldm.last_active_date = uua.deposit_active_date
        inner join {{ ref('mrt_global__user') }} eud
            on eud.user_id = uua.user_id
        left join {{ ref('region_department') }} rd
            on eud.user_department_code = rd.num_dep
        where cumulative_amount_spent < initial_deposit_amount
        group by execution_date, update_date, dim.name, dim.value_expr, deposit_type, indicator
    {% endfor %}


    ),

    aggregated_total_beneficiairy as (
    {% for dim in dimensions %}
        {% if not loop.first %}UNION ALL{% endif %}
        select
            execution_date,
            update_date,
            '{{ dim.name }}' as dimension_name,
            {{ dim.value_expr }} as dimension_value,
            cast(null as string) as user_type,
            "beneficiaire_total" as indicator,
            count(distinct eud.user_id) as numerator,
            1 as denominator
        from last_day_of_month ldm
        inner join
            ref("mrt_global__user") eud
            on date(eud.first_deposit_creation_date) <= date(ldm.last_active_date)
        left join
            ref("region_department") as rd
            on eud.user_department_code = rd.num_dep
        group by execution_date, update_date, dim.name, dim.value_expr, user_type, indiator
    {% endfor %}
    )

select *
from aggregated_active_beneficiary
union all
select *
from aggregated_total_beneficiairy
