with
    last_day_of_month as (
        select
            date_trunc(deposit_active_date, month) as month,
            max(deposit_active_date) as last_active_date
        from `{{ bigquery_analytics_dataset }}.native_daily_user_deposit`
        group by date_trunc(deposit_active_date, month)
    ),

    user_amount_spent_per_day as (
        select
            deposit_active_date,
            user_id,
            deposit_amount,
            coalesce(sum(booking_intermediary_amount), 0) as amount_spent,
        from `{{ bigquery_analytics_dataset }}.native_daily_user_deposit` uua
        left join
            {{ ref("mrt_global__booking") }} ebd
            on ebd.deposit_id = uua.deposit_id
            and uua.deposit_active_date = date(booking_used_date)
            and booking_is_used
        group by deposit_active_date, user_id, deposit_amount
    ),

    user_cumulative_amount_spent as (
        select
            user_id,
            deposit_id,
            sum(amount_spent) over (
                partition by user_id, deposit_id order by deposit_active_date asc
            ) as cumulative_amount_spent,
        from user_amount_spent_per_day
    ),

    aggregated_active_beneficiary as (
        select
            month,
            "{{ params.group_type }}" as dimension_name,
            {% if params.group_type == "NAT" %} 'NAT'
            {% else %} {{ params.group_type_name }}
            {% endif %} as dimension_value,
            deposit_type as user_type,
            "beneficiaire_actuel" as indicator,
            count(distinct uua.user_id) as numerator,
            1 as denominator
        from user_cumulative_amount_spent uua
        inner join last_day_of_month ldm on ldm.last_active_date = active_date
        -- active nor suspended
        inner join
            `{{ bigquery_analytics_dataset }}.global_user` eud
            on eud.user_id = uua.user_id
        left join
            `{{ bigquery_analytics_dataset }}.region_department` as rd
            on eud.user_department_code = rd.num_dep
        -- still have some credit at EOM
        where cumulative_amount_spent < initial_deposit_amount

        group by 1, 2, 3, 4, 5
    ),

    aggregated_total_beneficiairy as (
        select
            month,
            "{{ params.group_type }}" as dimension_name,
            {% if params.group_type == "NAT" %} 'NAT'
            {% else %} {{ params.group_type_name }}
            {% endif %} as dimension_value,
            cast(null as string) as user_type,
            "beneficiaire_total" as indicator,
            count(distinct eud.user_id) as numerator,
            1 as denominator
        from last_day_of_month ldm
        inner join
            `{{ bigquery_analytics_dataset }}.global_user` eud
            on date(eud.first_deposit_creation_date) <= date(ldm.last_active_date)
        left join
            `{{ bigquery_analytics_dataset }}.region_department` as rd
            on eud.user_department_code = rd.num_dep
        group by 1, 2, 3, 4, 5
    )

select *
from aggregated_active_beneficiary
union all
select *
from aggregated_total_beneficiairy
