with
    last_day_of_month as (
        select
            date_trunc(active_date, month) as month,
            max(active_date) as last_active_date
        from `{{ bigquery_analytics_dataset }}.aggregated_daily_user_used_activity`
        group by 1
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
        from `{{ bigquery_analytics_dataset }}.aggregated_daily_user_used_activity` uua
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
