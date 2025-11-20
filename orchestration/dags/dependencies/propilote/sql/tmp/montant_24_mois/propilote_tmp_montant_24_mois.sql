with
    dates as (
        select month as month
        from
            unnest(
                generate_date_array('2020-01-01', current_date(), interval 1 month)
            ) month
    )

select
    month,
    "{{ params.group_type }}" as dimension_name,
    {% if params.group_type == "NAT" %} 'NAT'
    {% else %} rd.{{ params.group_type_name }}
    {% endif %} as dimension_value,
    deposit.deposit_type as user_type,
    "montant_depense_24_month" as indicator,
    sum(deposit.total_actual_amount_spent) as numerator,  -- montant total dépensé
    count(distinct user.user_id) as denominator  -- nombre de bénéficiaires
from `{{ bigquery_analytics_dataset }}.global_deposit` as `deposit`
join
    `{{ bigquery_analytics_dataset }}.global_user_beneficiary` as user
    on deposit.user_id = user.user_id
left join
    `{{ bigquery_seed_dataset }}.region_department` as rd
    on user.user_department_code = rd.num_dep
join dates on date_diff(month, deposit.deposit_creation_date, month) = 24
group by 1, 2, 3, 4, 5
