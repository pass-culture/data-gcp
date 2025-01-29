with
    coverage_19 as (
        select
            date_trunc(active_month, month) as month,
            "{{ params.group_type }}" as dimension_name,
            {% if params.group_type == "NAT" %} 'NAT'
            {% else %} {{ params.group_type_name }}
            {% endif %} as dimension_value,
            '19' as user_type,
            "taux_couverture" as indicator,
            sum(total_users_last_12_months) as numerator,
            sum(population_last_12_months) as denominator
        from `{{ bigquery_analytics_dataset }}.monthly_beneficiary_coverage` as up
        where decimal_age = "19" and active_month <= date_trunc(current_date, month)
        group by 1, 2, 3, 4, 5
    ),

    coverage_18 as (
        select
            date_trunc(active_month, month) as month,
            "{{ params.group_type }}" as dimension_name,
            {% if params.group_type == "NAT" %} 'NAT'
            {% else %} {{ params.group_type_name }}
            {% endif %} as dimension_value,
            '18' as user_type,
            "taux_couverture" as indicator,
            sum(total_users_last_12_months) as numerator,
            sum(population_last_12_months) as denominator
        from `{{ bigquery_analytics_dataset }}.monthly_beneficiary_coverage` as up
        where decimal_age = "18" and active_month <= date_trunc(current_date, month)
        group by 1, 2, 3, 4, 5
    ),

    coverage_17 as (
        select
            date_trunc(active_month, month) as month,
            "{{ params.group_type }}" as dimension_name,
            {% if params.group_type == "NAT" %} 'NAT'
            {% else %} {{ params.group_type_name }}
            {% endif %} as dimension_value,
            '17' as user_type,
            "taux_couverture" as indicator,
            sum(total_users_last_12_months) as numerator,
            sum(population_last_12_months) as denominator
        from `{{ bigquery_analytics_dataset }}.monthly_beneficiary_coverage` as up
        where decimal_age = "17" and active_month <= date_trunc(current_date, month)
        group by 1, 2, 3, 4, 5
    ),

    coverage_16 as (
        select
            date_trunc(active_month, month) as month,
            "{{ params.group_type }}" as dimension_name,
            {% if params.group_type == "NAT" %} 'NAT'
            {% else %} {{ params.group_type_name }}
            {% endif %} as dimension_value,
            '16' as user_type,
            "taux_couverture" as indicator,
            sum(total_users_last_12_months) as numerator,
            sum(population_last_12_months) as denominator
        from `{{ bigquery_analytics_dataset }}.monthly_beneficiary_coverage` as up
        where decimal_age = "16" and active_month <= date_trunc(current_date, month)
        group by 1, 2, 3, 4, 5
    )

select *
from coverage_19
union all
select *
from coverage_18
union all
select *
from coverage_17
union all
select *
from coverage_16
