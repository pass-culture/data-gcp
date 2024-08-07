WITH dates AS (
    select 
        month as month
    from unnest(generate_date_array('2020-01-01', current_date(), interval 1 month)) month
)

SELECT 
    month
    , "{{ params.group_type }}" as dimension_name
    , {% if params.group_type == 'NAT' %}
        'NAT'
    {% else %}
        rd.{{ params.group_type_name }}
    {% endif %} as dimension_value
    , deposit.deposit_type as user_type
    , "montant_depense_24_month" as indicator
    , SUM(deposit.total_actual_amount_spent) AS numerator -- montant total dépensé
    , count(distinct user.user_id) as denominator -- nombre de bénéficiaires
FROM `{{ bigquery_analytics_dataset }}.global_deposit` as `deposit`
JOIN `{{ bigquery_analytics_dataset }}.global_user` as user
    ON deposit.user_id = user.user_id 
LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` as rd
        on  user.user_department_code = rd.num_dep 
JOIN dates 
    ON DATE_DIFF(month, deposit.deposit_creation_date, MONTH) = 24
GROUP BY 1, 2, 3, 4, 5 