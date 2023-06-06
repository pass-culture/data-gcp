WITH dates AS (
    SELECT 
        DISTINCT DATE_TRUNC(user_activation_date, MONTH) AS month 
    FROM `{{ bigquery_analytics_dataset }}.enriched_user_data`
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
    , SUM(deposit_actual_amount_spent) AS numerator -- montant total dépensé
    , count(distinct user.user_id) as denominator -- nombre de bénéficiaires
FROM `{{ bigquery_analytics_dataset }}.enriched_deposit_data` as deposit
JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` as user 
    ON deposit.user_id = user.user_id 
LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` as rd
        on  user.user_department_code = rd.num_dep 
JOIN dates 
    ON DATE_DIFF(month, deposit.deposit_creation_date, MONTH) = 24
GROUP BY 1, 2, 3, 4, 5 