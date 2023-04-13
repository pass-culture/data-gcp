WITH couverture_18 as (
    SELECT 
        DATE_TRUNC(active_month, MONTH) AS mois
        , "{{ params.group_type }}" as dimension_name
        , {% if params.group_type == 'all' %}
            'NAT'
        {% else %}
            {{ params.group_type_name }}
        {% endif %} as dimension_value
        , '18' as user_type
        , "taux_couverture" as indicator
        , sum(total_users) as numerator
        , sum(population) as denominator
    FROM `{{ bigquery_analytics_dataset }}.user_penetration` as up
    LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` as rd
        on  up.department_code = rd.num_dep 
    WHERE floor(decimal_age) = 19
    AND active_month <= DATE_TRUNC(CURRENT_DATE, MONTH)
    GROUP BY 1, 2, 3, 4, 5
),

couverture_16 as (
    SELECT 
        DATE_TRUNC(active_month,MONTH) AS mois
        , "{{ params.group_type }}" as dimension_name
        , {% if params.group_type == 'all' %}
            'NAT' 
        {% else %}
            {{ params.group_type_name }}
        {% endif %} as dimension_value
        , '16' as user_type
        , "taux_couverture" as indicator
        , sum(total_users) as numerator
        , sum(population) as denominator

    FROM `{{ bigquery_analytics_dataset }}.user_penetration` as up
    LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` as rd
        on  up.department_code = rd.num_dep 
    WHERE floor(decimal_age) = 17
    AND active_month <= DATE_TRUNC(CURRENT_DATE, MONTH)
    GROUP BY 1, 2, 3, 4, 5
),

couverture_15_17 as (
    SELECT 
        DATE_TRUNC(active_month,MONTH) AS mois
        , "{{ params.group_type }}" as dimension_name
        , {% if params.group_type == 'all' %}
            'NAT'
        {% else %}
            {{ params.group_type_name }}
        {% endif %} as dimension_value
        , '15_17' as user_type
        , "taux_couverture" as indicator
        , sum(total_users) as numerator
        , sum(population) as denominator
    FROM `{{ bigquery_analytics_dataset }}.user_penetration` as up
    LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` as rd
        on  up.department_code = rd.num_dep 
    WHERE age_range = "15_17"
    AND active_month <= DATE_TRUNC(CURRENT_DATE, MONTH)
    GROUP BY 1, 2, 3, 4, 5
)


SELECT *
FROM couverture_18 
UNION ALL
SELECT *
FROM couverture_16
UNION ALL
SELECT *
FROM couverture_15_17
