WITH dates AS (
    SELECT 
        DISTINCT DATE_TRUNC(deposit_creation_date, MONTH) AS month 
    FROM `{{ bigquery_analytics_dataset }}.enriched_deposit_data`
),

infos_users AS (
    SELECT 
        deposit.user_id
        , deposit.deposit_type
        , DATE_TRUNC(deposit_creation_date, MONTH) AS date_deposit
        , DATE_TRUNC(deposit_expiration_date, MONTH) AS date_expiration
        , user.user_department_code
        , user.user_region_name
        , rd.academy_name
    FROM `{{ bigquery_analytics_dataset }}.enriched_deposit_data` as deposit
    JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` as user ON deposit.user_id = user.user_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` as rd
        on  user.user_department_code = rd.num_dep 
),

current_beneficiary AS (
    SELECT 
        month -- tous les month
        , "{{ params.group_type }}" as dimension_name
        , {% if params.group_type == 'NAT' %}
            'NAT'
        {% else %}
            {{ params.group_type_name }}
        {% endif %} as dimension_value
        , deposit_type as user_type
        , "beneficiaire_actuel" as indicator
        , COUNT(DISTINCT user_id) as numerator  -- ceux qui sont bénéficiaires actuels
        , 1 as denominator
    FROM dates
    LEFT JOIN infos_users
        ON dates.month >= infos_users.date_deposit -- ici pour prendre uniquement les bénéficiaires actuels
        AND dates.month <= infos_users.date_expiration -- idem
    GROUP BY 1, 2, 3, 4, 5
),


total_beneficiary AS (
    SELECT 
        month -- tous les month
        , "{{ params.group_type }}" as dimension_name
        , {% if params.group_type == 'NAT' %}
            'NAT'
        {% else %}
            {{ params.group_type_name }}
        {% endif %} as dimension_value
        , cast(null as string) as user_type
        , "beneficiaire_total" as indicator
        , COUNT(DISTINCT user_id) as numerator  
        , 1 as denominator
    FROM dates
    LEFT JOIN infos_users
        ON dates.month >= infos_users.date_deposit
    GROUP BY 1, 2, 3, 4, 5
)




SELECT *
FROM current_beneficiary 
UNION ALL
SELECT *
FROM total_beneficiary