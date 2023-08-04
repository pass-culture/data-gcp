WITH dates AS (
    SELECT
        DISTINCT DATE_TRUNC(deposit_creation_date, MONTH) AS month
    FROM `{{ bigquery_analytics_dataset }}.enriched_deposit_data`
    WHERE deposit_creation_date >= '2023-01-01'
),

,active AS (SELECT
    DATE_TRUNC(retention_partner_history.day, MONTH) as month
    ,"{{ params.group_type }}" as dimension_name
        , {% if params.group_type == 'NAT' %}
            'NAT'
        {% else %}
            rd.{{ params.group_type_name }}
        {% endif %} as dimension_value
    ,COUNT(DISTINCT retention_partner_history.partner_id) AS nb_active_partners
FROM dates
LEFT JOIN {{ bigquery_analytics_dataset }}.retention_partner_history ON dates.month >= DATE_TRUNC(retention_partner_history.day, MONTH)
LEFT JOIN {{ bigquery_analytics_dataset }}.enriched_cultural_partner_data ON retention_partner_history.partner_id = enriched_cultural_partner_data.partner_id
WHERE DATE_DIFF(CURRENT_DATE,last_bookable_date,YEAR) = 0
AND was_registered_last_year IS TRUE
GROUP BY 1)

,all_partners AS (SELECT
    COUNT(partner_id) AS nb_total_partners
FROM {{ bigquery_analytics_dataset }}.enriched_cultural_partner_data
WHERE was_registered_last_year IS TRUE)

SELECT
    active.month AS mois
    ,dimension_name
    ,dimension_value
    , null as user_type
    , "taux_activation_partenaires" as indicator
    ,nb_total_partners AS denominator
    ,nb_active_partners AS numerator
FROM active
CROSS JOIN all_partners