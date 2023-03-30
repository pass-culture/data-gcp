-- TAUX D'UTILISATION

WITH dates AS (
    SELECT 
        DISTINCT DATE_TRUNC(deposit_creation_date, MONTH) AS mois 
    FROM `{{ bigquery_analytics_dataset }}.enriched_deposit_data`
),

infos_users AS (
    SELECT 
        deposit.user_id
        , deposit.deposit_type
        , DATE_TRUNC(deposit_creation_date,MONTH) AS date_deposit
        , DATE_TRUNC(deposit_expiration_date,MONTH) AS date_expiration
        , first_booking_date 
        , user.user_department_code
        , user.user_region_name
    FROM `{{ bigquery_analytics_dataset }}.enriched_deposit_data` as deposit
    JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` as user ON deposit.user_id = user.user_id
)

-- Chaque mois on compte tous les jeunes qui sont bénéficiaires
-- On considère comme actifs ceux qui ont fait au moins une réservation dans les 3 premiers mois d'inscriptions.
-- Ex : en janvier 2023, il y a 2M bénéficiaires. Sur ces 2M bénéficiaires actuels (crédit n'est pas expiré),
-- 57% ont fait une réservation dans les 3 premiers mois d'inscription, qu'elle que soit la date à laquelle ils aient reçu leur crédit

SELECT 
    mois -- tous les mois
    , "{{ params.group_type }}" as dimension_name
    , {% if params.group_type == 'all' %}
        'NAT'
    {% else %}
        {{ params.group_type_name }}
    {% endif %} as dimension_value
    , deposit_type as user_type
    , "taux_activation" as indicator
    , COUNT(DISTINCT 
        CASE
            WHEN DATE_DIFF(first_booking_date, date_deposit, DAY) <= {{ params.months_threshold }} 
            THEN user_id 
            ELSE NULL 
        END) as numerator -- ceux qui sont bénéficiaires actuels et qui ont fait 1 résa dans les 3 mois après inscription
    , COUNT(DISTINCT user_id) as denominator -- ceux qui sont bénéficiaires actuels
FROM dates
LEFT JOIN infos_users
    ON dates.mois >= infos_users.date_deposit -- ici pour prendre uniquement les bénéficiaires actuels
    AND dates.mois <= infos_users.date_expiration -- idem
    AND DATE_DIFF(CURRENT_DATE, date_deposit, DAY) >= {{ params.months_threshold }} -- Base = uniquement les jeunes inscrits depuis +3 mois
GROUP BY 1, 2, 3, 4, 5