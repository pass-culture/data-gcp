WITH dates AS (
    SELECT DISTINCT 
        DATE_TRUNC(user_activation_date,MONTH) AS mois 
    FROM `{{ bigquery_analytics_dataset }}.enriched_user_data`
),

institutions AS (
    SELECT 
        dates.mois
        , "{{ params.group_type }}" as dimension_name
        , {% if params.group_type == 'NAT' %}
            'NAT'
        {% else %}
            {{ params.group_type_name }}
        {% endif %} as dimension_value
        , COUNT(DISTINCT institution_id) AS total_institutions
    FROM dates
    LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_institution_data` institution
        ON dates.mois >= DATE_TRUNC(institution.first_deposit_creation_date, MONTH)
    GROUP BY 1, 2, 3
), 

active_institutions AS (
    SELECT 
        dates.mois
        , "{{ params.group_type }}" as dimension_name
        , {% if params.group_type == 'NAT' %}
            'NAT'
        {% else %}
            {{ params.group_type_name }}
        {% endif %} as dimension_value
        , COUNT(DISTINCT educational_institution_id) AS total_active_institutions
    FROM dates
    JOIN `{{ bigquery_analytics_dataset }}.enriched_collective_booking_data` as collective_booking
        ON dates.mois >= DATE_TRUNC(collective_booking.collective_booking_creation_date, MONTH)
        AND collective_booking_status IN ('USED','REIMBURSED','CONFIRMED')
    JOIN `{{ bigquery_analytics_dataset }}.enriched_institution_data` as institution
        ON collective_booking.educational_institution_id = institution.institution_id
    GROUP BY 1, 2, 3
    )

SELECT 
    institutions.mois 
    , institutions.dimension_name
    , institutions.dimension_value
    , null as user_type 
    , "taux_participation_eac_ecoles" as indicator
    , total_active_institutions as numerator
    , total_institutions as denominator

FROM institutions
LEFT JOIN active_institutions 
    ON institutions.mois = active_institutions.mois 
    AND institutions.dimension_value = active_institutions.dimension_value