WITH last_year_beginning_date as (
SELECT 
    educational_year_beginning_date as last_year_start_date
FROM `{{ bigquery_raw_dataset }}.applicative_database_educational_year`
WHERE educational_year_beginning_date <= DATE_SUB(current_date(), interval 1 year) AND educational_year_expiration_date > DATE_SUB(current_date(), interval 1 year)
)

, last_day AS (
SELECT 
    DATE_TRUNC(date,MONTH) AS date,
    MAX(date) AS last_date,
    MAX(adage_id) AS last_adage_id
FROM `{{ bigquery_analytics_dataset }}.adage_involved_student`
WHERE 
    date <= current_date 
AND 
    date > (select last_year_start_date from last_year_beginning_date)
GROUP BY 1
)

SELECT
    DATE_TRUNC(involved.date, MONTH) AS month
    , "{{ params.group_type }}" as dimension_name
    , {% if params.group_type == 'NAT' %}
        'NAT'
    {% else %}
        {{ params.group_type_name }}
    {% endif %} as dimension_value
    , level_code as user_type -- On utilise le user_type pour distinguer le taux de participation EAC par niveau d'éducation
    , "taux_participation_eac_jeunes" as indicator
    , SUM(involved_students) AS numerator -- students_involved_in_eac_offer
    , SUM(total_involved_students) AS denominator -- students_eligible
FROM `{{ bigquery_analytics_dataset }}.adage_involved_student` as involved
-- take only last day for each month.
JOIN last_day ON last_day.last_date = involved.date AND DATE_TRUNC(involved.date,MONTH) = last_day.date AND last_day.last_adage_id = involved.adage_id
LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` as rd
    ON involved.department_code = rd.num_dep
-- on selectionne uniquement l'année scolaire qui correspond à la date de calcul
WHERE
{% if params.group_type == 'NAT' %}
    department_code = '-1'
{% else %}
    NOT department_code = '-1'
{% endif %}
GROUP BY 1, 2, 3, 4, 5