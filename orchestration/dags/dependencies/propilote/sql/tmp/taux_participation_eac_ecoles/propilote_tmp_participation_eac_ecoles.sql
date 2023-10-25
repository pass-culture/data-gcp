WITH last_day AS (
SELECT 
    DATE_TRUNC(date,MONTH) AS date,
    MAX(date) AS last_date 
FROM `{{ bigquery_analytics_dataset }}.adage_involved_student`
WHERE 
    -- On prend l'année scolaire en cours 
    (educational_year_beginning_date <= current_date() AND educational_year_expiration_date > current_date()) 
    OR 
    -- Et l'année scolaire précédente pour la comparaison 
    (educational_year_beginning_date <= DATE_SUB(current_date(), interval 1 year) AND educational_year_expiration_date > DATE_SUB(current_date(), interval 1 year)) 
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
    , null as user_type -- nous n'avons pas le détail par age dans la table adage_involved_institution
    , "taux_participation_eac_jeunes" as indicator
    , SUM(institutions) AS numerator -- active_institutions
    , SUM(total_institutions) AS denominator -- total_institutions
FROM `{{ bigquery_analytics_dataset }}.adage_involved_institution` as involved
-- take only last day for each month.
JOIN last_day ON last_day.last_date = involved.date AND DATE_TRUNC(involved.date,MONTH) = last_day.date 
LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` as rd
    ON involved.department_code = rd.num_dep
-- on selectionne uniquement l'année scolaire qui correspond à la date de calcul
JOIN `{{ bigquery_analytics_dataset }}.applicative_database_educational_year` as ey ON ey.scholar_year = involved.scholar_year AND last_day.last_date >= ey.educational_year_beginning_date AND last_day.last_date < ey.educational_year_expiration_date
WHERE
{% if params.group_type == 'NAT' %}
    department_code = '-1'
{% else %}
    NOT department_code = '-1'
{% endif %}
GROUP BY 1, 2, 3, 4, 5