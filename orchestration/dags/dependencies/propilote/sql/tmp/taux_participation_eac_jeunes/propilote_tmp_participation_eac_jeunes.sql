-- Part des élèves ayant participé à une offre EAC

SELECT
    DATE_TRUNC(date, MONTH) AS mois
    , "{{ params.group_type }}" as dimension_name
    , {% if params.group_type == 'all' %}
        'NAT'
    {% else %}
        {{ params.group_type_name }}
    {% endif %} as dimension_value
    , null as user_type -- nous n'avons pas le détail par age dans la table adage_involved_students
    , "taux_participation_eac_jeunes" as indicator
    , SUM(involved_students) AS numerator -- students_involved_in_eac_offer
    , SUM(total_involved_students) AS denominator -- students_eligible
FROM `{{ bigquery_analytics_dataset }}.adage_involved_student` as involved
LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` as rd
    ON involved.department_code = rd.num_dep
WHERE NOT department_code = '-1' 
GROUP BY 1, 2, 3, 4, 5