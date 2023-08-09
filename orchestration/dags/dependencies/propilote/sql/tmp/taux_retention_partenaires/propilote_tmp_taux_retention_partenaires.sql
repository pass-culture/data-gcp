WITH dates AS
  (SELECT DISTINCT DATE_TRUNC(deposit_creation_date, MONTH) AS MONTH
   FROM `{{ bigquery_analytics_dataset }}.enriched_deposit_data`
   WHERE deposit_creation_date >= '2023-01-01' ) ,
     active AS
  (SELECT DATE_TRUNC(retention_partner_history.day, MONTH) AS MONTH ,
          "{{ params.group_type }}" AS dimension_name , {% IF params.group_type == 'NAT' %} 'NAT' {% ELSE %} {{ params.group_type_name }} {% endif %} AS dimension_value ,
                                                                                                                                                    COUNT(DISTINCT retention_partner_history.partner_id) AS nb_active_partners
   FROM dates
   LEFT JOIN {{ bigquery_analytics_dataset }}.retention_partner_history ON dates.month >= DATE_TRUNC(retention_partner_history.day, MONTH)
   LEFT JOIN {{ bigquery_analytics_dataset }}.enriched_cultural_partner_data ON retention_partner_history.partner_id = enriched_cultural_partner_data.partner_id
   LEFT JOIN {{ bigquery_analytics_dataset }}.region_department ON enriched_cultural_partner_data.partner_department_code = region_department.num_dep
   WHERE DATE_DIFF(CURRENT_DATE, last_bookable_date, MONTH) < 12
     AND was_registered_last_year IS TRUE
   GROUP BY 1,
            2,
            3) ,
     all_partners AS
  (SELECT DATE_TRUNC(partner_creation_date, MONTH) AS MONTH ,
          "{{ params.group_type }}" AS dimension_name , {% IF params.group_type == 'NAT' %} 'NAT' {% ELSE %} {{ params.group_type_name }} {% endif %} AS dimension_value ,
                                                                                                                                                    COUNT(partner_id) AS nb_total_partners
   FROM dates
   LEFT JOIN {{ bigquery_analytics_dataset }}.enriched_cultural_partner_data ON dates.month >= DATE_TRUNC(partner_creation_date, MONTH)
   LEFT JOIN {{ bigquery_analytics_dataset }}.region_department ON enriched_cultural_partner_data.partner_department_code = region_department.num_dep
   WHERE was_registered_last_year IS TRUE
     AND total_offers_created > 0
   GROUP BY 1,
            2,
            3)
SELECT active.month AS mois ,
       active.dimension_name ,
       active.dimension_value ,
       NULL AS user_type ,
       "taux_activation_partenaires" AS
INDICATOR ,
       nb_total_partners AS denominator ,
       nb_active_partners AS numerator
FROM all_partners
JOIN active ON active.dimension_name = all_partners.dimension_name
AND active.month = all_partners.month