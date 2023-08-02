WITH temp1 AS
  (SELECT FORMAT_DATE("%Y-%m-%d", MONTH) AS date_valeur ,
          CASE
              WHEN dimension_value = 'NAT' THEN 'FRANCE'
              ELSE dimension_value
          END AS dimension_value ,
          dep_name ,
          "va" AS type_valeur ,
          CASE
              WHEN
                   INDICATOR = "taux_couverture"
                   AND user_type = "19" THEN "IND-200"
              WHEN
                   INDICATOR = "taux_couverture"
                   AND user_type = "17" THEN "IND-199"
              WHEN
                   INDICATOR = "montant_depense_24_month"
                   AND user_type = "GRANT_18"
                   AND MONTH > "2023-05-01" THEN "IND-202"
              WHEN
                   INDICATOR = "taux_participation_eac_jeunes" THEN "IND-205"
              WHEN
                   INDICATOR = "taux_activation_structure" THEN "IND-201"
          END AS identifiant_indic ,
          numerator ,
          denominator
   FROM `{{ bigquery_analytics_dataset }}`.propilote_kpis
   LEFT JOIN `{{ bigquery_analytics_dataset }}`.region_department ON propilote_kpis.dimension_value = region_department.num_dep) ,
     temp2 AS
  (SELECT identifiant_indic ,
          CASE
              WHEN dep_name IS NOT NULL THEN dep_name
              ELSE dimension_value
          END AS dimension_value ,
          date_valeur ,
          type_valeur ,
          CASE
              WHEN identifiant_indic = 'IND-202' THEN SAFE_DIVIDE(numerator, denominator)
              WHEN identifiant_indic != 'IND-202'
                   AND ROUND(SAFE_DIVIDE(numerator, denominator)*100, 2) > 100 THEN 100
              ELSE ROUND(SAFE_DIVIDE(numerator, denominator)*100, 2)
          END AS valeur
   FROM temp1
   WHERE identifiant_indic IS NOT NULL
     AND dimension_value IS NOT NULL)
SELECT identifiant_indic ,
       CASE
           WHEN temp2.dimension_value = 'FRANCE' THEN temp2.dimension_value
           ELSE pilote_geographic_standards.zone_id
       END AS zone_id ,
       date_valeur ,
       type_valeur ,
       valeur
FROM temp2
LEFT JOIN `{{ bigquery_analytics_dataset }}`.pilote_geographic_standards ON temp2.dimension_value = pilote_geographic_standards.nom
WHERE pilote_geographic_standards.zone_id IS NOT NULL
  OR temp2.dimension_value = 'FRANCE'
