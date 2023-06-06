WITH eple_infos AS (
SELECT DISTINCT 
    institution_id 
    ,institution_external_id
    ,institution_name
    ,institution_city
    ,institution_departement_code
    ,region_name
    ,institution_academie
    ,eid.ministry 
    ,institution_type
    ,ey.scholar_year
    ,institution_current_deposit_amount
FROM `{{ bigquery_analytics_dataset }}.enriched_institution_data` eid
LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` rd ON eid.institution_departement_code = rd.num_dep
JOIN `{{ bigquery_clean_dataset }}.applicative_database_educational_deposit` ed ON ed.educational_institution_id = eid.institution_id
JOIN `{{ bigquery_clean_dataset }}.applicative_database_educational_year` ey ON ey.adage_id = ed.educational_year_id
), 

eple_bookings AS (
SELECT 
    eple_infos.institution_id
    ,ey.scholar_year
    ,SUM(CASE WHEN collective_booking_status != 'CANCELLED' THEN booking_amount ELSE NULL END) AS montant_depense_theorique
    ,SUM(CASE WHEN collective_booking_status IN ('USED','REIMBURSED') THEN booking_amount ELSE NULL END) AS montant_depense_reel
FROM eple_infos
JOIN `{{ bigquery_analytics_dataset }}.enriched_collective_booking_data` ecbd ON ecbd.educational_institution_id = eple_infos.institution_id
JOIN `{{ bigquery_clean_dataset }}.applicative_database_educational_year` ey ON ecbd.educational_year_id = ey.adage_id
GROUP BY 1, 2)

,total_nb_of_students AS (
SELECT 
    eid.institution_id 
    ,eid.institution_name 
    ,SUM(number_of_students) AS nb_students
FROM `{{ bigquery_analytics_dataset }}.enriched_institution_data` eid   
LEFT JOIN `{{ bigquery_analytics_dataset }}.number_of_students_per_eple` ns ON eid.institution_external_id = ns.institution_external_id
GROUP BY 1,2)

,nb_eleves_educonnectes_per_eple AS (
SELECT  
     TRIM(json_extract(result_content, '$.school_uai'), '"') AS school 
    , COUNT(DISTINCT edd.user_id) AS nb_jeunes_credited
    , COUNT(DISTINCT CASE WHEN DATE_DIFF(current_date, deposit_creation_date, DAY) <= 365 THEN edd.user_id ELSE NULL END) AS nb_jeunes_credited_last_12_months
    , AVG(COALESCE(deposit_theoretical_amount_spent,0)) AS avg_spent_per_user
    , SAFE_DIVIDE(SUM(deposit_theoretical_amount_spent), SUM(deposit_amount)) AS pct_spent
    , COUNT(DISTINCT ebd.user_id) AS nb_jeunes_credit_utilise
FROM `{{ bigquery_clean_dataset }}.applicative_database_beneficiary_fraud_check` bfc
JOIN `{{ bigquery_analytics_dataset }}.enriched_deposit_data` edd ON edd.user_id = bfc.user_id
LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_booking_data` ebd ON ebd.user_id = edd.user_id AND not booking_is_cancelled
WHERE type = 'EDUCONNECT'
AND json_extract(result_content, '$.school_uai') IS NOT NULL
AND edd.deposit_type = 'GRANT_15_17'
GROUP BY 1)

SELECT 
    eple_infos.institution_id 
    ,eple_infos.institution_external_id
    ,eple_infos.institution_name
    ,eple_infos.institution_academie
    ,eple_infos.region_name
    ,eple_infos.institution_departement_code
    ,eple_infos.institution_city
    ,eple_infos.ministry
    ,eple_infos.institution_type
    ,eple_infos.scholar_year
    ,institution_current_deposit_amount
    ,montant_depense_theorique
    ,SAFE_DIVIDE(montant_depense_theorique, institution_current_deposit_amount) AS part_credit_depense_theorique
    ,montant_depense_reel
    ,SAFE_DIVIDE(montant_depense_reel, institution_current_deposit_amount) AS part_credit_depense_reel
    ,nb_students AS nb_total_eleves
    ,nb_jeunes_credited AS nb_inscrits_educonnect
    ,nb_jeunes_credited_last_12_months AS dont_inscrits_last_12_months
    ,nb_jeunes_credit_utilise
    ,avg_spent_per_user
    ,pct_spent AS pct_spent_per_user
    ,SAFE_DIVIDE(nb_jeunes_credited_last_12_months, nb_students) AS part_eleves_beneficiaires
FROM eple_infos
LEFT JOIN eple_bookings ON eple_bookings.institution_id = eple_infos.institution_id AND eple_infos.scholar_year = eple_bookings.scholar_year
LEFT JOIN total_nb_of_students ON eple_infos.institution_id = total_nb_of_students.institution_id
LEFT JOIN nb_eleves_educonnectes_per_eple ON eple_infos.institution_external_id = nb_eleves_educonnectes_per_eple.school
ORDER BY montant_depense_theorique DESC, nb_students DESC