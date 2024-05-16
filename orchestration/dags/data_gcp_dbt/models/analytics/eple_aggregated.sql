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
    ,eat.macro_institution_type
    ,ey.scholar_year
    ,ed.educational_deposit_amount AS institution_deposit_amount
FROM {{ ref('enriched_institution_data') }} eid
LEFT JOIN {{ source('analytics','region_department') }} rd ON eid.institution_departement_code = rd.num_dep
JOIN {{ ref('educational_deposit') }} ed ON ed.educational_institution_id = eid.institution_id
JOIN {{ ref('educational_year') }} ey ON ey.adage_id = ed.educational_year_id
LEFT JOIN  {{ source('raw','eple_aggregated_type') }} as eat
        ON eid.institution_type = eat.institution_type
), 

eple_bookings AS (
SELECT 
    eple_infos.institution_id
    ,eple_infos.scholar_year
    ,SUM(CASE WHEN collective_booking_status != 'CANCELLED' THEN booking_amount ELSE NULL END) AS theoric_amount_spent
    ,SUM(CASE WHEN collective_booking_status IN ('USED','REIMBURSED') THEN booking_amount ELSE NULL END) AS real_amount_spent
FROM eple_infos
JOIN {{ ref('enriched_collective_booking_data') }} ecbd ON ecbd.educational_institution_id = eple_infos.institution_id AND ecbd.scholar_year = eple_infos.scholar_year
GROUP BY 1, 2)

,total_nb_of_students AS (
SELECT 
    eid.institution_id 
    ,eid.institution_name 
    ,SUM(number_of_students) AS nb_students
FROM {{ ref('enriched_institution_data') }} eid
LEFT JOIN {{ source('analytics','number_of_students_per_eple') }} ns ON eid.institution_external_id = ns.institution_external_id
GROUP BY 1,2)

,nb_eleves_educonnectes_per_eple AS (
SELECT  
     TRIM(json_extract(result_content, '$.school_uai'), '"') AS school 
    , COUNT(DISTINCT edd.user_id) AS educonnect_inscriptions
    , COUNT(DISTINCT CASE WHEN DATE_DIFF(current_date, deposit_creation_date, DAY) <= 365 THEN edd.user_id ELSE NULL END) AS last_12_months_inscriptions
    , AVG(COALESCE(deposit_theoretical_amount_spent,0)) AS avg_spent_per_user
    , SAFE_DIVIDE(SUM(deposit_theoretical_amount_spent), SUM(deposit_amount)) AS pct_spent
    , COUNT(DISTINCT ebd.user_id) AS nb_credit_used_students
FROM {{ ref('beneficiary_fraud_check') }} bfc
JOIN {{ ref('enriched_deposit_data') }} edd ON edd.user_id = bfc.user_id
LEFT JOIN {{ ref('mrt_global__booking') }} ebd ON ebd.user_id = edd.user_id AND not booking_is_cancelled
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
    ,eple_infos.macro_institution_type
    ,eple_infos.scholar_year
    ,institution_deposit_amount
    ,theoric_amount_spent
    ,SAFE_DIVIDE(theoric_amount_spent, institution_deposit_amount) AS pct_credit_theoric_amount_spent
    ,real_amount_spent
    ,SAFE_DIVIDE(real_amount_spent, institution_deposit_amount) AS pct_credit_real_amount_spent
    ,nb_students AS total_students
    ,educonnect_inscriptions
    ,last_12_months_inscriptions
    ,nb_credit_used_students
    ,avg_spent_per_user
    ,pct_spent AS pct_spent_per_user
    ,SAFE_DIVIDE(last_12_months_inscriptions, nb_students) AS pct_beneficiary_students
FROM eple_infos
LEFT JOIN eple_bookings ON eple_bookings.institution_id = eple_infos.institution_id AND eple_infos.scholar_year = eple_bookings.scholar_year
LEFT JOIN total_nb_of_students ON eple_infos.institution_id = total_nb_of_students.institution_id
LEFT JOIN nb_eleves_educonnectes_per_eple ON eple_infos.institution_external_id = nb_eleves_educonnectes_per_eple.school
