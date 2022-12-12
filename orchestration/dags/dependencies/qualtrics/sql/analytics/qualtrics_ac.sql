WITH cnt_appli_dms as (SELECT 
  demandeur_siret
  , application_status
  , count(distinct application_id) as cnt_applications
FROM `{{ bigquery_analytics_dataset }}.dms_pro`
WHERE demandeur_siret != 'nan'
GROUP BY 
  demandeur_siret
  , application_status
),

cnt_appli_dms_detailed as (SELECT
  demandeur_siret
  , coalesce(cnt_applications_accepte, 0) as cnt_applications_accepte
  , coalesce(cnt_applications_refuse, 0 ) as cnt_applications_refuse
  , coalesce(cnt_applications_sans_suite, 0) as cnt_applications_sans_suite
  , coalesce(cnt_applications_en_construction, 0) as cnt_applications_en_construction
  , coalesce(cnt_applications_en_instruction, 0) as cnt_applications_en_instruction
FROM cnt_appli_dms
PIVOT(sum(cnt_applications) as cnt_applications FOR application_status IN ('accepte', 'refuse', 'sans_suite', 'en_construction', 'en_instruction'))
),

last_application_dms as (
  SELECT 
  demandeur_siret
  , application_status as last_application_status
  , application_submitted_at as last_application_submitted_at
  , processed_at as last_application_processed_at
FROM `{{ bigquery_analytics_dataset }}.dms_pro`
qualify row_number() over(partition by demandeur_siret order by application_submitted_at desc) = 1
), 

dms_infos as (
SELECT 
  cnt_appli_dms_detailed.demandeur_siret
  , cnt_applications_accepte + cnt_applications_refuse + cnt_applications_sans_suite + cnt_applications_en_construction + cnt_applications_en_instruction as cnt_applications 
  , cnt_applications_accepte
  , cnt_applications_refuse
  , cnt_applications_sans_suite
  , cnt_applications_en_construction
  , cnt_applications_en_instruction
  , last_application_status
  , last_application_submitted_at
  , last_application_processed_at
FROM cnt_appli_dms_detailed
LEFT JOIN last_application_dms
ON cnt_appli_dms_detailed.demandeur_siret = last_application_dms.demandeur_siret
)

SELECT *
FROM `{{ bigquery_clean_dataset }}.qualtrics_ac` qac
LEFT JOIN dms_infos
ON qac.venue_siret = dms_infos.demandeur_siret