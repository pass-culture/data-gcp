WITH previous_export AS (
    SELECT 
        DISTINCT email 
    FROM  `{{ bigquery_clean_dataset }}.qualtrics_ac`
    WHERE calculation_month >= DATE_SUB(DATE("{{ current_month(ds) }}"), INTERVAL 1 MONTH)


),lieux_physique AS (
    SELECT
        enriched_venue_data.venue_id,
        enriched_venue_data.venue_siret,
        venue_booking_email as email,
        venue_name,
        venue_type_label,
        DATE_DIFF(current_date, venue_creation_date, DAY) AS anciennete_en_jours,
        non_cancelled_bookings,
        individual_offers_created,
        collective_offers_created,
        individual_offers_created + collective_offers_created AS offers_created,
        theoretic_revenue,
        venue_is_permanent,
        venue_region_name,
        enriched_venue_data.venue_department_code,
        geo_type,
        venue_is_virtual,
        DATE_DIFF(CURRENT_DATE, last_booking_date, DAY) AS nb_jours_depuis_derniere_resa
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_venue_data` enriched_venue_data
        LEFT JOIN `{{ bigquery_analytics_dataset }}.venue_locations` venue_locations ON venue_locations.venue_id = enriched_venue_data.venue_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}.rural_city_type_data` rural_city_type_data ON CAST(rural_city_type_data.geo_code AS string) = CAST(venue_locations.codgeo AS string)
        LEFT JOIN `{{ bigquery_raw_dataset }}.qualtrics_opt_out_users` opt_out on opt_out.ext_ref = enriched_venue_data.venue_id
    WHERE
        NOT venue_is_virtual AND opt_out.contact_id IS NULL
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17
   
),

generate_export AS (
    SELECT 
        lp.*

    FROM lieux_physique lp
    LEFT JOIN previous_export pe on pe.email = lp.email
    WHERE pe.email is null
    ORDER BY
        RAND()
    LIMIT
        {{ params.volume }}
),

cnt_appli_dms as (SELECT 
  demandeur_siret
  , application_status
  , count(distinct application_id) as cnt_applications
FROM `{{ bigquery_clean_dataset }}.dms_pro`
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
FROM `{{ bigquery_clean_dataset }}.dms_pro`
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

SELECT
    DATE("{{ current_month(ds) }}") as calculation_month,
    CURRENT_DATE as export_date,
    *
FROM
    generate_export
LEFT JOIN dms_infos
ON generate_export.venue_siret = dms_infos.demandeur_siret