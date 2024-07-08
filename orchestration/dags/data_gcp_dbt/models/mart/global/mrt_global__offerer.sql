WITH siren_reference_adage AS (
  SELECT siren,
    max(siren_synchro_adage) AS siren_synchro_adage
  FROM {{ ref('adage')}}
  GROUP BY siren
),

first_dms_adage AS (
SELECT demandeur_entreprise_siren,
    application_status,
    processed_at
FROM {{ ref('dms_pro')}}
WHERE procedure_id IN ('57081', '57189','61589','65028','80264')
QUALIFY row_number() OVER(PARTITION BY demandeur_entreprise_siren ORDER BY application_submitted_at ASC) = 1
),

tagged_partners AS (
SELECT
    offerer_id,
    STRING_AGG(DISTINCT tag_label ORDER BY tag_label) AS partner_type
FROM {{ ref("mrt_global__offerer_tag") }}
WHERE tag_category_name = "comptage"
AND tag_label NOT IN ("Association", "EPN","Collectivité","Pas de tag associé","Auto-Entrepreneur","Compagnie","Tourneur")
GROUP BY offerer_id
)

SELECT
    ofr.offerer_id,
    ofr.partner_id,
    ofr.offerer_name,
    ofr.offerer_creation_date,
    ofr.offerer_validation_date,
    ofr.first_stock_creation_date,
    ofr.offerer_validation_status,
    ofr.offerer_is_active,
    ofr.first_individual_offer_creation_date,
    ofr.last_individual_offer_creation_date,
    ofr.first_collective_offer_creation_date,
    ofr.last_collective_offer_creation_date,
    ofr.first_offer_creation_date,
    ofr.last_offer_creation_date,
    ofr.first_individual_booking_date,
    ofr.last_individual_booking_date,
    ofr.first_bookable_offer_date,
    ofr.last_bookable_offer_date,
    ofr.first_individual_bookable_offer_date,
    ofr.last_individual_bookable_offer_date,
    ofr.first_collective_bookable_offer_date,
    ofr.last_collective_bookable_offer_date,
    ofr.first_booking_date,
    ofr.last_booking_date,
    ofr.total_non_cancelled_individual_bookings,
    ofr.total_non_cancelled_collective_bookings,
    ofr.total_non_cancelled_bookings,
    ofr.total_used_bookings,
    ofr.total_used_individual_bookings,
    ofr.total_used_collective_bookings,
    ofr.total_individual_theoretic_revenue,
    ofr.total_individual_real_revenue,
    ofr.total_collective_theoretic_revenue,
    ofr.total_collective_real_revenue,
    ofr.total_theoretic_revenue,
    ofr.total_real_revenue,
    ofr.total_current_year_real_revenue,
    ofr.first_collective_booking_date,
    ofr.last_collective_booking_date,
    ofr.total_created_individual_offers,
    ofr.total_created_collective_offers,
    ofr.total_created_offers,
    ofr.total_bookable_individual_offers,
    ofr.total_bookable_collective_offers,
    ofr.total_bookable_offers,
    ofr.offerer_department_code,
    ofr.offerer_postal_code,
    ofr.offerer_siren,
    ofr.is_active_last_30days,
    ofr.is_active_current_year,
    ofr.is_individual_active_last_30days,
    ofr.is_individual_active_current_year,
    ofr.is_collective_active_last_30days,
    ofr.is_collective_active_current_year,
    region_department.region_name AS offerer_region_name,
    region_department.academy_name,
    siren_data.activitePrincipaleUniteLegale AS legal_unit_business_activity_code,
    label_unite_legale AS legal_unit_business_activity_label,
    siren_data.categorieJuridiqueUniteLegale AS legal_unit_legal_category_code,
    label_categorie_juridique AS legal_unit_legal_category_label,
    siren_data.activitePrincipaleUniteLegale = '84.11Z' AS is_local_authority,
    ofr.total_managed_venues,
    ofr.total_physical_managed_venues,
    ofr.total_permanent_managed_venues,
    ofr.total_venues,
    ofr.offerer_humanized_id,
    COALESCE(first_dms_adage.application_status, 'dms_adage_non_depose') AS first_dms_adage_status,
    first_dms_adage_accepted.processed_at AS dms_accepted_at,
    siren_reference_adage.siren IS NOT NULL AS is_reference_adage,
    CASE WHEN siren_reference_adage.siren IS NULL THEN FALSE ELSE siren_synchro_adage END AS is_synchro_adage,
    tagged_partners.partner_type,
FROM {{ ref('int_applicative__offerer') }} AS ofr
LEFT JOIN {{ source('analytics', 'region_department') }} AS region_department ON ofr.offerer_department_code = region_department.num_dep
LEFT JOIN {{ source('clean', 'siren_data') }} AS siren_data ON siren_data.siren = ofr.offerer_siren
LEFT JOIN {{ source('analytics', 'siren_data_labels') }} AS siren_data_labels ON siren_data_labels.activitePrincipaleUniteLegale = siren_data.activitePrincipaleUniteLegale
                                            AND CAST(siren_data_labels.categorieJuridiqueUniteLegale AS STRING) = CAST(siren_data.categorieJuridiqueUniteLegale AS STRING)
LEFT JOIN first_dms_adage ON first_dms_adage.demandeur_entreprise_siren = ofr.offerer_siren
LEFT JOIN first_dms_adage AS first_dms_adage_accepted ON
    first_dms_adage_accepted.demandeur_entreprise_siren = ofr.offerer_siren
    AND first_dms_adage_accepted.application_status = "accepte"
LEFT JOIN siren_reference_adage ON ofr.offerer_siren = siren_reference_adage.siren
LEFT JOIN tagged_partners ON ofr.offerer_id = tagged_partners.offerer_id
WHERE ofr.offerer_validation_status='VALIDATED'
    AND ofr.offerer_is_active
QUALIFY ROW_NUMBER() OVER (PARTITION BY ofr.offerer_siren ORDER BY update_date DESC) = 1
