SELECT
    event_date,
    CASE WHEN event_name="page_view" THEN CONCAT(event_name,page_name) ELSE event_name END as event_name
    event_timestamp,
    user_id,
    user_pseudo_id,
    unique_session_id,
    origin,
    platform,
    user_device_category,
    user_device_operating_system,
    user_web_browser,
    STRUCT(    
        offer_id,
        offer_type,
        is_edition,
        is_draft,
        used,
        saved,
        eac_wrong_student_modal_only6and5,
        filled,
        filled_with_errors,
        onboarding_selected_legal_category
    ) as extra_params,
    STRUCT(
        offerer_id
        offerer_name,
        offerer_first_individual_offer_creation_date,
        offerer_first_collective_offer_creation_date,
        offerer_business_activity_label,
        offerer_legal_category_label,
        is_local_authority,
        permanent_venues_managed,
        is_synchro_adage,
        dms_accepted_at,
        first_dms_adage_status,
        venue_id,
        venue_name,
        venue_has_siret,
        venue_is_permanent,
        venue_type_label,
        partner_id,
        partner_name,
        partner_type,
        partner_cultural_sector,
        partner_nb_individual_offers,
        partner_nb_collective_offers
    ) as user_params,
    "pro" as origin
FROM {{ ref("mrt_global__firebase_pro_event") }}
WHERE MOD(ABS(FARM_FINGERPRINT(user_pseudo_id)),10) = 0