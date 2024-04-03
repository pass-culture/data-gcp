{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date"},
        on_schema_change = "sync_all_columns"
    )
}}

SELECT 
    e.event_name,
    e.page_name,
    e.user_pseudo_id,
    e.user_id,
    e.event_date,
    e.event_timestamp,
    e.session_number,
    e.session_id,
    e.unique_session_id,
    e.origin,
    e.destination,
    e.traffic_campaign,
    e.traffic_medium,
    e.traffic_source,
    e.platform,
    e.user_device_category,
    e.user_device_operating_system,
    e.user_device_operating_system_version,
    e.user_web_browser,
    e.user_web_browser_version,
    e.page_location,
    e.url_path_extract,
    e.page_referrer,
    e.page_number,
    e.is_edition,
    e.is_draft,
    COALESCE(e.offerer_id,v.venue_managing_offerer_id,ps.offerer_id) AS offerer_id,
    e.venue_id,
    e.offer_id,
    e.offer_type,
    e.used,
    e.saved,
    e.eac_wrong_student_modal_only6and5,
    e.filled,
    e.filled_with_errors,
    e.onboarding_selected_legal_category,
    COALESCE(o.offerer_name,v.offerer_name) AS offerer_name,
    o.offerer_first_individual_offer_creation_date,
    o.offerer_first_collective_offer_creation_date,
    o.legal_unit_business_activity_label as offerer_business_activity_label,
    o.legal_unit_legal_category_label as offerer_legal_category_label,
    o.is_local_authority,
    o.permanent_venues_managed,
    o.is_synchro_adage,
    o.dms_accepted_at,
    o.first_dms_adage_status,
    v.venue_name,
    v.venue_siret IS NOT NULL AS venue_has_siret,
    v.venue_is_permanent,
    v.venue_type_label,
    p.partner_id,
    p.partner_name,
    p.partner_type,
    p.cultural_sector as partner_cultural_sector,
    p.individual_offers_created as partner_nb_individual_offers,
    p.collective_offers_created as partner_nb_collective_offers
FROM {{ ref("int_firebase__pro_event") }} AS e
LEFT JOIN {{ ref("int_firebase__pro_session") }} ps ON ps.unique_session_id = e.unique_session_id
LEFT JOIN {{ ref("enriched_venue_data") }} AS v ON COALESCE(e.venue_id,ps.venue_id)=v.venue_id
LEFT JOIN {{ ref("enriched_offerer_data") }} AS o ON COALESCE(e.offerer_id,v.venue_managing_offerer_id,ps.offerer_id)=o.offerer_id
LEFT JOIN {{ ref("enriched_cultural_partner_data") }} AS p ON v.partner_id=p.partner_id
{% if is_incremental() %}
AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 2 DAY) and DATE("{{ ds() }}")
{% endif %}