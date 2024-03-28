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
        COALESCE(e.offerer_id,u.offerer_id) as offerer_id
        e.venue_id,
        e.offer_id,
        e.offer_type,
        e.used,
        e.saved,
        e.eac_wrong_student_modal_only6and5,
        e.filled,
        e.filled_with_errors,
        e.onboarding_selected_legal_category,
        u.user_affiliation_rank,
        o.offerer_name,
        o.offerer_first_individual_offer_creation_date,
        o.offerer_first_collective_offer_creation_date,
        o.legal_unit_business_activity_label as offerer_business_activity_label,
        o.legal_unit_legal_category_label as offerer_legal_category_label,
        o.is_local_authority,
        o.permanent_venues_managed,
        o.is_synchro_adage,
        o.dms_accepted_at,
        o.first_dms_adage_status
FROM {{ ref("int_firebase__pro_event") }} AS e
LEFT JOIN {{ ref("enriched_user_offerer") }} AS u ON e.user_id=u.user_id
LEFT JOIN {{ ref("enriched_offerer_data") }} AS o ON u.offerer_id=o.offerer_id

{% if is_incremental() %}
AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 2 DAY) and DATE("{{ ds() }}")
{% endif %}