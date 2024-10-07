select
    event_date,
    case
        when event_name = "page_view" then concat("Page: ", page_name) else event_name
    end as event_name,
    timestamp(event_timestamp) as event_timestamp,
    user_id,
    user_pseudo_id,
    platform,
    struct(
        unique_session_id,
        origin,
        offer_id,
        offer_type,
        is_edition,
        is_draft,
        has_saved_query,
        has_opened_wrong_student_modal,
        filled,
        filled_with_errors,
        onboarding_selected_legal_category,
        download_format,
        download_booking_status,
        download_button_type,
        download_file_type,
        download_files_cnt
    ) as extra_params,
    struct(
        offerer_id,
        offerer_name,
        offerer_first_individual_offer_creation_date,
        offerer_first_collective_offer_creation_date,
        offerer_business_activity_label,
        offerer_legal_category_label,
        is_local_authority,
        total_permanent_managed_venues,
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
        total_partner_created_individual_offers,
        total_partner_created_collective_offers,
        user_device_category,
        user_device_operating_system,
        user_web_browser
    ) as user_params,
    "pro" as origin
from {{ ref("mrt_pro__event") }}
where
    (not regexp_contains(event_name, '^[a-z]+(_[a-z]+)*$') or event_name = "page_view")
    and user_pseudo_id is not null
