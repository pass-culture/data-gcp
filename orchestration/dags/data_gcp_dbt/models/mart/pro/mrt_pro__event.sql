{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

with
    most_active_offerer_per_user as (
        select distinct uo.user_id, uo.offerer_id
        from {{ ref("mrt_global__user_offerer") }} as uo
        left join {{ ref("mrt_global__offerer") }} as o on uo.offerer_id = o.offerer_id
        qualify
            row_number() over (
                partition by user_id order by total_non_cancelled_bookings desc
            )
            = 1
    ),

    offerer_per_session as (
        select
            unique_session_id,
            coalesce(ps.offerer_id, v.offerer_id, mau.offerer_id) as offerer_id
        from {{ ref("int_firebase__pro_session") }} as ps
        left join {{ ref("mrt_global__venue") }} as v on ps.venue_id = v.venue_id
        left join most_active_offerer_per_user as mau on mau.user_id = ps.user_id
    )

select
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
    coalesce(e.offerer_id, v.offerer_id, ps.offerer_id) as offerer_id,
    e.venue_id,
    e.offer_id,
    e.offer_type,
    e.has_saved_query,
    e.has_opened_wrong_student_modal,
    e.filled,
    e.filled_with_errors,
    e.onboarding_selected_legal_category,
    e.download_format,
    e.download_booking_status,
    e.url_path_agg,
    e.download_button_type,
    e.download_file_type,
    e.download_files_cnt,
    e.offer_subcategory_id,
    e.suggested_offer_subcategory_selected,
    e.image_creation_stage,
    e.headline_offer_action_type,
    coalesce(o.offerer_name, v.offerer_name) as offerer_name,
    o.first_individual_offer_creation_date
    as offerer_first_individual_offer_creation_date,
    o.first_collective_offer_creation_date
    as offerer_first_collective_offer_creation_date,
    o.legal_unit_business_activity_label as offerer_business_activity_label,
    o.legal_unit_legal_category_label as offerer_legal_category_label,
    o.is_local_authority,
    o.total_permanent_managed_venues,
    o.is_synchro_adage,
    o.dms_accepted_at,
    o.first_dms_adage_status,
    v.venue_name,
    v.venue_siret is not null as venue_has_siret,
    v.venue_is_permanent,
    v.venue_type_label,
    p.partner_id,
    p.partner_name,
    p.partner_type,
    p.cultural_sector as partner_cultural_sector,
    p.total_created_individual_offers as total_partner_created_individual_offers,
    p.total_created_collective_offers as total_partner_created_collective_offers
from {{ ref("int_firebase__pro_event") }} as e
left join offerer_per_session as ps on ps.unique_session_id = e.unique_session_id
left join {{ ref("mrt_global__venue") }} as v on e.venue_id = v.venue_id
left join
    {{ ref("mrt_global__offerer") }} as o
    on coalesce(e.offerer_id, v.offerer_id, ps.offerer_id) = o.offerer_id
left join {{ ref("mrt_global__cultural_partner") }} as p on v.partner_id = p.partner_id
where
    true
    {% if is_incremental() %}
        and event_date
        between date_sub(date("{{ ds() }}"), interval 2 day) and date("{{ ds() }}")
    {% endif %}
