{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "first_event_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

select
    fpv.user_id,
    fpv.user_pseudo_id,
    fpv.session_id,
    fpv.unique_session_id,
    fpv.session_number,
    fpv.first_event_date,
    fpv.first_event_timestamp,
    fpv.last_event_timestamp,
    fpv.visit_duration_seconds,
    count(distinct uo.offerer_id) as total_offerers,
    count(distinct p.partner_id) as total_partners,

    -- offer management (modification and creation after hub)
    sum(
        case
            when
                total_started_created_individual_offers
                + total_confirmed_created_individual_offers
                + total_started_created_template_collective_offers
                + total_confirmed_created_template_collective_offers
                + total_started_created_bookable_collective_offers
                + total_confirmed_created_bookable_collective_offers
                + total_started_edited_individual_offers
                + total_confirmed_edited_individual_offers
                + total_started_edited_collective_offers
                + total_confirmed_edited_collective_offers
                > 0
            then 1
            else 0
        end
    ) as total_managed_offers,

    -- individual offer consultation (one offer page, not catalog page)
    sum(case when total_individual_offer_page_views>0 then 1 else 0 end ) as total_consulted_offers,

    -- guichet management
    sum(
        case when total_ticket_page_views > 0 then 1 else 0 end
    ) as total_managed_tickets,
    -- booking management (see pages about booking)
    sum(
        case
            when
                total_individual_booking_page_views
                + total_collective_booking_page_views
                > 0
            then 1
            else 0
        end
    ) as total_managed_bookings,
    -- booking downloads
    sum(
        case
            when
                total_download_booking_clicks
                > 0
            then 1
            else 0
        end
    ) as total_booking_downloads,
    -- finance management (consult finance pages and add bank account)
    sum(
        case
            when
                total_financial_receipt_page_views
                + total_financial_details_page_views
                + total_banking_info_page_views
                + total_add_bank_account_clicks
                + total_add_venue_to_bank_account_clicks
                > 0
            then 1
            else 0
        end
    ) as total_managed_finance,

    -- venue management (create and modify venue)
    sum(
        case
            when
                total_started_created_venues
                + total_confirmed_created_venues
                + total_started_edited_venues
                + total_confirmed_edited_venues
                + total_save_image_clicks
                > 0
            then 1
            else 0
        end
    ) as total_managed_venues,
    -- profil management (modify profil and invite user)
    sum(
        case
            when
                total_modify_profile_clicks
                + total_add_collaborator_clicks
                + total_send_invitation_clicks
                > 0
            then 1
            else 0
        end
    ) as total_managed_profiles,

    -- stat consultation
    sum(case when total_stat_page_views > 0 then 1 else 0 end) as total_stat_page_views,
    -- help consultation
    sum(
        case
            when
                total_collective_help_clicks
                + total_help_center_clicks
                + total_best_practices_clicks
                + total_consult_support_clicks
                + total_consult_cgu_clicks
                + total_contact_our_team
                + total_new_evolutions_clicks
                > 0
            then 1
            else 0
        end
    ) as total_consulted_help
from {{ ref("int_firebase__pro_visit") }} as fpv
left join {{ ref("mrt_global__user_offerer") }} as uo on fpv.user_id = uo.user_id
left join {{ ref("mrt_global__user_offerer") }} as o on uo.offerer_id = o.offerer_id
left join {{ ref("mrt_global__cultural_partner") }} as p on uo.offerer_id = p.offerer_id
where
    true
    {% if is_incremental() %}
        and first_event_date
        between date_sub(date("{{ ds() }}"), interval 3 day) and date("{{ ds() }}")
    {% endif %}
group by
    user_id,
    user_pseudo_id,
    session_id,
    unique_session_id,
    session_number,
    first_event_date,
    first_event_timestamp,
    last_event_timestamp,
    visit_duration_seconds
