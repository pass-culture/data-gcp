{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "first_event_date", "data_type": "date"},
        on_schema_change = "sync_all_columns"
    )
}}

-- check où c'est censé aller cette colonne total_confirmed_collective_offer_edition (nouveau nom : total_confirmed_edited_collective_offers)

SELECT
    fpv.user_id,
    fpv.user_pseudo_id,
    fpv.session_id,
    fpv.unique_session_id,
    fpv.session_number,
    fpv.first_event_date,
    fpv.first_event_timestamp,
    fpv.last_event_timestamp,
    fpv.visit_duration_seconds,
    COUNT(DISTINCT uo.offerer_id) as total_offerers,
    COUNT(DISTINCT p.partner_id) as total_partners,

    -- offer management (modification and creation after hub)
    SUM(CASE WHEN total_started_individual_offers + total_confirmed_individual_offers + total_started_created_template_collective_offers + total_confirmed_created_template_collective_offers + total_started_created_bookable_collective_offers + total_confirmed_created_bookable_collective_offers + total_detailed_edited_started_individual_offers + total_confirmed_detail_edited_individual_offers + total_stock_edition_started_individual_offers + total_confirmed_stock_edited_individual_offers > 0 THEN 1 ELSE 0 END) AS total_managed_offers,

    -- guichet management
    SUM(CASE WHEN total_ticket_page_views > 0 THEN 1 ELSE 0 END) AS total_managed_tickets,
    -- booking management (see pages about booking)
    SUM(CASE WHEN total_individual_booking_page_views + total_collective_booking_page_views > 0 THEN 1 ELSE 0 END) AS total_managed_bookings,
    -- finance management (consult finance pages and add bank account)
    SUM(CASE WHEN total_financial_receipt_page_views + total_financial_details_page_views + total_banking_info_page_views + total_add_bank_account_clicks + total_add_venue_to_bank_account_clicks > 0 THEN 1 ELSE 0 END) AS total_managed_finance,

    -- venue management (create and modify venue)
    SUM(CASE WHEN total_started_created_venues + total_confirmed_created_venues + total_started_edited_venues + total_confirmed_edited_venues + total_add_image_clicks > 0 THEN 1 ELSE 0 END) AS total_managed_venues,
    -- profil management (modify profil and invite user)
    SUM(CASE WHEN total_modify_profile_clicks + total_add_collaborator_clicks + total_send_invitation_clicks > 0 THEN 1 ELSE 0 END) AS total_managed_profiles,

    -- stat consultation
    SUM(CASE WHEN total_stat_page_views > 0 THEN 1 ELSE 0 END) AS total_stat_page_views,
    -- help consultation
    SUM(CASE WHEN total_collective_help_clicks + total_help_center_clicks + total_help_center_clicks + total_consult_support_clicks + total_consult_cgu_clicks > 0 THEN 1 ELSE 0 END) AS total_consulted_help,
FROM {{ ref('int_firebase__pro_visit') }} AS fpv
LEFT JOIN {{ ref('enriched_user_offerer') }} AS uo ON fpv.user_id = uo.user_id
LEFT JOIN {{ ref('enriched_user_offerer') }} AS o ON uo.offerer_id = o.offerer_id
LEFT JOIN {{ ref('enriched_cultural_partner_data') }} AS p ON uo.offerer_id = p.offerer_id
WHERE TRUE
    {% if is_incremental() %}
    AND first_event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 3 DAY) and DATE("{{ ds() }}")
    {% endif %}
GROUP BY user_id,
        user_pseudo_id,
        session_id,
        unique_session_id,
        session_number,
        first_event_date,
        first_event_timestamp,
        last_event_timestamp,
        visit_duration_seconds
        