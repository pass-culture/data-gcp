{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "first_event_date", "data_type": "date"},
        on_schema_change = "sync_all_columns"
    )
}}

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
    COUNT(DISTINCT uo.offerer_id) as nb_offerer,
    COUNT(DISTINCT p.partner_id) as nb_partner,

    -- offer management (modification and creation after hub)
    SUM(CASE WHEN nb_start_individual_offer_creation + nb_confirmed_individual_offer_creation + nb_start_collective_offer_template_creation + nb_confirmed_collective_offer_template_creation + nb_start_collective_offer_bookable_creation + nb_confirmed_collective_offer_bookable_creation + nb_start_individual_offer_details_edition + nb_confirmed_individual_offer_details_edition + nb_start_individual_offer_stock_edition + nb_confirmed_individual_offer_stock_edition > 0 THEN 1 ELSE 0 END) AS nb_manage_offer,

    -- guichet management
    SUM(CASE WHEN nb_view_ticket_page > 0 THEN 1 ELSE 0 END) AS nb_manage_ticket,
    -- booking management (see pages about booking)
    SUM(CASE WHEN nb_view_individual_booking_page + nb_view_collective_booking_page>0 THEN 1 ELSE 0 END) AS nb_manage_booking,
    -- finance management (consult finance pages and add bank account)
    SUM(CASE WHEN nb_view_financial_receipt_page+nb_view_financial_details_page + nb_view_banking_info_page + nb_clic_add_bank_account + nb_clic_add_venue_to_bank_account > 0 THEN 1 ELSE 0 END) AS nb_manage_finance,

    -- venue management (create and modify venue)
    SUM(CASE WHEN nb_start_venue_creation + nb_confirmed_venue_creation + nb_start_venue_edition + nb_confirmed_venue_edition + nb_clic_add_image > 0 THEN 1 ELSE 0 END) AS nb_manage_venue,
    -- profil management (modify profil and invite user)
    SUM(CASE WHEN nb_clic_modify_profil + nb_clic_add_collaborator + nb_clic_send_invitation > 0 THEN 1 ELSE 0 END) AS nb_manage_profil,

    -- stat consultation
    SUM(CASE WHEN nb_view_stat_page > 0 THEN 1 ELSE 0 END) AS nb_consult_stat,
    -- help consultation
    SUM(CASE WHEN nb_clic_collective_help + nb_clic_help_center + nb_clic_best_practices + nb_clic_consult_support + nb_clic_consult_cgu > 0 THEN 1 ELSE 0 END) AS nb_consult_help,
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
        