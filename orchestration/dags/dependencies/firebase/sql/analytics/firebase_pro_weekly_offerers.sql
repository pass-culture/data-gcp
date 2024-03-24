WITH weeks AS (
    SELECT
        *
    FROM
        UNNEST(
            GENERATE_DATE_ARRAY('2018-01-01', CURRENT_DATE, INTERVAL 1 WEEK) 
        ) AS week
)

    SELECT
        user_offerer.offerer_id
        ,COUNT(distinct partner.partner_id) as nb_partner
        ,COUNT(distinct user_offerer.user_id) as nb_user
        ,date(offerer_creation_date) AS offerer_creation_date
        ,CASE WHEN offerer.dms_accepted_at IS NOT NULL THEN date(offerer.dms_accepted_at)
        WHEN offerer.dms_accepted_at IS NOT NULL AND is_synchro_adage IS TRUE THEN date(offerer_creation_date)
        ELSE NULL END AS adage_synchro_date
        ,weeks.week AS active_week
        ,DATE_DIFF(weeks.week,DATE_TRUNC(offerer_creation_date,WEEK(MONDAY)),WEEK) AS nb_weeks_since_offerer_created
        ,DATE_DIFF(CURRENT_DATE, offerer_creation_date, WEEK) AS offerer_seniority_weeks
        ,COUNT(distinct unique_session_id) as nb_session
        ,AVG(visit_duration_seconds) as avg_visit_duration_seconds
        ,SUM(visit_duration_seconds) as sum_visit_duration_seconds

-- offer management (modification and creation after hub)
        ,COUNT(distinct CASE WHEN nb_start_individual_offer_creation+nb_confirmed_individual_offer_creation+nb_start_collective_offer_template_creation+nb_confirmed_collective_offer_template_creation+nb_start_collective_offer_bookable_creation+nb_confirmed_collective_offer_bookable_creation+nb_start_individual_offer_details_edition+nb_confirmed_individual_offer_details_edition+nb_start_individual_offer_stock_edition+nb_confirmed_individual_offer_stock_edition>0 THEN unique_session_id ELSE NULL END) AS nb_session_manage_offer

-- guichet management
        ,COUNT(distinct CASE WHEN nb_view_ticket_page>0 THEN unique_session_id ELSE NULL END) AS nb_session_manage_ticket
-- booking management (see pages about booking)
        ,COUNT(distinct CASE WHEN nb_view_individual_booking_page+nb_view_collective_booking_page>0 THEN unique_session_id ELSE NULL END) AS nb_session_manage_booking
-- finance management (consult finance pages and add bank account)
        ,COUNT(distinct CASE WHEN nb_view_financial_receipt_page+nb_view_financial_details_page+nb_view_banking_info_page+nb_clic_add_bank_account>0 THEN unique_session_id ELSE NULL END) AS nb_session_manage_finance

-- venue management (create and modify venue)
        ,COUNT(distinct CASE WHEN nb_start_venue_creation+nb_confirmed_venue_creation+nb_start_venue_edition+nb_confirmed_venue_edition+nb_clic_add_image>0 THEN unique_session_id ELSE NULL END) AS nb_session_manage_venue
-- profil management (modify profil and invite user)
        ,COUNT(distinct CASE WHEN nb_clic_modify_profil+nb_clic_add_collaborator+nb_clic_send_invitation>0 THEN unique_session_id ELSE NULL END) AS nb_session_manage_profil

-- stat consultation
        ,COUNT(distinct CASE WHEN nb_view_stat_page>0 THEN unique_session_id ELSE NULL END) AS nb_session_consult_stat
-- help consultation
        ,COUNT(distinct CASE WHEN nb_clic_collective_help+nb_clic_help_center+nb_clic_best_practices+nb_clic_consult_support+nb_clic_consult_cgu>0 THEN unique_session_id ELSE NULL END) AS nb_session_consult_help

    FROM `{{ bigquery_analytics_dataset }}.enriched_user_offerer` user_offerer
        LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_offerer_data` offerer ON user_offerer.offerer_id=offerer.offerer_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_cultural_partner_data` partner ON user_offerer.offerer_id=partner.offerer_id
        JOIN weeks ON weeks.week >= offerer_creation_date
        LEFT JOIN `{{ bigquery_clean_dataset }}.firebase_pro_visits` firebase ON firebase.user_id=user_offerer.user_id 
            AND DATE_TRUNC(date(first_event_date),WEEK(MONDAY)) = weeks.week
            AND visit_duration_seconds>2
    GROUP BY 1,4,5,6,7,8