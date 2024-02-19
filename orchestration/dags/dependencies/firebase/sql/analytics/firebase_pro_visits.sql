SELECT 
        clean_firebase.user_id 
        ,clean_firebase.user_pseudo_id 
        ,clean_firebase.session_id
        ,clean_firebase.unique_session_id
        ,clean_firebase.session_number
        ,clean_firebase.first_event_date
        ,clean_firebase.first_event_timestamp
        ,clean_firebase.last_event_timestamp
        ,clean_firebase.visit_duration_seconds
        ,COUNT(distinct user_offerer.offerer_id) as nb_offerer
        ,COUNT(distinct partner.partner_id) as nb_partner

-- offer management (modification and creation after hub)
        ,SUM(CASE WHEN nb_start_individual_offer_creation+nb_confirmed_individual_offer_creation+nb_start_collective_offer_template_creation+nb_confirmed_collective_offer_template_creation+nb_start_collective_offer_bookable_creation+nb_confirmed_collective_offer_bookable_creation+nb_start_individual_offer_details_edition+nb_confirmed_individual_offer_details_edition+nb_start_individual_offer_stock_edition+nb_confirmed_individual_offer_stock_edition>0 THEN 1 ELSE 0 END) AS nb_manage_offer

-- guichet management
        ,SUM(CASE WHEN nb_view_ticket_page>0 THEN 1 ELSE 0 END) AS nb_manage_ticket
-- booking management (see pages about booking)
        ,SUM(CASE WHEN nb_view_individual_booking_page+nb_view_collective_booking_page>0 THEN 1 ELSE 0 END) AS nb_manage_booking
-- finance management (consult finance pages and add bank account)
        ,SUM(CASE WHEN nb_view_financial_receipt_page+nb_view_financial_details_page+nb_view_banking_info_page+nb_clic_add_bank_account>0 THEN 1 ELSE 0 END) AS nb_manage_finance

-- venue management (create and modify venue)
        ,SUM(CASE WHEN nb_start_venue_creation+nb_confirmed_venue_creation+nb_start_venue_edition+nb_confirmed_venue_edition+nb_clic_add_image>0 THEN 1 ELSE 0 END) AS nb_manage_venue
-- profil management (modify profil and invite user)
        ,SUM(CASE WHEN nb_clic_modify_profil+nb_clic_add_collaborator+nb_clic_send_invitation>0 THEN 1 ELSE 0 END) AS nb_manage_profil

-- stat consultation
        ,SUM(CASE WHEN nb_view_stat_page>0 THEN 1 ELSE 0 END) AS nb_consult_stat
-- help consultation
        ,SUM(CASE WHEN nb_clic_collective_help+nb_clic_help_center+nb_clic_best_practices+nb_clic_consult_support+nb_clic_consult_cgu>0 THEN 1 ELSE 0 END) AS nb_consult_help

FROM 
 `{{ bigquery_clean_dataset }}.firebase_pro_visits` clean_firebase
LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_user_offerer` user_offerer ON clean_firebase.user_id=user_offerer.user_id
LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_offerer_data` offerer ON user_offerer.offerer_id=offerer.offerer_id
LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_cultural_partner_data` partner ON user_offerer.offerer_id=partner.offerer_id
{% if params.dag_type == 'intraday' %}
        where clean_firebase.first_event_date = PARSE_DATE('%Y-%m-%d','{{ ds }}')
{% else %}
        where clean_firebase.first_event_date = DATE_SUB(PARSE_DATE('%Y-%m-%d','{{ ds }}'),INTERVAL 1 DAY)
{% endif %}
GROUP BY 1,2,3,4,5,6,7,8,9
        