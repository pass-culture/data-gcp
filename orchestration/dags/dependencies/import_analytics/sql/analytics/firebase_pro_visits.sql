WITH offerer_partner AS(
    SELECT 
    offerer_id 
    ,COUNT(distinct CASE WHEN partner_status = "venue" THEN partner_id ELSE NULL END) AS nb_partner_venue
FROM `{{ bigquery_analytics_dataset }}.enriched_cultural_partner_data` 
GROUP BY 1
)

SELECT
    session_id,
    CONCAT(session_id, user_pseudo_id) as unique_session_id,
    user_pseudo_id,
    ANY_VALUE(firebase_pro_events.user_id) AS user_id,
    user_offerer.offerer_id AS offerer_id,
    offerer.first_dms_adage_status AS first_dms_adage_status,
    offerer.dms_accepted_at AS adage_synchro_date,
    offerer.is_synchro_adage AS is_synchro_adage,
    offerer_partner.nb_partner_venue,
    ANY_VALUE(session_number) AS session_number,
    MIN(event_date) AS first_event_date,
    MIN(event_timestamp) AS first_event_timestamp,
    MAX(event_timestamp) AS last_event_timestamp,
    DATE_DIFF(MAX(event_timestamp),MIN(event_timestamp),SECOND) AS visit_duration_seconds,

-- count page view
    COUNTIF(event_name = "page_view" AND page_name = "Espace acteurs culturels - pass Culture Pro") AS nb_view_home_page,
    COUNTIF(event_name = "page_view" AND page_name = "Guichet - pass Culture Pro") AS nb_view_ticket_page,
    COUNTIF(event_name = "page_view" AND page_name = "Offres individuelles - pass Culture Pro") AS nb_view_individual_offer_page,
    COUNTIF(event_name = "page_view" AND page_name = "Offres collectives - pass Culture Pro") AS nb_collective_offer_page,
    COUNTIF(event_name = "page_view" AND page_name = "Réservations individuelles - pass Culture Pro") AS nb_view_individual_booking_page,
    COUNTIF(event_name = "page_view" AND page_name = "Réservations collectives - pass Culture Pro") AS nb_view_collective_booking_page,
    COUNTIF(event_name = "page_view" AND page_name IN ("Remboursements - pass Culture Pro","Gestion financière - pass Culture Pro")) AS nb_view_financial_receipt_page,
    COUNTIF(event_name = "page_view" AND page_name = "Détails - pass Culture Pro") AS nb_view_financial_details_page,
    COUNTIF(event_name = "page_view" AND page_name = "Informations bancaires - pass Culture Pro") AS nb_view_banking_info_page,
    COUNTIF(event_name = "page_view" AND page_name = "Statistiques - pass Culture Pro") AS nb_view_stat_page,

-- count offer creation
-- hub
    COUNTIF(event_name = "page_view" AND page_name="Selection du type d’offre - pass Culture Pro") AS nb_hub_for_offer_creation,
-- indiv
    COUNTIF(event_name = "page_view" AND page_name= "Création - Détail de l’offre - pass Culture Pro") AS nb_start_individual_offer_creation,
    COUNTIF(event_name = "page_view" AND page_name= "Confirmation - Offre individuelle publiée - pass Culture Pro") AS nb_confirmed_individual_offer_creation,
-- collectiv
    COUNTIF(event_name = "page_view" AND page_location LIKE "%/offre/creation/collectif/vitrine%" ) AS nb_start_collective_offer_template_creation,
    COUNTIF(event_name = "page_view" AND page_location LIKE "%collectif/vitrine/confirmation%") AS nb_confirmed_collective_offer_template_creation,
    COUNTIF(event_name = "page_view" AND (page_location LIKE "%/offre/creation/collectif?%" OR (page_location LIKE "%offre/collectif%" AND page_location LIKE "%creation?%"))) AS nb_start_collective_offer_bookable_creation, 
    COUNTIF(event_name = "page_view" AND page_location LIKE "%/collectif/confirmation%") AS nb_confirmed_collective_offer_bookable_creation, 

-- count offer edition 
-- indiv
    COUNTIF(event_name = "page_view" AND page_name = "Détails - Modifier une offre individuelle - pass Culture Pro") AS nb_start_individual_offer_details_edition,
    COUNTIF(event_name = "page_view" AND page_name = "Récapitulatif - Modifier une offre individuelle - pass Culture Pro" AND origin LIKE "%/offre/individuelle%" AND origin LIKE "%edition/informations%") AS nb_confirmed_individual_offer_details_edition,
    COUNTIF(event_name = "page_view" AND page_name = "Stocks et prix - Modifier une offre individuelle - pass Culture Pro") AS nb_start_individual_offer_stock_edition,
    COUNTIF(event_name = "page_view" AND page_name = "Stocks et prix - Consulter une offre individuelle - pass Culture Pro" AND origin LIKE "%/offre/individuelle%" AND origin LIKE "%/stocks%") AS nb_confirmed_individual_offer_stock_edition,
-- collectiv
    COUNTIF(event_name = "page_view" AND page_name = "Détails - Modifier une offre collective réservable - pass Culture Pro") AS nb_start_collective_offer_edition,
    COUNTIF(event_name = "page_view" AND page_name = "Récapitulatif - Modifier une offre collective réservable - pass Culture Pro" AND origin LIKE "%collectif/edition%") AS nb_confirmed_collective_offer_edition,

-- count venue creation
    COUNTIF(event_name IN ("hasClickedAddFirstVenueInOfferer","hasClickedAddVenueInOfferer","hasClickedCreateVenue")) AS nb_start_venue_creation,
    COUNTIF(event_name="hasClickedSaveVenue" AND is_edition!="true") AS nb_confirmed_venue_creation, 

-- count venue edition
    COUNTIF(event_name = "page_view" AND page_name = "Modifier un lieu - pass Culture Pro") AS nb_start_venue_edition, 
    COUNTIF(event_name="hasClickedSaveVenue" AND is_edition="true") AS nb_confirmed_venue_edition,  
    COUNTIF(event_name="hasClickedAddImage") AS nb_clic_add_image,     

-- count other CTA 
    COUNTIF(event_name="hasClickedAddBankAccount") AS nb_clic_add_bank_account,   
    COUNTIF(event_name="hasClickedInviteCollaborator") AS nb_clic_add_collaborator,  
    COUNTIF(event_name="hasSentInvitation") AS nb_clic_send_invitation,   
    COUNTIF(event_name = "page_view" AND page_name = "Profil - pass Culture Pro") AS nb_clic_modify_profil,  
    COUNTIF(event_name="hasClickedPartnerBlockPreviewVenueLink") AS nb_clic_partner_preview,
    COUNTIF(event_name="hasClickedPartnerBlockCopyVenueLink") AS nb_clic_copy_partner_link,
    COUNTIF(event_name="hasClickedPartnerBlockDmsApplicationLink") AS nb_clic_adage_synchro,
    COUNTIF(event_name="hasClickedPartnerBlockCollectiveHelpLink") AS nb_clic_collective_help,
    COUNTIF(event_name="hasClickedHelpCenter") AS nb_clic_help_center,
    COUNTIF(event_name="hasClickedBestPracticesAndStudies") AS nb_clic_best_practices,
    COUNTIF(event_name="hasClickedConsultSupport") AS nb_clic_consult_support,
    COUNTIF(event_name="hasClickedConsultCGU") AS nb_clic_consult_cgu,


FROM
        `{{ bigquery_analytics_dataset }}.firebase_pro_events` as firebase_pro_events
LEFT JOIN 
        `{{ bigquery_analytics_dataset }}.enriched_user_offerer` as user_offerer
        ON firebase_pro_events.user_id=user_offerer.user_id
LEFT JOIN 
        `{{ bigquery_analytics_dataset }}.enriched_offerer_data` as offerer
        ON user_offerer.offerer_id=offerer.offerer_id
LEFT JOIN 
        offerer_partner 
        ON offerer_partner.offerer_id=offerer.offerer_id
WHERE event_date = DATE("{{ ds }}")
GROUP BY
    session_id,
    user_pseudo_id,
    unique_session_id,
    user_offerer.offerer_id,
    offerer.first_dms_adage_status,
    offerer.dms_accepted_at,
    offerer.is_synchro_adage,
    offerer_partner.nb_partner_venue