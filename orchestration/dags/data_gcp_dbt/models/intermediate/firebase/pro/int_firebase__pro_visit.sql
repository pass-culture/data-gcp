{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "first_event_date", "data_type": "date"},
        on_schema_change = "sync_all_columns",
        alias = "firebase_pro_visits"
    )
}}

WITH filtered_events AS(
SELECT
        session_id,
        unique_session_id,
        session_number,
        user_id,
        user_pseudo_id,
        event_name,
        page_name,
        event_timestamp,
        event_date,
        page_location,
        origin,
        is_edition
    FROM {{ ref('int_firebase__pro_event') }}
    WHERE TRUE
        {% if is_incremental() %}
        AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 3+1 DAY) and DATE("{{ ds() }}")
        {% endif %}

)
        
SELECT
    session_id,
    unique_session_id,
    user_pseudo_id,
    ANY_VALUE(user_id) AS user_id,
    ANY_VALUE(session_number) AS session_number,
    MIN(event_date) AS first_event_date,
    MIN(event_timestamp) AS first_event_timestamp,
    MAX(event_timestamp) AS last_event_timestamp,
    DATE_DIFF(MAX(event_timestamp),MIN(event_timestamp),SECOND) AS visit_duration_seconds,

-- count page view
    COUNTIF(event_name = "page_view" AND page_name = "Espace acteurs culturels - pass Culture Pro") AS total_home_page_views,
    COUNTIF(event_name = "page_view" AND page_name = "Guichet - pass Culture Pro") AS total_ticket_page_views,
    COUNTIF(event_name = "page_view" AND page_name = "Offres individuelles - pass Culture Pro") AS total_individual_offer_page_views,
    COUNTIF(event_name = "page_view" AND page_name = "Offres collectives - pass Culture Pro") AS total_collective_offer_pages,
    COUNTIF(event_name = "page_view" AND page_name = "Réservations individuelles - pass Culture Pro") AS total_individual_booking_page_views,
    COUNTIF(event_name = "page_view" AND page_name = "Réservations collectives - pass Culture Pro") AS total_collective_booking_page_views,
    COUNTIF(event_name = "page_view" AND page_name IN ("Remboursements - pass Culture Pro","Gestion financière - pass Culture Pro")) AS total_financial_receipt_page_views,
    COUNTIF(event_name = "page_view" AND page_name = "Détails - pass Culture Pro") AS total_financial_details_page_views,
    COUNTIF(event_name = "page_view" AND page_name = "Informations bancaires - pass Culture Pro") AS total_banking_info_page_views,
    COUNTIF(event_name = "page_view" AND page_name = "Statistiques - pass Culture Pro") AS total_stat_page_views,

-- count offer creation
-- hub
    COUNTIF(event_name = "page_view" AND page_name IN ("Selection du type d’offre - pass Culture Pro","Choix de la nature de l'offre - Créer une offre - pass Culture Pro")) AS total_hub_for_offer_creation,
-- indiv
    COUNTIF(event_name = "page_view" AND page_name IN ("Création - Détail de l’offre - pass Culture Pro","Détails - Créer une offre individuelle - pass Culture Pro")) AS total_started_individual_offers,
    COUNTIF(event_name = "page_view" AND page_name= "Confirmation - Offre individuelle publiée - pass Culture Pro") AS total_confirmed_individual_offers,
-- collectiv
    COUNTIF(event_name = "page_view" AND page_location LIKE "%/offre/creation/collectif/vitrine%" ) AS total_started_collective_offer_template,
    COUNTIF(event_name = "page_view" AND page_location LIKE "%collectif/vitrine/confirmation%") AS total_confirmed_collective_offer_template,
    COUNTIF(event_name = "page_view" AND (page_location LIKE "%/offre/creation/collectif?%" OR (page_location LIKE "%offre/collectif%" AND page_location LIKE "%creation?%"))) AS total_started_bookable_collective_offers,
    COUNTIF(event_name = "page_view" AND page_location LIKE "%/collectif/confirmation%") AS total_confirmed_bookable_collective_offers,

-- count offer edition 
-- indiv
    COUNTIF(event_name = "page_view" AND page_name = "Détails - Modifier une offre individuelle - pass Culture Pro") AS total_detail_edition_started_individual_offers,
    COUNTIF(event_name = "page_view" AND page_name = "Récapitulatif - Modifier une offre individuelle - pass Culture Pro" AND origin LIKE "%/offre/individuelle%" AND origin LIKE "%edition/informations%") AS total_detail_edition_confirmed_individual_offers,
    COUNTIF(event_name = "page_view" AND page_name = "Stocks et prix - Modifier une offre individuelle - pass Culture Pro") AS total_stock_edition_started_individual_offers,
    COUNTIF(event_name = "page_view" AND page_name = "Stocks et prix - Consulter une offre individuelle - pass Culture Pro" AND origin LIKE "%/offre/individuelle%" AND origin LIKE "%/stocks%") AS total_stock_edition_confirmed_individual_offers,
-- collectiv
    COUNTIF(event_name = "page_view" AND page_name = "Détails - Modifier une offre collective réservable - pass Culture Pro") AS total_edition_started_collective_offers,
    COUNTIF(event_name = "page_view" AND page_name = "Récapitulatif - Modifier une offre collective réservable - pass Culture Pro" AND origin LIKE "%collectif/edition%") AS total_confirmed_collective_offer_edition,

-- count venue creation
    COUNTIF(event_name IN ("hasClickedAddFirstVenueInOfferer","hasClickedAddVenueInOfferer","hasClickedCreateVenue")) AS total_started_venue_creation, -- total_created_venue ?
    COUNTIF(event_name = "hasClickedSaveVenue" AND is_edition != "true") AS total_confirmed_venue_creation, -- total confirmed_venue ?

-- count venue edition
    COUNTIF(event_name = "page_view" AND (page_name = "Modifier un lieu - pass Culture Pro" OR (page_name = "Modifier ma page partenaire - pass Culture Pro" AND origin LIKE "%lieux%") OR (page_name IN ("Gérer ma page sur l’application - pass Culture Pro","Gérer ma page sur ADAGE - pass Culture Pro") AND page_location LIKE "%edition"))) AS total_started_venue_edition,
    COUNTIF(event_name = "hasClickedSaveVenue" AND is_edition = "true") AS total_confirmed_venue_edition,
    COUNTIF(event_name = "hasClickedAddImage") AS total_add_image_clicks,

-- count other CTA 
    COUNTIF(event_name IN ("hasClickedAddBankAccount","hasClickedContinueToDS","hasClickedBankDetailsRecordFollowUp")) AS total_add_bank_account_clicks,
    COUNTIF(event_name IN ("hasClickedAddVenueToBankAccount","hasClickedSaveVenueToBankAccount","hasClickedChangeVenueToBankAccount")) AS total_add_venue_to_bank_account_clicks,
    COUNTIF(event_name="hasClickedInviteCollaborator") AS total_add_collaborator_clicks,
    COUNTIF(event_name="hasSentInvitation") AS total_send_invitation_clicks,
    COUNTIF(event_name = "page_view" AND page_name = "Profil - pass Culture Pro") AS total_modify_profile_clicks,
    COUNTIF(event_name= "hasClickedPartnerBlockPreviewVenueLink") AS total_partner_preview_clicks,
    COUNTIF(event_name= "hasClickedPartnerBlockCopyVenueLink") AS total_copy_partner_link_clicks,
    COUNTIF(event_name= "hasClickedPartnerBlockDmsApplicationLink") AS total_adage_synchro_clicks,
    COUNTIF(event_name= "hasClickedPartnerBlockCollectiveHelpLink") AS total_collective_help_clicks,
    COUNTIF(event_name= "hasClickedHelpCenter") AS total_help_center_clicks,
    COUNTIF(event_name= "hasClickedBestPracticesAndStudies") AS total_best_practices_clicks,
    COUNTIF(event_name= "hasClickedConsultSupport") AS total_consult_support_clicks,
    COUNTIF(event_name= "hasClickedConsultCGU") AS total_consult_cgu_clicks,

FROM filtered_events
WHERE TRUE
        {% if is_incremental() %}
        AND event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 3 DAY) and DATE("{{ ds() }}")
        {% endif %}
GROUP BY
    session_id,
    user_pseudo_id,
    unique_session_id