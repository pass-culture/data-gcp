SELECT
    session_id,
    CONCAT(session_id, user_pseudo_id) as unique_session_id,
    user_pseudo_id,
    ANY_VALUE(firebase_pro_events.user_id) AS user_id,
    user_offerer.offerer_id AS offerer_id,
    ANY_VALUE(session_number) AS session_number,
    MIN(event_timestamp) AS first_event_timestamp,
    MAX(event_timestamp) AS last_event_timestamp,
    DATE_DIFF(MAX(event_timestamp),MIN(event_timestamp),SECOND) AS visit_duration_seconds,

-- count page view
    COUNTIF(event_name = "page_view" AND page_name = "Guichet - pass Culture Pro") AS nb_view_ticket_page,
    COUNTIF(event_name = "page_view" AND page_name IN ("Offres individuelles - pass Culture Pro","Offres collectives - pass Culture Pro")) AS nb_view_offer_page,
    COUNTIF(event_name = "page_view" AND page_name IN ("Réservations individuelles - pass Culture Pro","Réservations collectives - pass Culture Pro")) AS nb_view_booking_page,
    COUNTIF(event_name = "page_view" AND page_name= "Remboursements - pass Culture Pro") AS nb_view_reimbursment_page,
    COUNTIF(event_name = "page_view" AND page_name= "Statistiques - pass Culture Pro") AS nb_view_stat_page,
    COUNTIF(event_name = "page_view" AND page_name = "Stocks et prix - Modifier une offre individuelle - pass Culture Pro" AND origin LIKE '%/offre/individuelle%' AND origin LIKE '%/stocks%' THEN 1 ELSE 0 END) as nb_go_edition_stock_indiv,
    COUNTIF(event_name = "page_view" AND page_name = "Récapitulatif - Modifier une offre individuelle - pass Culture Pro" AND origin LIKE '%/offre/individuelle%' AND origin LIKE '%/recapitulatif%' THEN 1 ELSE 0 END) as nb_go_edition_offer_indiv,
    COUNTIF(event_name = "page_view" AND page_name = "Edition d’une offre collective - pass Culture Pro" AND origin LIKE '%collectif/recapitulatif%' THEN 1 ELSE 0 END) as nb_go_edition_offer_collectiv,

-- count offer creation
-- hub
    COUNTIF(event_name = "page_view" AND page_name="Selection du type d’offre - pass Culture Pro") AS nb_hub_for_offer_creation,
-- indiv
    COUNTIF(event_name = "page_view" AND page_name= "Création - Détail de l’offre - pass Culture Pro") AS nb_start_individual_offer_creation,
    COUNTIF(event_name = "page_view" AND page_name= "Confirmation - Offre individuelle publiée - pass Culture Pro") AS nb_confirmed_individual_offer_creation,
-- collectiv
    COUNTIF(event_name = "page_view" AND page_name= "Détails - Créer une offre collective vitrine - pass Culture Pro") AS nb_start_collective_offer_template_creation,
    COUNTIF(event_name = "page_view" AND page_name= "Confirmation - Offre collective vitrine publiée - pass Culture Pro") AS nb_confirmed_collective_offer_template_creation,
    COUNTIF(event_name = "page_view" AND page_name= "Détails - Créer une offre réservable - pass Culture Pro") AS nb_start_collective_offer_bookable_creation, 
    COUNTIF(event_name = "page_view" AND page_name= "Confirmation - Offre réservable publiée - pass Culture Pro") AS nb_confirmed_collective_offer_bookable_creation, 

-- timestamp offer creation, each stage
-- hub
    MIN(CASE WHEN event_name = "page_view" AND page_name="Selection du type d’offre - pass Culture Pro" THEN event_timestamp ELSE NULL END) AS ts_hub_for_offer_creation,
-- indiv
    MIN(CASE WHEN event_name = "page_view" AND page_name= "Création - Détail de l’offre - pass Culture Pro" THEN event_timestamp ELSE NULL END) AS ts_individual_offer_details,
    MIN(CASE WHEN event_name = "page_view" AND page_name= "Stocks et prix - Créer une offre individuelle - pass Culture Pro" THEN event_timestamp ELSE NULL END) AS ts_individual_offer_stock,
    MIN(CASE WHEN event_name = "page_view" AND page_name= "Récapitulatif - Créer une offre individuelle - pass Culture Pro" THEN event_timestamp ELSE NULL END) AS ts_individual_offer_recap,
    MIN(CASE WHEN event_name = "page_view" AND page_name= "Confirmation - Offre individuelle publiée - pass Culture Pro" THEN event_timestamp ELSE NULL END) AS ts_individual_offer_confirmation,
-- collectiv
    MIN(CASE WHEN event_name = "page_view" AND page_name= "Détails - Créer une offre collective vitrine - pass Culture Pro" THEN event_timestamp ELSE NULL END) AS ts_collective_offer_template_details,
    MIN(CASE WHEN event_name = "page_view" AND page_name= "Confirmation - Offre collective vitrine publiée - pass Culture Pro" THEN event_timestamp ELSE NULL END) AS ts_collective_offer_template_confirmation,
    MIN(CASE WHEN event_name = "page_view" AND page_name= "Détails - Créer une offre réservable - pass Culture Pro" THEN event_timestamp ELSE NULL END) AS ts_collective_offer_bookable_details, 
    MIN(CASE WHEN event_name = "page_view" AND page_name= "Date et prix - Créer une offre réservable - pass Culture Pro" THEN event_timestamp ELSE NULL END) AS ts_collective_offer_bookable_price_and_date, 
    MIN(CASE WHEN event_name = "page_view" AND page_name= "Visibilité - Créer une offre réservable - pass Culture Pro" THEN event_timestamp ELSE NULL END) AS ts_collective_offer_bookable_visibility, 
    MIN(CASE WHEN event_name = "page_view" AND page_name= "Récapitulatif - Créer une offre réservable - pass Culture Pro" THEN event_timestamp ELSE NULL END) AS ts_collective_offer_bookable_recap, 
    MIN(CASE WHEN event_name = "page_view" AND page_name= "Confirmation - Offre réservable publiée - pass Culture Pro" THEN event_timestamp ELSE NULL END) AS ts_collective_offer_bookable_confirmation, 

-- count offer edition 
-- indiv
    COUNTIF(event_name = "page_view" AND page_name = "Détails - Modifier une offre individuelle - pass Culture Pro") AS nb_start_individual_offer_details_edition,
    COUNTIF(event_name = "page_view" AND page_name = "Récapitulatif - Modifier une offre individuelle - pass Culture Pro" AND origin LIKE '%/offre/individuelle%' AND origin LIKE '%edition/informations%') AS nb_confirmed_individual_offer_details_edition,
    COUNTIF(event_name = "page_view" AND page_name = "Stocks et prix - Modifier une offre individuelle - pass Culture Pro") AS nb_start_individual_offer_stock_edition,
    COUNTIF(event_name = "page_view" AND page_name = "Stocks et prix - Consulter une offre individuelle - pass Culture Pro" AND origin LIKE '%/offre/individuelle%' AND origin LIKE '%/stocks%') AS nb_confirmed_individual_offer_stock_edition,
-- collectiv
    COUNTIF(event_name = "page_view" AND page_name = "Détails - Modifier une offre réservable - pass Culture Pro") AS nb_start_collective_offer_details_edition,
    COUNTIF(event_name = "page_view" AND page_name = "Edition d’une offre collective - pass Culture Pro" AND origin LIKE '%collectif/edition%') AS nb_confirmed_collective_offer_details_edition,

-- count venue creation
    COUNTIF(event_name IN ('hasClickedAddFirstVenueInOfferer','hasClickedAddVenueInOfferer','hasClickedCreateVenue')) AS nb_start_venue_creation,
    COUNTIF(event_name='hasClickedSaveVenue' AND is_edition!="true") AS nb_confirmed_venue_creation, 

-- count venue edition
    COUNTIF(event_name = "page_view" AND page_name = "Modifier un lieu - pass Culture Pro") AS nb_start_venue_edition, 
    COUNTIF(event_name='hasClickedSaveVenue' AND is_edition="true") AS nb_confirmed_venue_edition,   

FROM
        `{{ bigquery_analytics_dataset }}.firebase_pro_events` as firebase_pro_events
LEFT JOIN 
        `{{ bigquery_analytics_dataset }}.enriched_user_offerer` as user_offerer
        ON firebase_pro_events.user_id=user_offerer.user_id
GROUP BY
    session_id,
    user_pseudo_id,
    unique_session_id,
    user_offerer.offerer_id