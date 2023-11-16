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

-- page view
    COUNTIF(event_name = "page_view" AND page_name = "Guichet - pass Culture Pro") AS nb_view_ticket_page,
    COUNTIF(event_name = "page_view" AND page_name IN ("Offres individuelles - pass Culture Pro","Offres collectives - pass Culture Pro")) AS nb_view_offer_page,
    COUNTIF(event_name = "page_view" AND page_name IN ("Réservations individuelles - pass Culture Pro","Réservations collectives - pass Culture Pro")) AS nb_view_booking_page,
    COUNTIF(event_name = "page_view" AND page_name= "Remboursements - pass Culture Pro") AS nb_view_reimbursment_page,
    COUNTIF(event_name = "page_view" AND page_name= "Statistiques - pass Culture Pro") AS nb_view_stat_page,

-- offer creation
    COUNTIF(event_name = "page_view" AND page_name="Selection du type d’offre - pass Culture Pro") AS nb_hub_for_offer_creation,
    COUNTIF(event_name = "page_view" AND page_name= "Création - Détail de l’offre - pass Culture Pro") AS nb_start_individual_offer_creation,
    COUNTIF(event_name = "page_view" AND page_name= "Confirmation - Offre individuelle publiée - pass Culture Pro") AS nb_confirmed_individual_offer_creation,
    COUNTIF(event_name = "page_view" AND page_name= "Détails - Créer une offre collective vitrine - pass Culture Pro") AS nb_start_collective_offer_template_creation,
    COUNTIF(event_name = "page_view" AND page_name= "Confirmation - Offre collective vitrine publiée - pass Culture Pro") AS nb_confirmed_collective_offer_template_creation,
    COUNTIF(event_name = "page_view" AND page_name= "Détails - Créer une offre réservable - pass Culture Pro") AS nb_start_collective_offer_bookable_creation, 
    COUNTIF(event_name = "page_view" AND page_name= "Confirmation - Offre réservable publiée - pass Culture Pro") AS nb_start_collective_offer_bookable_creation, 

-- offer edition (next update : confirmed modification, not possible at this stage)
    COUNTIF(event_name = "page_view" AND page_name IN ("Récapitulatif - Modifier une offre individuelle - pass Culture Pro","Edition d’une offre collective - pass Culture Pro")) AS nb_start_offer_edition,

-- venue creation
    COUNTIF(event_name IN ('hasClickedAddFirstVenueInOfferer','hasClickedAddVenueInOfferer','hasClickedCreateVenue')) AS nb_start_venue_creation,
    COUNTIF(event_name='hasClickedSaveVenue' AND is_edition!="true") AS nb_confirmed_venue_creation, 

-- venue edition
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