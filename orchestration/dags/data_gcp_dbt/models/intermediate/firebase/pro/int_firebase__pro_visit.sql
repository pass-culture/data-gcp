{{
    config(
        alias="firebase_pro_visits",
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "first_event_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

with
    filtered_events as (
        select
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
        from {{ ref("int_firebase__pro_event") }}
        where
            true
            {% if is_incremental() %}
                and event_date
                between date_sub(date("{{ ds() }}"), interval 3 + 1 day) and date(
                    "{{ ds() }}"
                )
            {% endif %}

    )

select
    session_id,
    unique_session_id,
    user_pseudo_id,
    any_value(user_id) as user_id,
    any_value(session_number) as session_number,
    min(event_date) as first_event_date,
    min(event_timestamp) as first_event_timestamp,
    max(event_timestamp) as last_event_timestamp,
    date_diff(
        max(event_timestamp), min(event_timestamp), second
    ) as visit_duration_seconds,

    -- count page view
    countif(
        event_name = "page_view"
        and page_name = "Espace acteurs culturels - pass Culture Pro"
    ) as total_home_page_views,
    countif(
        event_name = "page_view" and page_name = "Guichet - pass Culture Pro"
    ) as total_ticket_page_views,
    countif(
        event_name = "page_view"
        and page_name = "Offres individuelles - pass Culture Pro"
    ) as total_individual_catalog_page_views,
    countif(
        event_name = "page_view"
        and page_name like "%Consulter une offre individuelle - pass Culture Pro"
    ) as total_individual_offer_page_views,
    countif(
        event_name = "page_view" and page_name = "Offres collectives - pass Culture Pro"
    ) as total_collective_offer_pages,
    countif(
        event_name = "page_view"
        and page_name = "Réservations individuelles - pass Culture Pro"
    ) as total_individual_booking_page_views,
    countif(
        event_name = "page_view"
        and page_name = "Réservations collectives - pass Culture Pro"
    ) as total_collective_booking_page_views,
    countif(
        event_name = "page_view"
        and page_name in (
            "Remboursements - pass Culture Pro", "Gestion financière - pass Culture Pro","Gestion financière - justificatifs - pass Culture Pro"
        )
    ) as total_financial_receipt_page_views,
    countif(
        event_name = "page_view" and page_name = "Détails - pass Culture Pro"
    ) as total_financial_details_page_views,
    countif(
        event_name = "page_view"
        and page_name = "Informations bancaires - pass Culture Pro"
    ) as total_banking_info_page_views,
    countif(
        event_name = "page_view"
        and page_name
        in ("Statistiques - pass Culture Pro", "Chiffre d’affaires - pass Culture Pro")
    ) as total_stat_page_views,

    -- count offer creation
    -- hub
    countif(
        event_name = "page_view"
        and page_name in (
            "Selection du type d’offre - pass Culture Pro",
            "Choix de la nature de l'offre - Créer une offre - pass Culture Pro"
        )
    ) as total_offer_creation_hubs,
    -- indiv
    countif(
        event_name = "page_view"
        and page_name in (
            "Création - Détail de l’offre - pass Culture Pro",
            "Détails - Créer une offre individuelle - pass Culture Pro",
            "Détails de l’offre - Créer une offre individuelle - pass Culture Pro",
            "Description - Créer une offre individuelle - pass Culture Pro"
        )
    ) as total_started_created_individual_offers,
    countif(
        event_name = "page_view"
        and page_name = "Confirmation - Offre individuelle publiée - pass Culture Pro"
    ) as total_confirmed_created_individual_offers,
    -- collectiv
    countif(
        event_name = "page_view"
        and (page_location like "%/offre/creation/collectif/vitrine%" or page_name="Détails - Créer une offre collective vitrine - pass Culture Pro")
    ) as total_started_created_template_collective_offers,
    countif(
        event_name = "page_view"
        and (page_location like "%collectif/vitrine/confirmation%" or page_name="Confirmation - Offre collective vitrine publiée - pass Culture Pro")
    ) as total_confirmed_created_template_collective_offers,
    countif(
        event_name = "page_view"
        and (
            page_location like "%/offre/creation/collectif?%"
            or page_name="Détails - Créer une offre réservable - pass Culture Pro"
            or (
                page_location like "%offre/collectif%"
                and page_location like "%creation?%"
            )
        )
    ) as total_started_created_bookable_collective_offers,
    countif(
        event_name = "page_view" and (page_location like "%/collectif/confirmation%" or page_name="Confirmation - Offre réservable publiée - pass Culture Pro")
    ) as total_confirmed_created_bookable_collective_offers,

    -- count offer edition
    -- indiv
    countif(
        event_name = "page_view"
        and page_name like "%Modifier une offre individuelle - pass Culture Pro"
    ) as total_started_edited_individual_offers,

    countif(
        event_name = "page_view"
        and page_name
        like "%Modifier une offre individuelle - pass Culture Pro"
        and origin like "%/offre/individuelle%"
        and origin like "%edition%"
    ) as total_confirmed_edited_individual_offers,

    -- collectiv
    countif(
        event_name = "page_view"
        and page_name
        = "Détails - Modifier une offre collective réservable - pass Culture Pro"
    ) as total_started_edited_collective_offers,
    countif(
        event_name = "page_view"
        and page_name
        = "Récapitulatif - Modifier une offre collective réservable - pass Culture Pro"
        and origin like "%collectif/edition%"
    ) as total_confirmed_edited_collective_offers,


    -- count venue creation
    countif(
        event_name in (
            "hasClickedAddFirstVenueInOfferer",
            "hasClickedAddVenueInOfferer",
            "hasClickedCreateVenue"
        )
    ) as total_started_created_venues,
    countif(
        event_name = "hasClickedSaveVenue" and is_edition != "true"
    ) as total_confirmed_created_venues,  -- total confirmed_venue ?

    -- count venue edition
    countif(
        event_name = "page_view"
        and (
            page_name = "Modifier un lieu - pass Culture Pro"
            or (
                page_name = "Modifier ma page partenaire - pass Culture Pro"
                and origin like "%lieux%"
            )
            or (
                page_name in (
                    "Gérer ma page sur l’application - pass Culture Pro",
                    "Gérer ma page sur ADAGE - pass Culture Pro"
                )
                and page_location like "%edition"
            )
            or (page_name = "Paramètres généraux - pass Culture Pro")
        )
    ) as total_started_edited_venues,
    countif(
        event_name = "hasClickedSaveVenue" and is_edition = "true"
    ) as total_confirmed_edited_venues,
    countif(event_name = "hasClickedSaveImage" AND page_name IN ("Gérer ma page sur l’application - pass Culture Pro","Gérer ma page sur ADAGE - pass Culture Pro")) as total_save_image_clicks,

    -- count other CTA
    countif(
        event_name in (
            "hasClickedAddBankAccount",
            "hasClickedContinueToDS",
            "hasClickedBankDetailsRecordFollowUp"
        )
    ) as total_add_bank_account_clicks,
    countif(
        event_name in (
            "hasClickedAddVenueToBankAccount",
            "hasClickedSaveVenueToBankAccount",
            "hasClickedChangeVenueToBankAccount"
        )
    ) as total_add_venue_to_bank_account_clicks,
    countif(
        event_name IN ("hasClickedInviteCollaborator","hasClickedAddCollaborator")
    ) as total_add_collaborator_clicks,
    countif(event_name = "hasSentInvitation") as total_send_invitation_clicks,
    countif(
        event_name = "page_view" and page_name = "Profil - pass Culture Pro"
    ) as total_modify_profile_clicks,
    countif(
        event_name = "hasClickedPartnerBlockPreviewVenueLink"
    ) as total_partner_preview_clicks,
    countif(
        event_name = "hasClickedPartnerBlockCopyVenueLink"
    ) as total_copy_partner_link_clicks,
    countif(
        event_name = "hasClickedPartnerBlockDmsApplicationLink"
    ) as total_adage_synchro_clicks,
    countif(
        event_name = "hasClickedPartnerBlockCollectiveHelpLink"
    ) as total_collective_help_clicks,
    countif(
        event_name in ("hasClickedHelpCenter", "hasClickedConsultHelp")
    ) as total_help_center_clicks,
    countif(
        event_name = "hasClickedBestPracticesAndStudies"
    ) as total_best_practices_clicks,
    countif(event_name = "hasClickedConsultSupport") as total_consult_support_clicks,
    countif(event_name = "hasClickedConsultCGU") as total_consult_cgu_clicks,
    countif(event_name = "hasClickedContactOurTeams") as total_contact_our_team,
    countif(event_name = "hasClickedNewEvolutions") as total_new_evolutions_clicks,
    countif(event_name like "hasClickedDownloadBooking%") as total_download_booking_clicks

from filtered_events
where
    true
    {% if is_incremental() %}
        and event_date
        between date_sub(date("{{ ds() }}"), interval 3 day) and date("{{ ds() }}")
    {% endif %}
group by session_id, user_pseudo_id, unique_session_id
