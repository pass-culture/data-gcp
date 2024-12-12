with
    siren_reference_adage as (
        select siren, max(siren_synchro_adage) as siren_synchro_adage
        from {{ ref("adage") }}
        group by siren
    ),

    dms_adage as (
        select
            demandeur_entreprise_siren,
            application_status,
            processed_at,
            row_number() over (
                partition by demandeur_entreprise_siren
                order by application_submitted_at asc
            ) as rown_siren,
        from {{ ref("dms_pro") }}
        where procedure_id in ('57081', '57189', '61589', '65028', '80264')
    ),

    first_dms_adage_accepted as (
        select *
        from dms_adage
        where application_status = "accepte"
        qualify
            row_number() over (
                partition by demandeur_entreprise_siren order by processed_at asc
            )
            = 1
    ),

    tagged_partners as (
        select
            offerer_id,
            string_agg(distinct tag_label order by tag_label) as partner_type
        from {{ ref("int_applicative__offerer_tag") }}
        where
            tag_category_name = "comptage"
            and tag_label not in (
                "Association",
                "EPN",
                "Collectivité",
                "Pas de tag associé",
                "Auto-Entrepreneur",
                "Compagnie",
                "Tourneur"
            )
        group by offerer_id
    ),

    reimbursement_points as (
        select offerer_id, count(distinct bank_account_id) as total_reimbursement_points
        from {{ source("raw", "applicative_database_bank_account") }}
        where is_active
        group by offerer_id
    )

select
    ofr.offerer_id,
    ofr.partner_id,
    ofr.offerer_name,
    ofr.offerer_creation_date,
    ofr.offerer_validation_date,
    ofr.first_stock_creation_date,
    ofr.offerer_validation_status,
    ofr.offerer_is_active,
    ofr.first_individual_offer_creation_date,
    ofr.last_individual_offer_creation_date,
    ofr.first_collective_offer_creation_date,
    ofr.last_collective_offer_creation_date,
    ofr.first_offer_creation_date,
    ofr.last_offer_creation_date,
    ofr.first_individual_booking_date,
    ofr.last_individual_booking_date,
    ofr.first_bookable_offer_date,
    ofr.last_bookable_offer_date,
    ofr.first_individual_bookable_offer_date,
    ofr.last_individual_bookable_offer_date,
    ofr.first_collective_bookable_offer_date,
    ofr.last_collective_bookable_offer_date,
    ofr.first_booking_date,
    ofr.last_booking_date,
    ofr.total_non_cancelled_individual_bookings,
    ofr.total_non_cancelled_collective_bookings,
    ofr.total_non_cancelled_bookings,
    ofr.total_used_bookings,
    ofr.total_used_individual_bookings,
    ofr.total_used_collective_bookings,
    ofr.total_individual_theoretic_revenue,
    ofr.total_individual_real_revenue,
    ofr.total_collective_theoretic_revenue,
    ofr.total_collective_real_revenue,
    ofr.total_theoretic_revenue,
    ofr.total_real_revenue,
    ofr.total_current_year_real_revenue,
    ofr.first_collective_booking_date,
    ofr.last_collective_booking_date,
    ofr.total_created_individual_offers,
    ofr.total_created_collective_offers,
    ofr.total_created_offers,
    ofr.total_bookable_individual_offers,
    ofr.total_bookable_collective_offers,
    ofr.total_bookable_offers,
    ofr.offerer_department_code,
    ofr.offerer_postal_code,
    ofr.offerer_siren,
    ofr.is_active_last_30days,
    ofr.is_active_current_year,
    ofr.is_individual_active_last_30days,
    ofr.is_individual_active_current_year,
    ofr.is_collective_active_last_30days,
    ofr.is_collective_active_current_year,
    ofr.top_real_revenue_venue_type,
    ofr.top_bookings_venue_type,
    region_department.region_name as offerer_region_name,
    ofr.offerer_city,
    region_department.academy_name,
    siren_data.activiteprincipaleunitelegale as legal_unit_business_activity_code,
    label_unite_legale as legal_unit_business_activity_label,
    siren_data.categoriejuridiqueunitelegale as legal_unit_legal_category_code,
    label_categorie_juridique as legal_unit_legal_category_label,
    case
        when siren_data.activiteprincipaleunitelegale = '84.11Z' then true else false
    end as is_local_authority,
    case
        when
            (
                lower(ofr.offerer_name) like "commune%"
                or lower(ofr.offerer_name) like "%ville%de%"
            )
        then "Communes"
        when
            (
                lower(ofr.offerer_name) like "%departement%"
                or lower(ofr.offerer_name) like "%département%"
            )
        then "Départements"
        when
            (
                lower(ofr.offerer_name) like "region%"
                or lower(ofr.offerer_name) like "région%"
            )
        then "Régions"
        when
            (
                lower(ofr.offerer_name) like "ca%"
                or lower(ofr.offerer_name) like "%agglo%"
                or lower(ofr.offerer_name) like "cc%"
                or lower(ofr.offerer_name) like "cu%"
                or lower(ofr.offerer_name) like "%communaute%"
                or lower(ofr.offerer_name) like "%agglomeration%"
                or lower(ofr.offerer_name) like "%agglomération%"
                or lower(ofr.offerer_name) like "%metropole%"
                or lower(ofr.offerer_name) like "%com%com%"
                or lower(ofr.offerer_name) like "%petr%"
                or lower(ofr.offerer_name) like "%intercommunal%"
            )
        then "CC / Agglomérations / Métropoles"
        else "Non qualifiable"
    end as local_authority_type,
    case
        when
            ofr.offerer_id in (
                select priority_offerer_id
                from {{ source("seed", "priority_local_authorities") }}
            )
        then true
        else false
    end as local_authority_is_priority,
    ofr.total_managed_venues,
    ofr.total_physical_managed_venues,
    ofr.total_permanent_managed_venues,
    ofr.all_physical_venues_types,
    ofr.total_administrative_venues,
    ofr.total_venues,
    ofr.offerer_humanized_id,
    coalesce(
        first_dms_adage.application_status, 'dms_adage_non_depose'
    ) as first_dms_adage_status,
    first_dms_adage_accepted.processed_at as dms_accepted_at,
    siren_reference_adage.siren is not null as is_reference_adage,
    case
        when siren_reference_adage.siren is null then false else siren_synchro_adage
    end as is_synchro_adage,
    tagged_partners.partner_type,
    rp.total_reimbursement_points
from {{ ref("int_applicative__offerer") }} as ofr
left join
    {{ source("seed", "region_department") }} as region_department
    on ofr.offerer_department_code = region_department.num_dep
left join
    {{ source("clean", "siren_data") }} as siren_data
    on siren_data.siren = ofr.offerer_siren
left join
    {{ source("seed", "siren_data_labels") }} as siren_data_labels
    on siren_data_labels.activiteprincipaleunitelegale
    = siren_data.activiteprincipaleunitelegale
    and cast(siren_data_labels.categoriejuridiqueunitelegale as string)
    = cast(siren_data.categoriejuridiqueunitelegale as string)
left join
    dms_adage as first_dms_adage
    on first_dms_adage.demandeur_entreprise_siren = ofr.offerer_siren
    and first_dms_adage.rown_siren = 1
left join
    first_dms_adage_accepted
    on first_dms_adage_accepted.demandeur_entreprise_siren = ofr.offerer_siren
left join siren_reference_adage on ofr.offerer_siren = siren_reference_adage.siren
left join tagged_partners on ofr.offerer_id = tagged_partners.offerer_id
left join reimbursement_points as rp on rp.offerer_id = ofr.offerer_id
qualify
    row_number() over (partition by offerer_siren order by siren_data.update_date desc)
    = 1
