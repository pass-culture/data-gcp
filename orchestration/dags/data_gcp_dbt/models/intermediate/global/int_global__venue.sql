with
    bookable_offer_history as (
        select
            venue_id,
            min(partition_date) as first_bookable_offer_date,
            max(partition_date) as last_bookable_offer_date,
            min(
                case when total_individual_bookable_offers > 0 then partition_date end
            ) as first_individual_bookable_offer_date,
            max(
                case when total_individual_bookable_offers > 0 then partition_date end
            ) as last_individual_bookable_offer_date,
            min(
                case when total_collective_bookable_offers > 0 then partition_date end
            ) as first_collective_bookable_offer_date,
            max(
                case when total_collective_bookable_offers > 0 then partition_date end
            ) as last_collective_bookable_offer_date
        from {{ ref("int_history__bookable_venue") }}
        group by venue_id
    ),

    venue_tags as (
        select
            vc.venue_id,
            string_agg(case when cc.criterion_category_label="Comptage Réseaux pro" then c.name else null end," - ") as venue_network_tags,
            string_agg(case when cc.criterion_category_label="Comptage partenaire label et appellation du MC" then c.name else null end," - ") as venue_label_tags,
            string_agg(case when cc.criterion_category_label="Comptage partenaire sectoriel" then c.name else null end," - ") as venue_sector_tags
        from {{ source("raw", "applicative_database_venue_criterion") }} as vc
        inner join
            {{ source("raw", "applicative_database_criterion_category_mapping") }} as ccm
            on ccm.criterion_id = vc.criterion_id
        inner join
            {{ source("raw", "applicative_database_criterion_category") }} as cc
            on cc.criterion_category_id = ccm.criterion_category_id
            and cc.criterion_category_label IN ("Comptage Réseaux pro","Comptage partenaire label et appellation du MC","Comptage partenaire sectoriel")
        inner join
            {{ source("raw", "applicative_database_criterion") }} as c
            on ccm.criterion_id = c.id
        group by 1 
    )

select
    v.venue_id,
    v.venue_name,
    v.venue_public_name,
    v.venue_booking_email,
    v.venue_street,
    v.venue_latitude,
    v.venue_longitude,
    v.venue_department_code,
    v.venue_department_name,
    v.venue_postal_code,
    v.venue_city,
    v.venue_siret,
    v.venue_is_virtual,
    v.venue_managing_offerer_id as offerer_id,
    v.venue_creation_date,
    v.venue_is_permanent,
    v.venue_is_open_to_public,
    v.venue_is_acessibility_synched,
    v.venue_type_label,
    v.venue_label,
    v.venue_humanized_id,
    v.venue_backoffice_link,
    v.venue_region_name,
    v.venue_epci,
    v.venue_academy_name,
    v.venue_in_qpv,
    v.venue_in_zrr,
    v.venue_rural_city_type,
    v.venue_density_label,
    v.venue_macro_density_label,
    v.venue_density_level,
    v.venue_targeted_audience,
    v.banner_url,
    v.venue_description,
    v.venue_withdrawal_details,
    v.venue_contact_phone_number,
    v.venue_contact_email,
    v.venue_contact_website,
    v.total_individual_bookings,
    v.total_collective_bookings,
    v.total_bookings,
    v.total_non_cancelled_individual_bookings,
    v.total_non_cancelled_collective_bookings,
    v.first_individual_booking_date,
    v.last_individual_booking_date,
    v.first_collective_booking_date,
    v.last_collective_booking_date,
    v.total_non_cancelled_bookings,
    v.total_used_bookings,
    v.total_used_individual_bookings,
    v.total_used_collective_bookings,
    v.total_individual_theoretic_revenue,
    v.total_individual_real_revenue,
    v.total_collective_theoretic_revenue,
    v.total_collective_real_revenue,
    v.total_theoretic_revenue,
    v.total_real_revenue,
    v.first_individual_offer_creation_date,
    v.last_individual_offer_creation_date,
    v.total_created_individual_offers,
    v.first_collective_offer_creation_date,
    v.last_collective_offer_creation_date,
    v.total_created_collective_offers,
    v.total_created_offers,
    boh.first_bookable_offer_date,
    boh.last_bookable_offer_date,
    boh.first_individual_bookable_offer_date,
    boh.last_individual_bookable_offer_date,
    boh.first_collective_bookable_offer_date,
    boh.last_collective_bookable_offer_date,
    v.first_booking_date,
    v.last_booking_date,
    v.first_offer_creation_date,
    v.last_offer_creation_date,
    v.total_bookable_individual_offers,
    v.total_bookable_collective_offers,
    v.total_bookable_offers,
    v.venue_iris_internal_id,
    v.total_non_cancelled_tickets,
    v.total_current_year_non_cancelled_tickets,
    v.offerer_address_id,
    v.venue_image_source,
    v.total_distinct_headline_offers,
    v.has_headline_offer,
    v.first_headline_offer_date,
    v.last_headline_offer_date,
    v.venue_adage_inscription_date,
    ofr.offerer_name,
    ofr.offerer_validation_status,
    ofr.offerer_is_active,
    ofr.dms_accepted_at,
    ofr.first_dms_adage_status,
    ofr.is_reference_adage,
    ofr.is_synchro_adage,
    ofr.total_reimbursement_points,
    ofr.is_local_authority,
    coalesce(
        date_diff(current_date, boh.last_bookable_offer_date, day) <= 30, false
    ) as is_active_last_30days,
    coalesce(
        date_diff(current_date, boh.last_bookable_offer_date, year) = 0, false
    ) as is_active_current_year,
    coalesce(
        date_diff(current_date, boh.last_individual_bookable_offer_date, day) <= 30,
        false
    ) as is_individual_active_last_30days,
    coalesce(
        date_diff(current_date, boh.last_individual_bookable_offer_date, year) = 0,
        false
    ) as is_individual_active_current_year,
    coalesce(
        date_diff(current_date, boh.last_collective_bookable_offer_date, day) <= 30,
        false
    ) as is_collective_active_last_30days,
    coalesce(
        date_diff(current_date, boh.last_collective_bookable_offer_date, year) = 0,
        false
    ) as is_collective_active_current_year,
    concat(
        "https://passculture.pro/structures/",
        ofr.offerer_humanized_id,
        "/lieux/",
        v.venue_humanized_id
    ) as venue_pc_pro_link,
    case
        when v.venue_is_permanent then concat("venue-", v.venue_id) else ofr.partner_id
    end as partner_id,
    row_number() over (
        partition by v.venue_managing_offerer_id
        order by
            v.total_theoretic_revenue desc,
            v.total_created_offers desc,
            v.venue_name desc
    ) as offerer_rank_desc,
    row_number() over (
        partition by v.venue_managing_offerer_id
        order by
            v.total_theoretic_revenue desc,
            v.total_created_offers desc,
            v.venue_name asc
    ) as offerer_rank_asc,
    offerer_is_epn,
    venue_network_tags,
    venue_label_tags,
    venue_sector_tags
from {{ ref("int_applicative__venue") }} as v
left join
    {{ ref("int_global__offerer") }} as ofr
    on v.venue_managing_offerer_id = ofr.offerer_id
left join bookable_offer_history as boh on v.venue_id = boh.venue_id
left join venue_tags as vt on vt.venue_id = v.venue_id
