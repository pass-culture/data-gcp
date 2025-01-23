WITH bookable_offer_history as (
    select
        venue_id,
        MIN(partition_date) as first_bookable_offer_date,
        MAX(partition_date) as last_bookable_offer_date,
        MIN(case when individual_bookable_offers > 0 then partition_date end) as first_individual_bookable_offer_date,
        MAX(case when individual_bookable_offers > 0 then partition_date end) as last_individual_bookable_offer_date,
        MIN(case when collective_bookable_offers > 0 then partition_date end) as first_collective_bookable_offer_date,
        MAX(case when collective_bookable_offers > 0 then partition_date end) as last_collective_bookable_offer_date
    from {{ ref('bookable_venue_history') }}
    group by venue_id
)

select
    v.venue_id,
    venue_name,
    venue_public_name,
    venue_booking_email,
    venue_street,
    venue_latitude,
    venue_longitude,
    venue_department_code,
    venue_department_name,
    venue_postal_code,
    venue_city,
    venue_siret,
    venue_is_virtual,
    venue_managing_offerer_id,
    venue_creation_date,
    venue_is_permanent,
    venue_is_open_to_public,
    venue_is_acessibility_synched,
    venue_type_label,
    venue_label,
    venue_humanized_id,
    venue_backoffice_link,
    venue_region_name,
    venue_epci,
    venue_density_label,
    venue_macro_density_label,
    venue_density_level,
    venue_academy_name,
    venue_in_qpv,
    venue_targeted_audience,
    banner_url,
    venue_description,
    venue_withdrawal_details,
    venue_contact_phone_number,
    venue_contact_email,
    venue_contact_website,
    total_individual_bookings,
    total_collective_bookings,
    total_bookings,
    total_non_cancelled_individual_bookings,
    total_non_cancelled_collective_bookings,
    first_individual_booking_date,
    last_individual_booking_date,
    first_collective_booking_date,
    last_collective_booking_date,
    total_non_cancelled_bookings,
    total_used_bookings,
    total_used_individual_bookings,
    total_used_collective_bookings,
    total_individual_theoretic_revenue,
    total_individual_real_revenue,
    total_collective_theoretic_revenue,
    total_collective_real_revenue,
    total_theoretic_revenue,
    total_real_revenue,
    first_individual_offer_creation_date,
    last_individual_offer_creation_date,
    total_created_individual_offers,
    first_collective_offer_creation_date,
    last_collective_offer_creation_date,
    total_created_collective_offers,
    total_created_offers,
    first_bookable_offer_date,
    last_bookable_offer_date,
    first_individual_bookable_offer_date,
    last_individual_bookable_offer_date,
    first_collective_bookable_offer_date,
    last_collective_bookable_offer_date,
    first_booking_date,
    last_booking_date,
    first_offer_creation_date,
    last_offer_creation_date,
    total_bookable_individual_offers,
    total_bookable_collective_offers,
    total_bookable_offers,
    total_non_cancelled_tickets,
    total_current_year_non_cancelled_tickets,
    case when DATE_DIFF(CURRENT_DATE, boh.last_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_active_last_30days,
    case when DATE_DIFF(CURRENT_DATE, boh.last_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_active_current_year,
    case when DATE_DIFF(CURRENT_DATE, boh.last_individual_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_individual_active_last_30days,
    case when DATE_DIFF(CURRENT_DATE, boh.last_individual_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_individual_active_current_year,
    case when DATE_DIFF(CURRENT_DATE, boh.last_collective_bookable_offer_date, day) <= 30 then TRUE else FALSE end as is_collective_active_last_30days,
    case when DATE_DIFF(CURRENT_DATE, boh.last_collective_bookable_offer_date, year) = 0 then TRUE else FALSE end as is_collective_active_current_year,
    offerer_id,
    offerer_name,
    offerer_validation_status,
    offerer_is_active,
    dms_accepted_at,
    first_dms_adage_status,
    is_reference_adage,
    is_synchro_adage,
    venue_pc_pro_link,
    partner_id,
    venue_iris_internal_id,
    offerer_address_id,
    offerer_rank_desc,
    offerer_rank_asc,
    venue_image_source,
    venue_adage_inscription_date
from {{ ref("int_global__venue") }} as v
left join bookable_offer_history as boh on boh.venue_id = v.venue_id
where offerer_validation_status = 'VALIDATED' and offerer_is_active
