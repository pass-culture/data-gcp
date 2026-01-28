select
    cb.collective_booking_id,
    cb.collective_stock_id,
    cb.educational_deposit_id,
    cb.is_used_collective_booking,
    co.collective_offer_id,
    co.collective_offer_name,
    co.collective_offer_format,
    co.venue_id,
    co.venue_name,
    co.venue_department_code,
    co.venue_department_name,
    co.venue_region_name,
    cb.offerer_id,
    co.partner_id,
    co.offerer_name,
    co.venue_iris_internal_id,
    co.collective_stock_price as booking_amount,
    co.collective_stock_number_of_tickets,
    co.collective_stock_beginning_date_time,
    co.collective_stock_end_date_time,
    cb.educational_institution_id,
    cb.educational_year_id,
    cb.educational_redactor_id,
    co.institution_program_name,
    co.institution_internal_iris_id,
    educational_institution.institution_name,
    educational_institution.ministry,
    co.institution_academy_name,
    co.institution_region_name,
    co.institution_department_code,
    co.institution_department_name,
    co.institution_postal_code,
    co.institution_city,
    co.institution_city_code,
    co.institution_epci,
    co.institution_epci_code,
    co.institution_density_label,
    co.institution_macro_density_label,
    co.institution_density_level,
    co.venue_city,
    co.venue_city_code,
    co.venue_epci,
    co.venue_epci_code,
    co.venue_academy_name,
    co.venue_density_label,
    co.venue_macro_density_label,
    co.venue_density_level,
    co.venue_postal_code,
    co.is_local_authority,
    co.venue_type_label,
    co.venue_is_permanent,
    cb.collective_booking_creation_date,
    cb.collective_booking_cancellation_date,
    cb.collective_booking_is_cancelled,
    cb.collective_booking_status,
    cb.collective_booking_cancellation_reason,
    cb.collective_booking_confirmation_date,
    cb.collective_booking_confirmation_limit_date,
    cb.collective_booking_used_date,
    cb.collective_booking_reimbursement_date,
    cb.collective_booking_rank_asc,
    cb.collective_booking_rank_desc,
    co.collective_offer_image_id,
    co.collective_offer_location_type,
    cb.is_current_deposit as is_current_deposit_booking,
    coalesce(cb.deposit_scholar_year, ey.scholar_year) as scholar_year,
    coalesce(
        (
            cast(ey.educational_year_beginning_date as date) <= current_date
            and cast(ey.educational_year_expiration_date as date) >= current_date
        ),
        false
    ) as is_current_scholar_year_booking,
    coalesce(

        extract(year from cb.collective_booking_creation_date)
        = extract(year from current_date),
        false

    ) as is_current_calendar_year_booking
from {{ ref("int_applicative__collective_booking") }} as cb
inner join
    {{ ref("int_global__collective_offer") }} as co
    on cb.collective_stock_id = co.collective_stock_id
inner join
    {{ source("raw", "applicative_database_educational_year") }} as ey
    on cb.educational_year_id = ey.adage_id
inner join
    {{ ref("int_applicative__educational_institution") }} as educational_institution
    on cb.educational_institution_id
    = educational_institution.educational_institution_id
