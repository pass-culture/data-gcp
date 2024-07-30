with collective_booking_ranking_view as (
    select
        collective_booking.collective_booking_id,
        rank() over (
            partition by collective_booking.educational_institution_id
            order by
                collective_booking.collective_booking_creation_date
        ) as collective_booking_rank
    from
        {{ source('raw', 'applicative_database_collective_booking') }} as collective_booking
)

select
    collective_booking.collective_booking_id,
    collective_booking.collective_stock_id as collective_stock_id,
    collective_stock.stock_id as stock_id,
    collective_stock.collective_offer_id as collective_offer_id,
    offer_id as offer_id,
    collective_offer.collective_offer_name,
    collective_offer.collective_offer_subcategory_id,
    subcategories.category_id as collective_offer_category_id,
    collective_offer.collective_offer_format as collective_offer_format,
    collective_offer.venue_id as venue_id,
    venue.venue_name,
    venue.venue_postal_code,
    venue.venue_department_code,
    venue.venue_region_name,
    venue.venue_city,
    venue.venue_epci,
    venue.venue_academy_name,
    venue.venue_density_label,
    venue.venue_macro_density_label,
    collective_booking.offerer_id as offerer_id,
    case
        when venue.venue_is_permanent then concat("venue-", venue.venue_id)
        else concat("offerer-", offerer.offerer_id)
    end as partner_id,
    offerer.offerer_name,
    collective_stock.collective_stock_price as booking_amount,
    collective_stock.collective_stock_number_of_tickets as number_of_tickets,
    collective_stock.collective_stock_beginning_date_time,
    collective_booking.educational_institution_id as educational_institution_id,
    collective_booking.educational_year_id as educational_year_id,
    educational_year.scholar_year,
    collective_booking.educational_redactor_id as educational_redactor_id,
    educational_institution.institution_name,
    institution_program.institution_program_name,
    institution_locations.institution_academy_name,
    institution_locations.institution_region_name,
    institution_locations.institution_department_code,
    institution_locations.institution_postal_code,
    institution_locations.institution_city,
    institution_locations.institution_epci,
    institution_locations.institution_density_label,
    institution_locations.institution_macro_density_label,
    collective_offer.collective_offer_venue_address_type as collective_offer_address_type,
    collective_booking.collective_booking_creation_date,
    collective_booking.collective_booking_cancellation_date,
    case when collective_booking.collective_booking_cancellation_date is NULL then "FALSE" else "TRUE" end as collective_booking_is_cancelled,
    collective_booking.collective_booking_status,
    collective_booking.collective_booking_cancellation_reason,
    collective_booking.collective_booking_confirmation_date,
    collective_booking.collective_booking_confirmation_limit_date,
    collective_booking.collective_booking_used_date,
    collective_booking.collective_booking_reimbursement_date,
    collective_booking_ranking_view.collective_booking_rank,
    collective_offer.collective_offer_image_id,
    institution_locations.institution_internal_iris_id,
    venue.venue_iris_internal_id
from
    {{ source('raw', 'applicative_database_collective_booking') }} as collective_booking
    inner join {{ source('raw', 'applicative_database_collective_stock') }} as collective_stock on collective_stock.collective_stock_id = collective_booking.collective_stock_id
    inner join {{ source('raw', 'applicative_database_collective_offer') }} as collective_offer on collective_offer.collective_offer_id = collective_stock.collective_offer_id
    inner join {{ ref('int_applicative__venue') }} as venue on collective_booking.venue_id = venue.venue_id
    inner join {{ source('raw', 'applicative_database_offerer') }} as offerer on offerer.offerer_id = venue.venue_managing_offerer_id
    inner join {{ ref('educational_institution') }} as educational_institution on educational_institution.educational_institution_id = collective_booking.educational_institution_id
    inner join {{ source('raw', 'applicative_database_educational_year') }} as educational_year on educational_year.adage_id = collective_booking.educational_year_id
    left join collective_booking_ranking_view on collective_booking_ranking_view.collective_booking_id = collective_booking.collective_booking_id
    left join {{ source('clean', 'subcategories') }} subcategories on collective_offer.collective_offer_subcategory_id = subcategories.id
    left join {{ ref('int_applicative__institution') }} as institution_program
        on collective_booking.educational_institution_id = institution_program.institution_id
    left join {{ ref('institution_locations') }} as institution_locations on institution_locations.institution_id = educational_institution.institution_id
