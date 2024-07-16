WITH collective_booking_ranking_view AS (
    SELECT
        collective_booking.collective_booking_id,
        rank() OVER (
            PARTITION BY collective_booking.educational_institution_id
            ORDER BY
                collective_booking.collective_booking_creation_date
        ) AS collective_booking_rank
    FROM
        {{ source('raw', 'applicative_database_collective_booking') }} AS collective_booking
)

SELECT
    collective_booking.collective_booking_id,
    collective_booking.collective_stock_id AS collective_stock_id,
    collective_stock.stock_id AS stock_id,
    collective_stock.collective_offer_id AS collective_offer_id,
    offer_id AS offer_id,
    collective_offer.collective_offer_name,
    collective_offer.collective_offer_subcategory_id,
    subcategories.category_id AS collective_offer_category_id,
    collective_offer.collective_offer_format AS collective_offer_format,
    collective_offer.venue_id AS venue_id,
    venue.venue_name,
    venue.venue_postal_code,
    venue.venue_department_code,
    venue.venue_region_name,
    venue.venue_city,
    venue.venue_epci,
    venue.venue_academy_name,
    venue.venue_density_label,
    venue.venue_macro_density_label,
    collective_booking.offerer_id AS offerer_id,
    CASE WHEN venue.venue_is_permanent THEN CONCAT("venue-",venue.venue_id)
         ELSE CONCAT("offerer-", offerer.offerer_id) END AS partner_id,
    offerer.offerer_name,
    collective_stock.collective_stock_price AS booking_amount,
    collective_stock.collective_stock_number_of_tickets AS number_of_tickets,
    collective_stock.collective_stock_beginning_date_time,
    collective_booking.educational_institution_id AS educational_institution_id,
    collective_booking.educational_year_id AS educational_year_id,
    educational_year.scholar_year,
    collective_booking.educational_redactor_id AS educational_redactor_id,
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
    collective_offer.collective_offer_venue_address_type AS collective_offer_address_type,
    collective_booking.collective_booking_creation_date,
    collective_booking.collective_booking_cancellation_date,
    CASE WHEN collective_booking.collective_booking_cancellation_date IS NULL THEN "FALSE" ELSE "TRUE" END AS collective_booking_is_cancelled,
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
FROM
    {{ source('raw', 'applicative_database_collective_booking') }}  AS collective_booking
    INNER JOIN {{ source('raw', 'applicative_database_collective_stock') }} AS collective_stock ON collective_stock.collective_stock_id = collective_booking.collective_stock_id
    INNER JOIN {{ source('raw', 'applicative_database_collective_offer') }} AS collective_offer ON collective_offer.collective_offer_id = collective_stock.collective_offer_id
    INNER JOIN {{ ref('int_applicative__venue') }} AS venue ON collective_booking.venue_id = venue.venue_id
    INNER JOIN {{ source('raw', 'applicative_database_offerer') }} AS offerer ON offerer.offerer_id = venue.venue_managing_offerer_id
    INNER JOIN {{ ref('educational_institution') }} AS educational_institution ON educational_institution.educational_institution_id = collective_booking.educational_institution_id
    INNER JOIN {{ source('raw', 'applicative_database_educational_year') }} AS educational_year ON educational_year.adage_id = collective_booking.educational_year_id
    LEFT JOIN collective_booking_ranking_view ON collective_booking_ranking_view.collective_booking_id = collective_booking.collective_booking_id
    LEFT JOIN {{ source('clean', 'subcategories') }} subcategories ON collective_offer.collective_offer_subcategory_id = subcategories.id
    LEFT JOIN {{ ref('int_applicative__institution') }} AS institution_program
        ON collective_booking.educational_institution_id = institution_program.institution_id
    LEFT JOIN {{ ref('institution_locations') }} AS institution_locations ON institution_locations.institution_id = educational_institution.institution_id
