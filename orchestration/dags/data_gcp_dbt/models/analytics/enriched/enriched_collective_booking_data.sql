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
    venue.venue_department_code,
    venue_region_departement.region_name AS venue_region_name,
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
    eple.nom_etablissement,
    institution_program.institution_program_name AS institution_program_name,
    eple.code_departement AS school_department_code,
    school_region_departement.region_name AS school_region_name,
    eple.libelle_academie,
    collective_offer.collective_offer_venue_address_type AS collective_offer_address_type
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
FROM
    {{ source('raw', 'applicative_database_collective_booking') }}  AS collective_booking
    INNER JOIN {{ source('raw', 'applicative_database_collective_stock') }} AS collective_stock ON collective_stock.collective_stock_id = collective_booking.collective_stock_id
    INNER JOIN {{ source('raw', 'applicative_database_collective_offer') }} AS collective_offer ON collective_offer.collective_offer_id = collective_stock.collective_offer_id
    INNER JOIN {{ ref('venue') }} AS venue ON collective_booking.venue_id = venue.venue_id
    INNER JOIN {{ source('raw', 'applicative_database_offerer') }} AS offerer ON offerer.offerer_id = venue.venue_managing_offerer_id
    INNER JOIN {{ ref('educational_institution') }} AS educational_institution ON educational_institution.educational_institution_id = collective_booking.educational_institution_id
    INNER JOIN {{ source('raw', 'applicative_database_educational_year') }} AS educational_year ON educational_year.adage_id = collective_booking.educational_year_id
    LEFT JOIN {{ source('analytics', 'eple') }} AS eple ON eple.id_etablissement = educational_institution.institution_id
    LEFT JOIN {{ source('analytics', 'region_department') }} AS venue_region_departement ON venue.venue_department_code = venue_region_departement.num_dep
    LEFT JOIN {{ source('analytics', 'region_department') }} AS school_region_departement ON eple.code_departement = school_region_departement.num_dep
    LEFT JOIN collective_booking_ranking_view ON collective_booking_ranking_view.collective_booking_id = collective_booking.collective_booking_id
    LEFT JOIN {{ source('clean', 'subcategories') }} subcategories ON collective_offer.collective_offer_subcategory_id = subcategories.id
    LEFT JOIN {{ ref('int_applicative__institution') }} AS institution_program
        ON collective_booking.educational_institution_id = institution_program.institution_id