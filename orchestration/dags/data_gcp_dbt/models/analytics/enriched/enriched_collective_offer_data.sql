
{{ config(
    pre_hook="{{create_humanize_id_function()}}"
)}}

{% set target_name = target.name %}
{% set target_schema = generate_schema_name('analytics_' ~ target_name) %}

WITH bookings_per_offer AS (
    SELECT
        collective_offer_id,
        COUNT(DISTINCT collective_booking_id) AS collective_booking_cnt,
        COUNT(
            DISTINCT CASE
                WHEN collective_booking_status NOT IN ('CANCELLED') THEN collective_booking_id
                ELSE NULL
            END
        ) AS collective_booking_no_cancelled_cnt,
        COUNT(
            DISTINCT CASE
                WHEN collective_booking_status IN ('USED', 'REIMBURSED') THEN collective_booking_id
                ELSE NULL
            END
        ) AS collective_booking_confirm_cnt
    FROM
        {{ source('raw', 'applicative_database_collective_booking') }} AS collective_booking
        JOIN {{ source('raw', 'applicative_database_collective_stock') }} AS collective_stock ON collective_stock.collective_stock_id = collective_booking.collective_stock_id
    GROUP BY
        collective_offer_id
),

bookings_per_stock AS (
    SELECT
        collective_stock_id,
        COUNT(
            DISTINCT CASE
                WHEN collective_booking_status NOT IN ('CANCELLED') THEN collective_booking_id
                ELSE NULL
            END
        ) AS collective_booking_stock_no_cancelled_cnt
    FROM
        {{ source('raw', 'applicative_database_collective_booking') }} AS collective_booking
    GROUP BY
        collective_stock_id
)

SELECT
    collective_offer.collective_offer_id,
    collective_offer.collective_offer_name,
    collective_offer.venue_id,
    CASE WHEN venue.venue_is_permanent THEN CONCAT("venue-",venue.venue_id)
         ELSE CONCAT("offerer-", offerer.offerer_id) END AS partner_id,
    collective_offer.institution_id,
    institution_program.institution_program_name AS institution_program_name,
    venue.venue_name,
    venue.venue_department_code,
    venue_region.region_name AS venue_region_name,
    venue_region.academy_name AS venue_academie,
    venue.venue_is_virtual,
    venue.venue_managing_offerer_id AS offerer_id,
    offerer.offerer_name,
    collective_offer.collective_offer_creation_date,
    collective_offer.collective_offer_date_updated,
    collective_stock.collective_stock_price,
    collective_stock.collective_stock_beginning_date_time,
    collective_stock.collective_stock_booking_limit_date_time,
    collective_stock.collective_stock_number_of_tickets AS number_of_tickets,
    collective_offer.collective_offer_subcategory_id,
    subcategories.category_id AS collective_offer_category_id,
    collective_offer.collective_offer_format,
    collective_offer.collective_offer_students,
    collective_offer.collective_offer_is_active,
    CASE
        WHEN collective_offer.collective_offer_id IN (
            SELECT
                collective_stock.collective_offer_id
            FROM
                {{ source('raw', 'applicative_database_collective_stock') }} AS collective_stock
                JOIN {{ source('raw', 'applicative_database_collective_offer') }} AS collective_offer ON collective_stock.collective_offer_id = collective_offer.collective_offer_id
                AND collective_offer.collective_offer_is_active
                LEFT JOIN bookings_per_stock ON collective_stock.collective_stock_id = bookings_per_stock.collective_stock_id
            WHERE
                (
                    (
                        DATE(
                            collective_stock.collective_stock_booking_limit_date_time
                        ) > CURRENT_DATE
                        OR collective_stock.collective_stock_booking_limit_date_time IS NULL
                    )
                    AND (
                        DATE(
                            collective_stock.collective_stock_beginning_date_time
                        ) > CURRENT_DATE
                        OR collective_stock.collective_stock_beginning_date_time IS NULL
                    )
                    AND collective_offer.collective_offer_is_active
                    AND (
                        collective_booking_stock_no_cancelled_cnt IS NULL
                    )
                )
        ) THEN TRUE
        ELSE FALSE
    END AS collective_offer_is_bookable,
    COALESCE(collective_booking_cnt, 0.0) AS collective_booking_cnt,
    COALESCE(collective_booking_no_cancelled_cnt, 0.0) AS collective_booking_no_cancelled_cnt,
    COALESCE(collective_booking_confirm_cnt, 0.0) AS collective_booking_confirm_cnt,
    {{target_schema}}.humanize_id(collective_offer.collective_offer_id) AS collective_offer_humanized_id,
    CONCAT(
        'https://passculture.pro/offre/',
        collective_offer.collective_offer_id,
        '/collectif/edition'
    ) AS passculture_pro_url,
    FALSE AS offer_is_template,
    collective_offer.collective_offer_image_id,
    collective_offer.provider_id,
    collective_offer.national_program_id,
    national_program.national_program_name,
    collective_offer.template_id,
    NULL as collective_offer_contact_url,
    NULL as collective_offer_contact_form
FROM
    {{ source('raw', 'applicative_database_collective_offer') }} AS collective_offer
    JOIN {{ ref('venue') }} AS venue ON venue.venue_id = collective_offer.venue_id
    LEFT JOIN {{ source('raw', 'applicative_database_collective_stock') }} AS collective_stock ON collective_stock.collective_offer_id = collective_offer.collective_offer_id
    JOIN {{ source('raw', 'applicative_database_offerer') }} AS offerer ON offerer.offerer_id = venue.venue_managing_offerer_id
    LEFT JOIN {{ source('clean', 'subcategories') }} ON subcategories.id = collective_offer.collective_offer_subcategory_id
    LEFT JOIN {{ source('analytics', 'region_department') }} venue_region ON venue_region.num_dep = venue.venue_department_code
    LEFT JOIN bookings_per_offer ON bookings_per_offer.collective_offer_id = collective_offer.collective_offer_id
    LEFT JOIN {{ source('raw', 'applicative_database_national_program') }} national_program USING(national_program_id)
    LEFT JOIN {{ ref('int_applicative__institution_program') }} AS institution_program
        ON collective_offer.institution_id = institution_program.institution_id
WHERE collective_offer.collective_offer_validation = 'APPROVED'
UNION
ALL
SELECT
    template.collective_offer_id,
    template.collective_offer_name,
    template.venue_id,
    CASE WHEN venue.venue_is_permanent THEN CONCAT("venue-",venue.venue_id)
         ELSE CONCAT("offerer-", offerer.offerer_id) END AS partner_id,
    NULL AS institution_id,
    NULL AS institution_program_name,
    venue.venue_name,
    venue.venue_department_code,
    venue_region.region_name AS venue_region_name,
    venue_region.academy_name AS venue_academie,
    venue.venue_is_virtual,
    venue.venue_managing_offerer_id AS offerer_id,
    offerer.offerer_name,
    template.collective_offer_creation_date,
    template.collective_offer_date_updated,
    collective_stock.collective_stock_price,
    collective_stock.collective_stock_beginning_date_time,
    collective_stock.collective_stock_booking_limit_date_time,
    collective_stock.collective_stock_number_of_tickets AS number_of_tickets,
    template.collective_offer_subcategory_id,
    subcategories.category_id AS collective_offer_category_id,
    template.collective_offer_format,
    template.collective_offer_students,
    template.collective_offer_is_active,
    FALSE AS collective_offer_is_bookable,
    COALESCE(collective_booking_cnt, 0.0) AS collective_booking_cnt,
    COALESCE(collective_booking_no_cancelled_cnt, 0.0) AS collective_booking_no_cancelled_cnt,
    COALESCE(collective_booking_confirm_cnt, 0.0) AS collective_booking_confirm_cnt,
    {{target_schema}}.humanize_id(template.collective_offer_id) AS collective_offer_humanized_id,
    CONCAT(
        'https://passculture.pro/offre/',
        template.collective_offer_id,
        '/collectif/edition'
    ) AS passculture_pro_url,
    TRUE AS offer_is_template,
    template.collective_offer_image_id,
    template.provider_id,
    template.national_program_id,
    national_program.national_program_name,
    NULL as template_id,
    collective_offer_contact_url,
    collective_offer_contact_form
FROM
    {{ source('raw', 'applicative_database_collective_offer_template') }} AS template
    JOIN {{ ref('venue') }} AS venue ON venue.venue_id = template.venue_id
    JOIN {{ source('raw', 'applicative_database_offerer') }} AS offerer ON offerer.offerer_id = venue.venue_managing_offerer_id
    LEFT JOIN {{ source('clean', 'subcategories') }} ON subcategories.id = template.collective_offer_subcategory_id
    LEFT JOIN {{ source('raw', 'applicative_database_collective_stock') }} AS collective_stock ON collective_stock.collective_offer_id = template.collective_offer_id
    LEFT JOIN {{ source('analytics', 'region_department') }} venue_region ON venue_region.num_dep = venue.venue_department_code
    LEFT JOIN bookings_per_offer ON bookings_per_offer.collective_offer_id = template.collective_offer_id
    LEFT JOIN {{ source('raw', 'applicative_database_national_program') }} national_program USING(national_program_id)
WHERE template.collective_offer_validation = 'APPROVED'