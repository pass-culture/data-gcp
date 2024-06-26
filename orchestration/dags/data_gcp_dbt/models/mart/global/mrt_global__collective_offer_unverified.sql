{{ config(
    pre_hook="{{create_humanize_id_function()}}"
)}}

{% set target_name = target.name %}
{% set target_schema = generate_schema_name('analytics_' ~ target_name) %}

SELECT co.collective_offer_id,
    {{target_schema}}.humanize_id(co.collective_offer_id) AS collective_offer_humanized_id,
    co.collective_offer_name,
    co.venue_id,
    v.partner_id,
    co.institution_id,
    institution_program.institution_program_name,
    v.venue_name,
    v.venue_department_code,
    v.venue_region_name,
    v.venue_academy_name,
    v.venue_is_virtual,
    v.venue_managing_offerer_id AS offerer_id,
    v.offerer_name,
    v.venue_iris_internal_id,
    co.offer_id,
    co.collective_offer_creation_date,
    co.collective_offer_date_updated,
    co.collective_offer_subcategory_id,
    subcategories.category_id AS collective_offer_category_id,
    co.collective_offer_format,
    co.collective_offer_students,
    co.collective_offer_is_active,
    co.collective_offer_validation,
    co.collective_offer_is_bookable,
    co.total_collective_bookings,
    co.total_non_cancelled_collective_bookings,
    co.total_used_collective_bookings,
    CONCAT(
        'https://passculture.pro/offre/',
        co.collective_offer_id,
        '/collectif/edition'
    ) AS passculture_pro_url,
    co.collective_offer_is_template,
    co.collective_offer_image_id,
    co.provider_id,
    co.national_program_id,
    national_program.national_program_name,
    co.template_id,
    co.collective_offer_address_type,
    co.collective_offer_contact_url,
    co.collective_offer_contact_form,
    co.collective_offer_contact_email,
    co.collective_offer_contact_phone,
    co.institution_internal_iris_id,
    cs.collective_stock_beginning_date_time,
    cs.collective_stock_booking_limit_date_time,
    cs.collective_stock_price,
    cs.collective_stock_number_of_tickets,
    cs.collective_stock_id,
    cs.stock_id,
FROM {{ ref('int_applicative__collective_offer') }} AS co
    INNER JOIN {{ref('mrt_global__venue_unverified')}} AS v ON v.venue_id = co.venue_id
    LEFT JOIN {{ source('clean', 'subcategories') }} ON subcategories.id = co.collective_offer_subcategory_id
    LEFT JOIN {{ source('raw', 'applicative_database_national_program') }} AS national_program ON national_program.national_program_id = co.national_program_id
    LEFT JOIN {{ ref('int_applicative__institution') }} AS institution_program ON co.institution_id = institution_program.institution_id
    INNER JOIN {{ ref('int_applicative__collective_stock') }} AS cs ON cs.collective_offer_id = co.collective_offer_id
