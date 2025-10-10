{{
    config(
        pre_hook="{{create_humanize_id_function()}}",
    )
}}

{% set target_name = var("ENV_SHORT_NAME") %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

select
    co.collective_offer_id,
    case  -- noqa: PRS
        when co.collective_offer_is_template
        then
            concat(
                'template-',
                {{ target_schema }}.humanize_id(
                    regexp_replace(co.collective_offer_id, r'^template-', '')
                )
            )
        else {{ target_schema }}.humanize_id(co.collective_offer_id)
    end as collective_offer_humanized_id,
    co.collective_offer_name,
    co.venue_id,
    v.partner_id,
    co.institution_id,
    institution_program.institution_program_name,
    v.venue_name,
    v.venue_region_name,
    v.venue_department_code,
    v.venue_department_name,
    v.venue_postal_code,
    v.venue_city,
    v.venue_epci,
    v.venue_academy_name,
    v.venue_density_label,
    v.venue_macro_density_label,
    v.venue_density_level,
    v.venue_is_virtual,
    v.offerer_id,
    v.offerer_name,
    v.venue_iris_internal_id,
    v.is_local_authority,
    v.venue_type_label,
    v.venue_is_permanent,
    co.collective_offer_creation_date,
    co.collective_offer_date_updated,
    co.collective_offer_format,
    co.collective_offer_students,
    co.collective_offer_is_active,
    co.collective_offer_validation,
    co.collective_offer_last_validation_type,
    co.collective_offer_is_bookable,
    co.total_collective_bookings,
    co.total_non_cancelled_collective_bookings,
    co.total_used_collective_bookings,
    co.institution_academy_name,
    co.institution_region_name,
    co.institution_department_code,
    co.institution_department_name,
    co.institution_postal_code,
    co.institution_city,
    co.institution_epci,
    co.institution_density_label,
    co.institution_macro_density_label,
    co.institution_density_level,
    concat(
        'https://backoffice.passculture.team/pro/collective-offer/',
        co.collective_offer_id,
        'details'
    ) as passculture_pro_url,
    co.collective_offer_is_template,
    co.collective_offer_image_id,
    co.provider_id,
    co.national_program_id,
    national_program.national_program_name,
    co.template_id,
    co.collective_offer_contact_url,
    co.collective_offer_contact_form,
    co.collective_offer_contact_email,
    co.collective_offer_contact_phone,
    co.institution_internal_iris_id,
    cs.collective_stock_beginning_date_time,
    cs.collective_stock_end_date_time,
    cs.collective_stock_booking_limit_date_time,
    co.collective_offer_template_beginning_date,
    co.collective_offer_template_ending_date,
    co.collective_offer_last_validation_date,
    co.collective_offer_rejection_reason,
    cs.collective_stock_price,
    cs.collective_stock_number_of_tickets,
    cs.collective_stock_id,
    co.collective_offer_location_type,
    co.offerer_address_id
from {{ ref("int_applicative__collective_offer") }} as co
inner join {{ ref("int_global__venue") }} as v on v.venue_id = co.venue_id
left join
    {{ source("raw", "applicative_database_national_program") }} as national_program
    on national_program.national_program_id = co.national_program_id
left join
    {{ ref("int_applicative__institution_program") }} as institution_program
    on co.institution_id = institution_program.institution_id
left join
    {{ ref("int_applicative__collective_stock") }} as cs
    on cs.collective_offer_id = co.collective_offer_id
