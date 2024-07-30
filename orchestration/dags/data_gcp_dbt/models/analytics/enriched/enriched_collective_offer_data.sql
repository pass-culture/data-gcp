{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

{% set target_name = target.name %}
{% set target_schema = generate_schema_name('analytics_' ~ target_name) %}

with bookings_per_offer as (
    select
        collective_offer_id,
        COUNT(distinct collective_booking_id) as collective_booking_cnt,
        COUNT(
            distinct case
                when collective_booking_status not in ('CANCELLED') then collective_booking_id
                else NULL
            end
        ) as collective_booking_no_cancelled_cnt,
        COUNT(
            distinct case
                when collective_booking_status in ('USED', 'REIMBURSED') then collective_booking_id
                else NULL
            end
        ) as collective_booking_confirm_cnt
    from
        {{ source('raw', 'applicative_database_collective_booking') }} as collective_booking
        join {{ source('raw', 'applicative_database_collective_stock') }} as collective_stock on collective_stock.collective_stock_id = collective_booking.collective_stock_id
    group by
        collective_offer_id
),

bookings_per_stock as (
    select
        collective_stock_id,
        COUNT(
            distinct case
                when collective_booking_status not in ('CANCELLED') then collective_booking_id
                else NULL
            end
        ) as collective_booking_stock_no_cancelled_cnt
    from
        {{ source('raw', 'applicative_database_collective_booking') }} as collective_booking
    group by
        collective_stock_id
)

select
    collective_offer.collective_offer_id,
    collective_offer.collective_offer_name,
    collective_offer.venue_id,
    case
        when venue.venue_is_permanent then CONCAT("venue-", venue.venue_id)
        else CONCAT("offerer-", offerer.offerer_id)
    end as partner_id,
    collective_offer.institution_id,
    institution_program.institution_program_name as institution_program_name,
    venue.venue_name,
    venue.venue_department_code,
    venue_region.region_name as venue_region_name,
    venue_region.academy_name as venue_academie,
    venue.venue_is_virtual,
    venue.venue_managing_offerer_id as offerer_id,
    offerer.offerer_name,
    collective_offer.collective_offer_creation_date,
    collective_offer.collective_offer_date_updated,
    collective_stock.collective_stock_price,
    collective_stock.collective_stock_beginning_date_time,
    collective_stock.collective_stock_booking_limit_date_time,
    collective_stock.collective_stock_number_of_tickets as number_of_tickets,
    collective_offer.collective_offer_subcategory_id,
    subcategories.category_id as collective_offer_category_id,
    collective_offer.collective_offer_format,
    collective_offer.collective_offer_students,
    collective_offer.collective_offer_is_active,
    case
        when collective_offer.collective_offer_id in (
            select collective_stock.collective_offer_id
            from
                {{ source('raw', 'applicative_database_collective_stock') }} as collective_stock
                join {{ source('raw', 'applicative_database_collective_offer') }} as collective_offer
                    on
                        collective_stock.collective_offer_id = collective_offer.collective_offer_id
                        and collective_offer.collective_offer_is_active
                left join bookings_per_stock on collective_stock.collective_stock_id = bookings_per_stock.collective_stock_id
            where
                (
                    (
                        DATE(
                            collective_stock.collective_stock_booking_limit_date_time
                        ) > CURRENT_DATE
                        or collective_stock.collective_stock_booking_limit_date_time is NULL
                    )
                    and (
                        DATE(
                            collective_stock.collective_stock_beginning_date_time
                        ) > CURRENT_DATE
                        or collective_stock.collective_stock_beginning_date_time is NULL
                    )
                    and collective_offer.collective_offer_is_active
                    and (
                        collective_booking_stock_no_cancelled_cnt is NULL
                    )
                )
        ) then TRUE
        else FALSE
    end as collective_offer_is_bookable,
    COALESCE(collective_booking_cnt, 0.0) as collective_booking_cnt,
    COALESCE(collective_booking_no_cancelled_cnt, 0.0) as collective_booking_no_cancelled_cnt,
    COALESCE(collective_booking_confirm_cnt, 0.0) as collective_booking_confirm_cnt,
    {{ target_schema }}.humanize_id(collective_offer.collective_offer_id) as collective_offer_humanized_id,
    CONCAT(
        'https://passculture.pro/offre/',
        collective_offer.collective_offer_id,
        '/collectif/edition'
    ) as passculture_pro_url,
    FALSE as offer_is_template,
    collective_offer.collective_offer_image_id,
    collective_offer.provider_id,
    collective_offer.national_program_id,
    national_program.national_program_name,
    collective_offer.template_id,
    collective_offer.collective_offer_venue_address_type as collective_offer_address_type,
    NULL as collective_offer_contact_url,
    NULL as collective_offer_contact_form,
    NULL as collective_offer_contact_email,
    NULL as collective_offer_contact_phone,
    venue.venue_iris_internal_id,
    institution_locations.institution_internal_iris_id
from
    {{ source('raw', 'applicative_database_collective_offer') }} as collective_offer
    join {{ ref('venue') }} as venue on venue.venue_id = collective_offer.venue_id
    left join {{ source('raw', 'applicative_database_collective_stock') }} as collective_stock on collective_stock.collective_offer_id = collective_offer.collective_offer_id
    join {{ source('raw', 'applicative_database_offerer') }} as offerer on offerer.offerer_id = venue.venue_managing_offerer_id
    left join {{ source('clean', 'subcategories') }} on subcategories.id = collective_offer.collective_offer_subcategory_id
    left join {{ source('analytics', 'region_department') }} venue_region on venue_region.num_dep = venue.venue_department_code
    left join bookings_per_offer on bookings_per_offer.collective_offer_id = collective_offer.collective_offer_id
    left join {{ source('raw', 'applicative_database_national_program') }} national_program using (national_program_id)
    left join {{ ref('int_applicative__institution') }} as institution_program
        on collective_offer.institution_id = institution_program.institution_id
    left join {{ ref('educational_institution') }} as educational_institution on educational_institution.educational_institution_id = collective_offer.institution_id
    left join {{ ref('institution_locations') }} as institution_locations on institution_locations.institution_id = educational_institution.institution_id
where collective_offer.collective_offer_validation = 'APPROVED'
union
all
select
    template.collective_offer_id,
    template.collective_offer_name,
    template.venue_id,
    case
        when venue.venue_is_permanent then CONCAT("venue-", venue.venue_id)
        else CONCAT("offerer-", offerer.offerer_id)
    end as partner_id,
    NULL as institution_id,
    NULL as institution_program_name,
    venue.venue_name,
    venue.venue_department_code,
    venue_region.region_name as venue_region_name,
    venue_region.academy_name as venue_academie,
    venue.venue_is_virtual,
    venue.venue_managing_offerer_id as offerer_id,
    offerer.offerer_name,
    template.collective_offer_creation_date,
    template.collective_offer_date_updated,
    collective_stock.collective_stock_price,
    collective_stock.collective_stock_beginning_date_time,
    collective_stock.collective_stock_booking_limit_date_time,
    collective_stock.collective_stock_number_of_tickets as number_of_tickets,
    template.collective_offer_subcategory_id,
    subcategories.category_id as collective_offer_category_id,
    template.collective_offer_format,
    template.collective_offer_students,
    template.collective_offer_is_active,
    FALSE as collective_offer_is_bookable,
    COALESCE(collective_booking_cnt, 0.0) as collective_booking_cnt,
    COALESCE(collective_booking_no_cancelled_cnt, 0.0) as collective_booking_no_cancelled_cnt,
    COALESCE(collective_booking_confirm_cnt, 0.0) as collective_booking_confirm_cnt,
    {{ target_schema }}.humanize_id(template.collective_offer_id) as collective_offer_humanized_id,
    CONCAT(
        'https://passculture.pro/offre/',
        template.collective_offer_id,
        '/collectif/edition'
    ) as passculture_pro_url,
    TRUE as offer_is_template,
    template.collective_offer_image_id,
    template.provider_id,
    template.national_program_id,
    national_program.national_program_name,
    NULL as template_id,
    template.collective_offer_venue_address_type as collective_offer_address_type,
    collective_offer_contact_url,
    collective_offer_contact_form,
    collective_offer_contact_email,
    collective_offer_contact_phone,
    venue.venue_iris_internal_id,
    NULL as institution_internal_iris_id
from
    {{ source('raw', 'applicative_database_collective_offer_template') }} as template
    join {{ ref('venue') }} as venue on venue.venue_id = template.venue_id
    join {{ source('raw', 'applicative_database_offerer') }} as offerer on offerer.offerer_id = venue.venue_managing_offerer_id
    left join {{ source('clean', 'subcategories') }} on subcategories.id = template.collective_offer_subcategory_id
    left join {{ source('raw', 'applicative_database_collective_stock') }} as collective_stock on collective_stock.collective_offer_id = template.collective_offer_id
    left join {{ source('analytics', 'region_department') }} venue_region on venue_region.num_dep = venue.venue_department_code
    left join bookings_per_offer on bookings_per_offer.collective_offer_id = template.collective_offer_id
    left join {{ source('raw', 'applicative_database_national_program') }} national_program using (national_program_id)
where template.collective_offer_validation = 'APPROVED'
