with collective_stocks_grouped_by_collective_offers as (
    select
        collective_offer_id,
        MAX(collective_stock_is_bookable) as collective_stock_is_bookable, -- bookable = if at least one collective_stock is_bookable
        SUM(total_collective_bookings) as total_collective_bookings,
        SUM(total_non_cancelled_collective_bookings) as total_non_cancelled_collective_bookings,
        SUM(total_used_collective_bookings) as total_used_collective_bookings,
        MIN(first_collective_booking_date) as first_collective_booking_date,
        MAX(last_collective_booking_date) as last_collective_booking_date,
        SUM(total_collective_theoretic_revenue) as total_collective_theoretic_revenue,
        SUM(total_collective_real_revenue) as total_collective_real_revenue,
        SUM(total_collective_current_year_real_revenue) as total_collective_current_year_real_revenue,
        SUM(case when total_non_cancelled_collective_bookings > 0 then collective_stock_number_of_tickets end) as total_non_cancelled_tickets,
        SUM(case when total_current_year_non_cancelled_collective_bookings > 0 then collective_stock_number_of_tickets end) as total_current_year_non_cancelled_tickets
    from {{ ref('int_applicative__collective_stock') }}
    group by collective_offer_id
)

(
    select
        co.collective_offer_audio_disability_compliant,
        co.collective_offer_mental_disability_compliant,
        co.collective_offer_motor_disability_compliant,
        co.collective_offer_visual_disability_compliant,
        co.collective_offer_last_validation_date,
        co.collective_offer_validation,
        co.collective_offer_id,
        co.offer_id,
        co.collective_offer_is_active,
        co.venue_id,
        co.collective_offer_name,
        co.collective_offer_description,
        co.collective_offer_duration_minutes,
        DATE(collective_offer_creation_date) as collective_offer_creation_date,
        collective_offer_creation_date as collective_offer_created_at,
        co.collective_offer_subcategory_id,
        co.collective_offer_format,
        co.collective_offer_date_updated,
        co.collective_offer_students,
        NULL as collective_offer_price_detail,
        co.collective_offer_booking_email,
        co.collective_offer_contact_email,
        co.collective_offer_contact_phone,
        NULL as collective_offer_contact_url,
        NULL as collective_offer_contact_form,
        co.collective_offer_offer_venue,
        co.collective_offer_venue_humanized_id,
        co.collective_offer_venue_address_type as collective_offer_address_type,
        co.collective_offer_venue_other_address,
        co.intervention_area,
        co.template_id,
        co.collective_offer_last_validation_type,
        co.institution_id,
        co.collective_offer_image_id,
        co.provider_id,
        co.national_program_id,
        NULL as collective_offer_template_beginning_date,
        NULL as collective_offer_template_ending_date,
        case when cs.collective_stock_is_bookable and co.collective_offer_is_active then TRUE else FALSE end as collective_offer_is_bookable,
        cs.total_non_cancelled_collective_bookings,
        cs.total_collective_bookings,
        cs.total_used_collective_bookings,
        cs.total_collective_theoretic_revenue,
        cs.total_collective_real_revenue,
        cs.total_collective_current_year_real_revenue,
        cs.first_collective_booking_date,
        cs.last_collective_booking_date,
        cs.total_non_cancelled_tickets,
        cs.total_current_year_non_cancelled_tickets,
        il.institution_internal_iris_id,
        il.institution_academy_name,
        il.institution_region_name,
        il.institution_department_code,
        il.institution_department_name,
        il.institution_postal_code,
        il.institution_city,
        il.institution_epci,
        il.institution_density_label,
        il.institution_macro_density_label,
        il.institution_density_level,
        FALSE as collective_offer_is_template
    from {{ source('raw','applicative_database_collective_offer') }} as co
        left join collective_stocks_grouped_by_collective_offers as cs on cs.collective_offer_id = co.collective_offer_id
        left join {{ ref('int_applicative__educational_institution') }} as ei on ei.educational_institution_id = co.institution_id
        left join {{ ref('int_geo__institution_location') }} as il on il.institution_id = ei.institution_id
)
union all
(
    select
        collective_offer_audio_disability_compliant,
        collective_offer_mental_disability_compliant,
        collective_offer_motor_disability_compliant,
        collective_offer_visual_disability_compliant,
        collective_offer_last_validation_date,
        collective_offer_validation,
        collective_offer_id,
        offer_id,
        collective_offer_is_active,
        venue_id,
        collective_offer_name,
        collective_offer_description,
        collective_offer_duration_minutes,
        DATE(collective_offer_creation_date) as collective_offer_creation_date,
        collective_offer_creation_date as collective_offer_created_at,
        collective_offer_subcategory_id,
        collective_offer_format,
        collective_offer_date_updated,
        collective_offer_students,
        collective_offer_price_detail,
        collective_offer_booking_email,
        collective_offer_contact_email,
        collective_offer_contact_phone,
        collective_offer_contact_url,
        collective_offer_contact_form,
        collective_offer_offer_venue,
        collective_offer_venue_humanized_id,
        collective_offer_venue_address_type,
        collective_offer_venue_other_address,
        intervention_area,
        NULL as template_id,
        collective_offer_last_validation_type,
        NULL as institution_id,
        collective_offer_image_id,
        provider_id,
        national_program_id,
        collective_offer_template_beginning_date,
        collective_offer_template_ending_date,
        TRUE as collective_offer_is_bookable,
        0 as total_non_cancelled_collective_bookings,
        0 as total_collective_bookings,
        0 as total_used_collective_bookings,
        0 as total_collective_theoretic_revenue,
        0 as total_collective_real_revenue,
        0 as total_collective_current_year_real_revenue,
        NULL as first_collective_booking_date,
        NULL as last_collective_booking_date,
        0 as total_non_cancelled_tickets,
        0 as total_current_year_non_cancelled_tickets,
        NULL as institution_internal_iris_id,
        NULL as institution_academy_name,
        NULL as institution_region_name,
        NULL as institution_department_code,
        NULL AS institution_department_name,
        NULL as institution_postal_code,
        NULL as institution_city,
        NULL as institution_epci,
        NULL as institution_density_label,
        NULL as institution_macro_density_label,
        NULL AS institution_density_level,
        TRUE as collective_offer_is_template
    from {{ source('raw','applicative_database_collective_offer_template') }}
)
