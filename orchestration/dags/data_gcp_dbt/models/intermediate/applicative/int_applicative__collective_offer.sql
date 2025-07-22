with
    collective_stocks_grouped_by_collective_offers as (
        select
            collective_offer_id,
            max(collective_stock_is_bookable) as collective_stock_is_bookable,  -- bookable = if at least one collective_stock is_bookable
            sum(total_collective_bookings) as total_collective_bookings,
            sum(
                total_non_cancelled_collective_bookings
            ) as total_non_cancelled_collective_bookings,
            sum(total_used_collective_bookings) as total_used_collective_bookings,
            min(first_collective_booking_date) as first_collective_booking_date,
            max(last_collective_booking_date) as last_collective_booking_date,
            sum(
                total_collective_theoretic_revenue
            ) as total_collective_theoretic_revenue,
            sum(total_collective_real_revenue) as total_collective_real_revenue,
            sum(
                total_collective_current_year_real_revenue
            ) as total_collective_current_year_real_revenue,
            sum(
                case
                    when total_non_cancelled_collective_bookings > 0
                    then collective_stock_number_of_tickets
                end
            ) as total_non_cancelled_tickets,
            sum(
                case
                    when total_current_year_non_cancelled_collective_bookings > 0
                    then collective_stock_number_of_tickets
                end
            ) as total_current_year_non_cancelled_tickets
        from {{ ref("int_applicative__collective_stock") }}
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
            co.collective_offer_is_active,
            co.venue_id,
            co.collective_offer_name,
            co.collective_offer_description,
            co.collective_offer_duration_minutes,
            date(co.collective_offer_creation_date) as collective_offer_creation_date,
            co.collective_offer_creation_date as collective_offer_created_at,
            co.collective_offer_format,
            co.collective_offer_date_updated,
            co.collective_offer_students,
            null as collective_offer_price_detail,
            co.collective_offer_booking_email,
            co.collective_offer_contact_email,
            co.collective_offer_contact_phone,
            null as collective_offer_contact_url,
            null as collective_offer_contact_form,
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
            null as collective_offer_template_beginning_date,
            null as collective_offer_template_ending_date,
            coalesce(
                cs.collective_stock_is_bookable
                and co.collective_offer_is_active
                and co.collective_offer_validation = "APPROVED",
                false
            ) as collective_offer_is_bookable,
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
            ei.institution_internal_iris_id,
            ei.institution_academy_name,
            ei.institution_region_name,
            ei.institution_department_code,
            ei.institution_department_name,
            ei.institution_postal_code,
            ei.institution_city,
            ei.institution_epci,
            ei.institution_density_label,
            ei.institution_macro_density_label,
            ei.institution_density_level,
            co.collective_offer_rejection_reason,
            case
                when co.collective_offer_location_type = "TO_BE_DEFINED"
                then "other"
                when co.collective_offer_location_type = "ADDRESS"
                then "offerer"
                when co.collective_offer_location_type = "SCHOOL"
                then "school"
            end as collective_offer_location_type,
            false as collective_offer_is_template
        from {{ source("raw", "applicative_database_collective_offer") }} as co
        left join
            collective_stocks_grouped_by_collective_offers as cs
            on co.collective_offer_id = cs.collective_offer_id
        left join
            {{ ref("int_applicative__educational_institution") }} as ei
            on co.institution_id = ei.educational_institution_id

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
        collective_offer_is_active,
        venue_id,
        collective_offer_name,
        collective_offer_description,
        collective_offer_duration_minutes,
        date(collective_offer_creation_date) as collective_offer_creation_date,
        collective_offer_creation_date as collective_offer_created_at,
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
        null as template_id,
        collective_offer_last_validation_type,
        null as institution_id,
        collective_offer_image_id,
        null as provider_id,
        national_program_id,
        collective_offer_template_beginning_date,
        collective_offer_template_ending_date,
        coalesce(
            collective_offer_validation = "APPROVED", false
        ) as collective_offer_is_bookable,
        0 as total_non_cancelled_collective_bookings,
        0 as total_collective_bookings,
        0 as total_used_collective_bookings,
        0 as total_collective_theoretic_revenue,
        0 as total_collective_real_revenue,
        0 as total_collective_current_year_real_revenue,
        null as first_collective_booking_date,
        null as last_collective_booking_date,
        0 as total_non_cancelled_tickets,
        0 as total_current_year_non_cancelled_tickets,
        null as institution_internal_iris_id,
        null as institution_academy_name,
        null as institution_region_name,
        null as institution_department_code,
        null as institution_department_name,
        null as institution_postal_code,
        null as institution_city,
        null as institution_epci,
        null as institution_density_label,
        null as institution_macro_density_label,
        null as institution_density_level,
        collective_offer_rejection_reason,
        case
            when collective_offer_location_type = "TO_BE_DEFINED"
            then "other"
            when collective_offer_location_type = "ADDRESS"
            then "offerer"
            when collective_offer_location_type = "SCHOOL"
            then "school"
        end as collective_offer_location_type,
        true as collective_offer_is_template
    from {{ source("raw", "applicative_database_collective_offer_template") }}
)
