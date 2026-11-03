with
    add_domains as (
        select
            collective_offer_template_id as collective_offer_id,
            string_agg(educational_domain_name) as educational_domains
        from
            `{{ bigquery_raw_dataset }}`.`applicative_database_collective_offer_template_domain`
        left join
            `{{ bigquery_raw_dataset }}`.`applicative_database_educational_domain`
            using (educational_domain_id)
        group by 1
    )

select
    collective_offer_audio_disability_compliant,
    collective_offer_mental_disability_compliant,
    collective_offer_motor_disability_compliant,
    collective_offer_visual_disability_compliant,
    collective_offer_last_validation_date,
    collective_offer_validation,
    template.collective_offer_id,
    collective_offer_is_active,
    venue_id,
    collective_offer_name,
    collective_offer_description,
    collective_offer_creation_date,
    collective_offer_date_updated,
    collective_offer_students,
    collective_offer_price_detail,
    collective_offer_booking_email,
    collective_offer_contact_email,
    collective_offer_contact_phone,
    cast(null as string) as collective_offer_offer_venue,
    collective_offer_last_validation_type,
    collective_offer_image_id,
    educational_domains,
    cast(null as string) as collective_offer_venue_address_type,
    date_add(current_date(), interval -1 day) as partition_date
from
    `{{ bigquery_raw_dataset }}`.`applicative_database_collective_offer_template` template
left join add_domains on template.collective_offer_id = add_domains.collective_offer_id
