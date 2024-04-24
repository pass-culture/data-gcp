WITH add_domains AS(
SELECT 
    collective_offer_template_id as collective_offer_id
    ,STRING_AGG(educational_domain_name) as educational_domains
FROM `{{ bigquery_raw_dataset }}`.`applicative_database_collective_offer_template_domain`
LEFT JOIN `{{ bigquery_raw_dataset }}`.`applicative_database_educational_domain` USING(educational_domain_id)
GROUP BY 1
)

SELECT
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
    collective_offer_subcategory_id,
    collective_offer_date_updated,
    collective_offer_students,
    collective_offer_price_detail,
    collective_offer_booking_email,
    collective_offer_contact_email,
    collective_offer_contact_phone,
    collective_offer_offer_venue,
    collective_offer_last_validation_type,
    collective_offer_image_id,
    educational_domains,
    collective_offer_venue_address_type,
    DATE_ADD(CURRENT_DATE(), INTERVAL -1 DAY) as partition_date
FROM
    `{{ bigquery_raw_dataset }}`.`applicative_database_collective_offer_template` template
LEFT JOIN add_domains ON template.collective_offer_id=add_domains.collective_offer_id