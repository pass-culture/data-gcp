select
    offer_modified_at_last_provider_date,
    offer_id,
    offer_creation_date,
    offer_product_id,
    venue_id,
    booking_email,
    offer_is_active,
    offer_name,
    coalesce(p.description, o.offer_description) as offer_description,
    offer_url,
    0 as offer_duration_minutes,
    offer_extra_data,
    offer_is_duo,
    offer_audio_disability_compliant,
    offer_mental_disability_compliant,
    offer_motor_disability_compliant,
    offer_visual_disability_compliant,
    offer_external_ticket_office_url,
    offer_validation,
    offer_subcategoryid,
    offer_updated_date,
    offer_withdrawal_type,
    offer_withdrawal_delay,
    offer_last_validation_type,
    date_add(current_date(), interval -1 day) as partition_date
from `{{ bigquery_raw_dataset }}`.`applicative_database_offer_legacy` o
left join
    `{{ bigquery_raw_dataset }}`.`applicative_database_product` p
    on o.offer_product_id = cast(p.id as string)
