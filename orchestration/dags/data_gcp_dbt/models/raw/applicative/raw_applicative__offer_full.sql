{{ config(
    tags="monthly",
    labels={"schedule": "monthly"})
}}

select
    offer_id_at_providers,
    offer_modified_at_last_provider_date,
    offer_id,
    offer_creation_date,
    offer_product_id,
    venue_id,
    offer_last_provider_id,
    booking_email,
    offer_is_active,
    offer_name,
    offer_description,
    offer_url,
    offer_is_national,
    offer_extra_data,
    offer_ean,
    offer_is_duo,
    offer_fields_updated,
    offer_withdrawal_details,
    offer_audio_disability_compliant,
    offer_mental_disability_compliant,
    offer_motor_disability_compliant,
    offer_visual_disability_compliant,
    offer_external_ticket_office_url,
    offer_validation,
    offer_last_validation_type,
    offer_subcategoryid,
    offer_withdrawal_type,
    offer_withdrawal_delay,
    booking_contact,
    offerer_address_id,
    timestamp(offer_updated_date) as offer_updated_date,
    to_hex(md5(to_json_string(offer))) as custom_scd_id
from {{ source("raw", "applicative_database_offer_legacy") }} as offer
