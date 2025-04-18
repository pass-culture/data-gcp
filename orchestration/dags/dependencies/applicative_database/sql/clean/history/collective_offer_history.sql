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
    collective_offer_booking_email,
    collective_offer_description,
    collective_offer_creation_date,
    collective_offer_date_updated,
    collective_offer_students,
    collective_offer_contact_email,
    collective_offer_contact_phone,
    collective_offer_offer_venue,
    collective_offer_last_validation_type,
    date_add(current_date(), interval -1 day) as partition_date
from `{{ bigquery_raw_dataset }}`.`applicative_database_collective_offer`
