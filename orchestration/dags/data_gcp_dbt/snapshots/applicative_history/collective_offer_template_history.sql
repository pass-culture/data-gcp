{% snapshot collective_offer_template_history %}
    
{{
    config(
      strategy='check',
      unique_key='venue_id',
      check_cols=['collective_offer_audio_disability_compliant', 'collective_offer_mental_disability_compliant', 'collective_offer_motor_disability_compliant', 'collective_offer_visual_disability_compliant', 'collective_offer_last_validation_date', 'collective_offer_validation', 'collective_offer_id', 'collective_offer_is_active', 'venue_id', 'collective_offer_name', 'collective_offer_description', 'collective_offer_creation_date', 'collective_offer_subcategory_id', 'collective_offer_date_updated', 'collective_offer_students', 'collective_offer_price_detail', 'collective_offer_booking_email', 'collective_offer_contact_email', 'collective_offer_contact_phone', 'collective_offer_offer_venue', 'collective_offer_last_validation_type', 'collective_offer_image_id', 'educational_domains', 'collective_offer_venue_address_type']
    )
}}

SELECT
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
	collective_offer_venue_address_type
FROM {{ ref('collective_offer_template') }}

{% endsnapshot %}