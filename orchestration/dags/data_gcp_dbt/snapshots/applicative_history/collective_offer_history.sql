{% snapshot collective_offer_history %}
    
{{
    config(
      strategy='check',
      unique_key='collective_offer_id',
      check_cols=['collective_offer_last_validation_date', 'collective_offer_validation', 'offer_id', 'collective_offer_is_active', 'venue_id', 'collective_offer_name', 'collective_offer_booking_email', 'collective_offer_description', 'collective_offer_creation_date', 'collective_offer_subcategory_id', 'collective_offer_date_updated', 'collective_offer_students', 'collective_offer_offer_venue', 'collective_offer_last_validation_type']
    )
}}

SELECT
	collective_offer_last_validation_date,
	collective_offer_validation,
	collective_offer_id,
	offer_id,
	collective_offer_is_active,
	venue_id,
	collective_offer_name,
	collective_offer_booking_email,
	collective_offer_description,
	collective_offer_creation_date,
	collective_offer_subcategory_id,
	collective_offer_date_updated,
	collective_offer_students,
	collective_offer_offer_venue,
	collective_offer_last_validation_type
FROM {{ source('raw', 'applicative_database_collective_offer') }}

{% endsnapshot %}