{% snapshot collective_offer_template_history %}

{{
    config(
      strategy='timestamp',
      unique_key='collective_offer_id',
      updated_at='collective_offer_date_updated'
    )
}}
with formated_offer_template as (
    SELECT
    	collective_offer_last_validation_date,
    	collective_offer_validation,
    	collective_offer_id,
    	collective_offer_is_active,
    	venue_id,
    	collective_offer_name,
    	collective_offer_creation_date,
    	collective_offer_subcategory_id,
    	cast(collective_offer_date_updated as timestamp) as collective_offer_date_updated,
    	collective_offer_students,
    	collective_offer_booking_email,
    	collective_offer_offer_venue,
    	collective_offer_last_validation_type,
    	educational_domains,
    	collective_offer_venue_address_type
    FROM {{ source('raw','applicative_database_collective_offer_template') }}
)
select * from formated_offer_template

{% endsnapshot %}
