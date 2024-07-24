{% snapshot snapshot__offer_history %}

{{
    config(
      strategy='timestamp',
      unique_key='offer_id',
      updated_at='offer_updated_date'
    )
}}

with formated_offer as (
    SELECT
    	offer_modified_at_last_provider_date,
    	offer_id,
    	offer_creation_date,
    	offer_product_id,
    	venue_id,
    	booking_email,
    	offer_is_active,
    	offer_name,
    	offer_is_duo,
    	offer_validation,
    	offer_subcategoryid,
    	cast(offer_date_updated  as timestamp) as offer_updated_date,
    	offer_last_validation_type
    FROM {{ source('raw', 'applicative_database_offer') }}
    WHERE offer_subcategoryid NOT IN ('ACTIVATION_THING', 'ACTIVATION_EVENT')
        AND (
            booking_email != 'jeux-concours@passculture.app'
            OR booking_email IS NULL
        )
)

select * from formated_offer

{% endsnapshot %}