{% snapshot snapshot__applicative_collective_offer_template %}

{{
    config(
      strategy='timestamp',
      unique_key='collective_offer_id',
      updated_at='collective_offer_date_updated'
    )
}}

WITH add_domains AS(
		SELECT
    		collective_offer_template_id as collective_offer_id
    		,STRING_AGG(educational_domain_name) as educational_domains
FROM {{ source('raw','applicative_database_collective_offer_template_domain') }}
LEFT JOIN {{ source('raw','applicative_database_educational_domain') }} USING(educational_domain_id)
GROUP BY 1
),
formated_offer_template as (
    SELECT
    	template.collective_offer_last_validation_date,
    	template.collective_offer_validation,
    	template.collective_offer_id,
    	template.collective_offer_is_active,
    	template.venue_id,
    	template.collective_offer_name,
    	template.collective_offer_creation_date,
    	template.collective_offer_subcategory_id,
    	cast(template.collective_offer_date_updated as timestamp) as collective_offer_date_updated,
    	template.collective_offer_students,
    	template.collective_offer_booking_email,
    	template.collective_offer_offer_venue,
    	template.collective_offer_last_validation_type,
    	add_domains.educational_domains,
    	template.collective_offer_venue_address_type
    FROM {{ source('raw','applicative_database_collective_offer_template') }} as template
	LEFT JOIN add_domains ON template.collective_offer_id = add_domains.collective_offer_id
)
select * from formated_offer_template

{% endsnapshot %}
