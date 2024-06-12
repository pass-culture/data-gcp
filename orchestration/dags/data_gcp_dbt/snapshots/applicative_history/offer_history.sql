{% snapshot offer_history %}
    
{{
    config(
      strategy='check',
      unique_key='offer_id',
      check_cols=['offer_modified_at_last_provider_date', 'offer_creation_date', 'offer_product_id', 'venue_id', 'booking_email', 'offer_is_active', 'offer_name', 'offer_description', 'offer_url', 'offer_media_urls_hash', 'offer_duration_minutes', 'offer_extra_data_hash', 'offer_is_duo', 'offer_audio_disability_compliant', 'offer_mental_disability_compliant', 'offer_motor_disability_compliant', 'offer_visual_disability_compliant', 'offer_external_ticket_office_url', 'offer_validation', 'offer_subcategoryid', 'offer_date_updated', 'offer_withdrawal_type', 'offer_withdrawal_delay', 'offer_last_validation_type']
    )
}}

WITH offer_rank as (
    SELECT
        *
        , ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY offer_date_updated DESC) as row_number
    FROM {{ source('raw', 'applicative_database_offer') }}
    WHERE offer_subcategoryid NOT IN ('ACTIVATION_THING', 'ACTIVATION_EVENT')
    AND (
        booking_email != 'jeux-concours@passculture.app'
        OR booking_email IS NULL
    )
)   
SELECT
	offer_modified_at_last_provider_date,
	offer_id,
	offer_creation_date,
	offer_product_id,
	venue_id,
	booking_email,
	offer_is_active,
	offer_name,
	offer_description,
	offer_url,
	offer_media_urls,
	offer_duration_minutes,
	offer_extra_data,
	offer_is_duo,
	offer_audio_disability_compliant,
	offer_mental_disability_compliant,
	offer_motor_disability_compliant,
	offer_visual_disability_compliant,
	offer_external_ticket_office_url,
	offer_validation,
	offer_subcategoryid,
	offer_date_updated,
	offer_withdrawal_type,
	offer_withdrawal_delay,
	offer_last_validation_type,
	TO_HEX(SHA256(TRIM(cast(offer_extra_data as string)))) as offer_extra_data_hash,
    TO_HEX(SHA256(ARRAY_TO_STRING(offer_media_urls,"|" ," "))) as offer_media_urls_hash
FROM offer_rank
WHERE row_number=1

{% endsnapshot %}