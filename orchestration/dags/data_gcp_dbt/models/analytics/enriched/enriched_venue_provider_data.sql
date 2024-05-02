SELECT 
  provider.provider_id
  , provider.provider_name
  , provider.is_active as is_active
  , venue_provider.venue_id as venue_id
  , venue.venue_name
  , venue_department_code
  , venue_creation_date
  , venue_is_permanent
  , venue_label
  , last_sync_date
  , creation_date
  , count(distinct offer_id) as offer_sync_cnt
  , count(distinct collective_offer_id) as collective_offer_sync_cnt
  , min(offer_creation_date) as first_offer_sync_date
  , min(collective_offer_creation_date) as first_collective_offer_sync_date
FROM {{ ref('provider') }} provider
JOIN {{ ref('venue_provider') }} venue_provider
USING(provider_id)
LEFT JOIN {{ ref('venue') }} venue
USING(venue_id)
LEFT JOIN {{ ref('venue_label') }} label
USING(venue_label_id)
LEFT JOIN {{ ref('offer') }} offer
ON venue.venue_id = offer.venue_id
AND provider.provider_id = offer.offer_last_provider_id 
LEFT JOIN {{ ref('collective_offer') }} collective_offer
ON venue.venue_id = collective_offer.venue_id
AND provider.provider_id = collective_offer.provider_id 
GROUP BY 
  provider.provider_id
  , provider.provider_name 
  , provider.is_active
  , venue_provider.venue_id
  , venue.venue_name
  , venue_department_code
  , venue_creation_date
  , venue_is_permanent
  , venue_label
  , last_sync_date
  , creation_date