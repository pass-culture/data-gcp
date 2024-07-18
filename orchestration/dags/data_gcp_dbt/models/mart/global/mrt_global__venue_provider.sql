WITH offers_grouped_by_venue_provider AS (

SELECT o.offer_last_provider_id AS provider_id,
  o.venue_id,
  COUNT(DISTINCT o.offer_id) AS total_individual_offers,
  COUNT(DISTINCT co.collective_offer_id) AS total_collective_offers,
  MIN(offer_creation_date) AS first_individual_offer_creation_date,
  MIN(collective_offer_creation_date) AS first_collective_offer_creation_date
FROM {{ ref('int_global__offer') }} AS o
LEFT JOIN {{ ref('mrt_global__collective_offer') }} AS co ON co.provider_id = o.offer_last_provider_id
    AND co.venue_id = o.venue_id
GROUP BY provider_id, venue_id

)

SELECT p.provider_id,
  p.provider_name,
  p.is_active,
  p.venue_id,
  v.venue_name,
  v.venue_region_name,
  v.venue_department_code,
  v.venue_postal_code,
  v.venue_city,
  v.venue_epci,
  v.venue_academy_name,
  v.venue_density_label,
  v.venue_macro_density_label,
  v.venue_creation_date,
  v.venue_is_permanent,
  v.venue_label,
  p.last_sync_date,
  p.creation_date,
  ovp.total_individual_offers,
  ovp.total_collective_offers,
  ovp.first_individual_offer_creation_date,
  ovp.first_collective_offer_creation_date
FROM {{ ref('int_applicative__venue_provider') }} AS p
LEFT JOIN offers_grouped_by_venue_provider AS ovp ON ovp.venue_id = p.venue_id AND ovp.provider_id = p.provider_id
LEFT JOIN {{ ref('int_global__venue') }} AS v ON p.venue_id = v.venue_id
