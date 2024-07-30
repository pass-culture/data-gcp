SELECT p.provider_id,
    p.provider_name,
    p.is_active,
    p.venue_id,
    p.last_sync_date,
    p.creation_date,
    v.venue_name,
    v.venue_department_code,
    v.venue_creation_date,
    v.venue_is_permanent,
    v.venue_label,
    COUNT(DISTINCT o.offer_id) AS total_individual_offers,
    COUNT(DISTINCT co.collective_offer_id) AS total_collective_offers,
    MIN(offer_creation_date) AS first_individual_offer_creation_date,
    MIN(collective_offer_creation_date) AS first_collective_offer_creation_date
FROM {{ ref('int_applicative__venue_provider') }} AS p
LEFT JOIN {{ ref('int_global__venue') }} AS v ON p.venue_id = v.venue_id
LEFT JOIN {{ ref('int_applicative__offer') }} AS o ON o.venue_id = v.venue_id AND o.offer_last_provider_id = p.provider_id
LEFT JOIN {{ ref('int_applicative__collective_offer') }} AS co ON co.provider_id = p.provider_id AND co.venue_id = v.venue_id
GROUP BY provider_id,
  provider_name,
  is_active,
  venue_id,
  venue_name,
  venue_department_code,
  venue_creation_date,
  venue_is_permanent,
  venue_label,
  last_sync_date,
  creation_date
