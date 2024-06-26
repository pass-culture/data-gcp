-- Get permanent venues with activ template offers --
WITH offerer_offer_info AS
  (SELECT o.offerer_id ,
          o.venue_id ,
          v.venue_latitude ,
          v.venue_longitude ,
          o.collective_offer_id
   FROM {{ ref('enriched_collective_offer_data') }}  o
   JOIN {{ ref('venue') }} v ON v.venue_id=o.venue_id
   AND venue_is_permanent IS TRUE
   JOIN
    {{ source('raw', 'applicative_database_collective_offer_template') }} t ON t.collective_offer_id=o.collective_offer_id
     AND collective_offer_venue_address_type != "school"
   WHERE offer_is_template IS TRUE
     AND o.collective_offer_is_active ), 

-- Get institutions
institution_info AS
  (SELECT institution_id ,
          institution_rural_level ,
          institution_latitude ,
          institution_longitude
   FROM {{ ref('enriched_institution_data') }}),

-- CROSS JOIN
calculate_distance AS
  (SELECT i.institution_id ,
          i.institution_rural_level ,
          o.offerer_id ,
          o.venue_id ,
          o.collective_offer_id ,
          ST_Distance(ST_geogPoint(venue_longitude, venue_latitude), ST_geogPoint(institution_longitude, institution_latitude)) AS distance
   FROM institution_info i
   CROSS JOIN offerer_offer_info o) 

-- Filter < 300km
SELECT institution_id ,
       institution_rural_level ,
       offerer_id ,
       venue_id ,
       collective_offer_id ,
       SAFE_DIVIDE(distance, 1000) as distance_in_km
FROM calculate_distance
WHERE distance < 300000
GROUP BY 1,
         2,
         3,
         4,
         5,
         6
