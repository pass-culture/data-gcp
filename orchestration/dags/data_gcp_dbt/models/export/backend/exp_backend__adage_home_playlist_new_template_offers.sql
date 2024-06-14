WITH offerer_offer_info AS -- last collective_offer_template by offerer_id created past 2 months
  (SELECT o.offerer_id ,
          o.venue_id ,
          v.venue_latitude ,
          v.venue_longitude ,
          o.collective_offer_id ,
          o.collective_offer_creation_date ,
   FROM {{ ref('enriched_collective_offer_data') }} o
   LEFT JOIN {{ ref('venue') }} v ON v.venue_id=o.venue_id
   WHERE o.collective_offer_creation_date >= DATE_SUB(current_date(), INTERVAL 2 MONTH) -- uniquement sur les 2 derniers mois
     AND offer_is_template IS TRUE
   QUALIFY ROW_NUMBER() OVER(PARTITION BY offerer_id ORDER BY collective_offer_creation_date DESC) = 1 ),  -- on garde seulement la plus récente


-- Add location of the venue_representation (venue_id of the field venue_adress) when it is different from the initial venue of the offer
add_representation_venue AS
  (SELECT o.* ,
          collective_offer_venue_humanized_id AS venue_v2_id ,
          v.venue_latitude AS venue_v2_latitude ,
          v.venue_longitude AS venue_v2_longitude
   FROM offerer_offer_info o
   LEFT JOIN {{ source('raw', 'applicative_database_collective_offer_template') }} a ON o.collective_offer_id=a.collective_offer_id AND collective_offer_venue_humanized_id IS NOT NULL AND collective_offer_venue_humanized_id != a.venue_id
   LEFT JOIN {{ ref('venue') }} v ON v.venue_id=a.collective_offer_venue_humanized_id),

-- Get institutions
institution_info AS
  (SELECT institution_id ,
          institution_rural_level ,
          institution_latitude ,
          institution_longitude
   FROM {{ ref('enriched_institution_data') }} id),

-- CROSS JOIN
calculate_distance AS
  (SELECT i.institution_id ,
          i.institution_rural_level ,
          o.offerer_id ,
          o.venue_id ,
          o.venue_v2_id ,
          o.collective_offer_id ,
          o.collective_offer_creation_date ,
          ST_Distance(ST_geogPoint(venue_longitude, venue_latitude), ST_geogPoint(institution_longitude, institution_latitude)) AS distance ,
          ST_Distance(ST_geogPoint(venue_v2_longitude, venue_v2_latitude), ST_geogPoint(institution_longitude, institution_latitude)) AS distance_v2
   FROM institution_info i
   CROSS JOIN add_representation_venue o)

-- Filter < 300km
SELECT institution_id ,
       institution_rural_level ,
       offerer_id ,
       venue_id ,
       collective_offer_id ,
       collective_offer_creation_date ,
       SAFE_DIVIDE(distance, 1000) as distance_in_km ,
       SAFE_DIVIDE(distance_v2, 1000) as distance_v2_in_km
FROM calculate_distance
WHERE distance < 300000
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7,
         8