-- Get offerers/venues which published their first template offer, last 2 months
WITH offerer_offer_info AS
  (SELECT o.offerer_id ,
          o.venue_id ,
          v.venue_latitude ,
          v.venue_longitude ,
          o.collective_offer_id ,
          o.collective_offer_creation_date ,
          MIN(collective_offer_creation_date) OVER (PARTITION BY offerer_id) AS first_template_offer_creation_date
   FROM `{{ bigquery_analytics_dataset }}`.enriched_collective_offer_data o
   LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_venue v ON v.venue_id=o.venue_id
   WHERE offer_is_template IS TRUE 
   QUALIFY ROW_NUMBER() OVER(PARTITION BY offerer_id ORDER BY collective_offer_creation_date ASC) = 1 ), 

-- Only keep first template offers of last 2 months
offerer_offer_2_month AS
  (SELECT *
   FROM offerer_offer_info
   WHERE collective_offer_creation_date >= DATE_SUB(current_date(), INTERVAL 2 MONTH) ), 

-- Get institutions
institution_info AS
  (SELECT institution_id ,
          institution_rural_level ,
          institution_latitude ,
          institution_longitude
   FROM `{{ bigquery_analytics_dataset }}`.enriched_institution_data id), 

-- CROSS JOIN
calculate_distance AS
  (SELECT i.institution_id ,
          i.institution_rural_level ,
          o.offerer_id ,
          o.venue_id ,
          o.collective_offer_id ,
          o.collective_offer_creation_date ,
          ST_Distance(ST_geogPoint(venue_longitude, venue_latitude), ST_geogPoint(institution_longitude, institution_latitude)) AS distance
   FROM institution_info i
   CROSS JOIN offerer_offer_2_month o) 
   
-- Filter < 300km
SELECT institution_id ,
       institution_rural_level ,
       offerer_id ,
       venue_id ,
       collective_offer_id ,
       collective_offer_creation_date ,
       SAFE_DIVIDE(distance, 1000) as distance_in_km
FROM calculate_distance
WHERE distance < 300000
GROUP BY 1,
         2,
         3,
         4,
         5,
         6,
         7