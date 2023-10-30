-- Les acteurs culturels ayant mis en ligne leur première offre vitrine, ces 2 derniers mois
WITH offerer_offer_info AS (
  SELECT 
    o.offerer_id
    ,o.venue_id
    ,v.venue_latitude
    ,v.venue_longitude
    ,o.collective_offer_id
    ,MIN(collective_offer_creation_date) OVER (PARTITION BY offerer_id) as first_template_offer_creation_date
  FROM `{{ bigquery_analytics_dataset }}`.enriched_collective_offer_data o
  LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_venue v ON v.venue_id=o.venue_id

  WHERE collective_offer_creation_date >= DATE_SUB(current_date(), INTERVAL 2 MONTH) -- à adapter
  AND offer_is_template IS TRUE

  QUALIFY ROW_NUMBER() OVER(PARTITION BY offerer_id ORDER BY collective_offer_creation_date ASC) = 1
),

-- Get institutions
institution_info AS (
  SELECT 
    institution_id
	,institution_rural_level
    ,institution_latitude
    ,institution_longitude
  FROM `{{ bigquery_analytics_dataset }}`.enriched_institution_data id
),
-- CROSS JOIN 
calculate_distance AS (
  SELECT 
    i.institution_id
		,i.institution_rural_level
    ,o.offerer_id
    ,o.venue_id 
    ,o.collective_offer_id
    ,ST_Distance(ST_geogPoint(venue_longitude,venue_latitude), ST_geogPoint(institution_longitude,institution_latitude)) as distance
  FROM institution_info i
  CROSS JOIN offerer_offer_info o

)

-- Filter < 300km 
SELECT * 
FROM calculate_distance
WHERE distance < 300000 -- 300km au min