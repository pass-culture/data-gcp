WITH offerer_venue_info AS (
SELECT 
  b.offerer_id
  ,b.venue_id
  ,b.educational_institution_id as institution_id
  ,id.institution_latitude as venue_latitude
  ,id.institution_longitude as venue_longitude
  ,COUNT(distinct b.collective_booking_id) as nb_booking
FROM `{{ bigquery_analytics_dataset }}`.enriched_collective_booking_data b
INNER JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_collective_offer o ON b.collective_offer_id=o.collective_offer_id
	AND collective_offer_offer_venue LIKE '%"addressType": "school"%'
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_institution_data id on id.institution_id=b.educational_institution_id
WHERE collective_booking_status IN ("CONFIRMED","REIMBURSED","USED")
AND collective_booking_creation_date >= DATE_SUB(current_date(), INTERVAL 12 MONTH) -- à adapter

GROUP BY 1,2,3,4,5
),

-- Get institutions
institution_info AS (
  SELECT 
    id.institution_id
		,id.institution_rural_level
    ,il.institution_latitude
    ,il.institution_longitude
  FROM `{{ bigquery_analytics_dataset }}`.enriched_institution_data id
  INNER JOIN  `{{ bigquery_analytics_dataset }}`.institution_locations il on il.institution_id= id.institution_external_id

),

-- Get all offerers with at least one reservations at less than 300KM.
ac_moving AS (
SELECT 
  i.institution_id
	,i.institution_rural_level
  ,o.offerer_id
  ,o.venue_id
  ,o.nb_booking
  ,o.venue_latitude
  ,o.venue_longitude
FROM offerer_venue_info  o
INNER JOIN institution_info i ON i.institution_id = o.institution_id
WHERE ST_Distance(ST_geogPoint(o.venue_longitude,o.venue_latitude), ST_geogPoint(i.institution_longitude,i.institution_latitude)) < 300000 -- à modifier
AND nb_booking >= 1
),

-- For all institutions, get all offerers 
all_institutions AS (
SELECT 
  i.institution_id
	,i.institution_rural_level
  , m.offerer_id as offerer_id 
  , m.venue_id
  , ST_Distance(ST_geogPoint(m.venue_longitude,m.venue_latitude), ST_geogPoint(i.institution_longitude,i.institution_latitude)) as distance
  , m.nb_booking as reserved_nb_booking
  , m.institution_id as reserved_institution_id


FROM institution_info i
CROSS JOIN ac_moving m
-- not the same institution
WHERE i.institution_id != m.institution_id
)

-- Filter < 300KM
SELECT 
	institution_id, 
	institution_rural_level,
	offerer_id,
	venue_id, 
	distance,
	sum(reserved_nb_booking) as total_offerer_bookings,
  count(distinct reserved_institution_id) as total_offerer_institutions
	
FROM all_institutions 
WHERE distance < 300000
GROUP BY 1,2,3,4,5
LIMIT 10

