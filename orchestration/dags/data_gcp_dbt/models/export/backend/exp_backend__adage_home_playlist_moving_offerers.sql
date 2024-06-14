-- Get one random template offer per venue, that can take place at school
WITH random_template_offer_per_venue AS
  (SELECT collective_offer_id,
          collective_offer_creation_date,
          venue_id
   FROM {{ source('raw', 'applicative_database_collective_offer_template') }} o
   WHERE o.collective_offer_venue_address_type = "school"
   AND collective_offer_is_active
   QUALIFY ROW_NUMBER() OVER (PARTITION BY venue_id ORDER BY RAND() )=1),

-- Get venues which moved to educational institutions, last 12 months, and the random active template offer
offerer_venue_info AS
  (SELECT b.offerer_id,
          b.venue_id,
          v.collective_offer_id,
          b.educational_institution_id AS institution_id,
          id.institution_latitude AS venue_moving_latitude,
          id.institution_longitude AS venue_moving_longitude,
          MAX(collective_booking_creation_date) AS last_booking_date,
          COUNT(DISTINCT b.collective_booking_id) AS nb_booking
   FROM {{ ref('enriched_collective_booking_data') }} b
   INNER JOIN {{ source('raw', 'applicative_database_collective_offer') }} o ON b.collective_offer_id=o.collective_offer_id
   AND o.collective_offer_venue_address_type = "school"
   JOIN random_template_offer_per_venue v ON v.venue_id=b.venue_id -- JOIN because we only keep venues that have bookings AND template offer
   LEFT JOIN {{ ref('enriched_institution_data') }} id ON id.institution_id=b.educational_institution_id
   WHERE collective_booking_status IN ("CONFIRMED",
                                       "REIMBURSED",
                                       "USED")
     AND collective_booking_creation_date >= DATE_SUB(current_date(), INTERVAL 12 MONTH)
   GROUP BY 1,
            2,
            3,
            4,
            5,
            6), 

-- Get institutions
institution_info AS
  (SELECT institution_id,
          institution_rural_level,
          institution_latitude,
          institution_longitude
   FROM {{ ref('enriched_institution_data') }} id),

-- Get all venues with at least one reservation at less than 300KM.
ac_moving AS
  (SELECT i.institution_id,
          i.institution_rural_level,
          o.venue_id,
          o.collective_offer_id,
          o.last_booking_date,
          o.nb_booking,
          o.venue_moving_latitude,
          o.venue_moving_longitude
   FROM offerer_venue_info o
   INNER JOIN institution_info i ON i.institution_id = o.institution_id
   WHERE ST_Distance(ST_geogPoint(o.venue_moving_longitude, o.venue_moving_latitude), ST_geogPoint(i.institution_longitude, i.institution_latitude)) < 300000
     AND nb_booking >= 1 ), 

-- For all institutions, get all offerers
all_institutions AS
  (SELECT i.institution_id,
          i.institution_rural_level,
          m.venue_id,
          m.collective_offer_id,
          ST_Distance(ST_geogPoint(m.venue_moving_longitude, m.venue_moving_latitude), ST_geogPoint(i.institution_longitude, i.institution_latitude)) AS distance,
          m.nb_booking AS reserved_nb_booking,
          m.last_booking_date AS last_booking_date,
          m.institution_id AS reserved_institution_id
   FROM institution_info i
   CROSS JOIN ac_moving m -- not the same institution

   WHERE i.institution_id != m.institution_id ) 

-- Filter < 300KM
SELECT institution_id,
       institution_rural_level,
       venue_id,
       collective_offer_id,
       SAFE_DIVIDE(distance, 1000) AS distance_in_km,
       last_booking_date
FROM all_institutions
WHERE distance < 300000
GROUP BY 1,
         2,
         3,
         4,
         5,
         6