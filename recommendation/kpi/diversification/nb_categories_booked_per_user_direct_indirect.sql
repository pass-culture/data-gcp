-- Nombre de catégories par utilisateur réservées, depuis n'importe quel parcours, sur des offres préalablement recommandées, le module ayant été vu.

WITH scrolls AS (
    SELECT
        server_time,
	    user_id_dehumanized
	FROM `passculture-data-prod.clean_prod.log_link_visit_action_preprocessed` llvap
	JOIN `passculture-data-prod.clean_prod.log_visit_preprocessed` lvp
	ON lvp.idvisit = llvap.idvisit
	WHERE llvap.idaction_event_action = 4394836                 --4394836 = AllModulesSeen
	AND (idaction_url=4394835 OR idaction_url=150307)           --4394835 & 150307 = page d'accueil
	AND llvap.server_time >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND llvap.server_time < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)        -- pour gérer la période d'AB testing
), booked_offers AS (
    SELECT
        b.user_id AS user_id,
        o.offer_id AS offer_id,
        o.offer_type,
        b.booking_creation_date
    FROM `passculture-data-prod.analytics_prod.applicative_database_booking` b
    INNER JOIN `passculture-data-prod.analytics_prod.applicative_database_stock` s
        ON b.stock_id = s.stock_id
    INNER JOIN `passculture-data-prod.analytics_prod.applicative_database_offer` o
        ON o.offer_id = s.offer_id
    WHERE b.booking_creation_date >= PARSE_DATETIME('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND b.booking_creation_date < PARSE_DATETIME('%Y%m%d',@DS_END_DATE)          -- pour gérer la période d'AB testing
), recommended_offers AS (
	SELECT
        userId,
        offerId,
        date
	FROM `passculture-data-prod.raw_prod.past_recommended_offers`
), viewed_recommended_offers AS (
	SELECT
        *
    FROM (
        SELECT
            CAST(ro.userId AS STRING) AS user_id,
            CAST(ro.offerId AS STRING) AS offer_id,
            ro.date AS reco_date,
            s.server_time AS scroll_time,
            RANK() OVER (PARTITION BY ro.userId, ro.date ORDER BY TIMESTAMP_DIFF(ro.date, s.server_time, SECOND)) AS time_rank
	    FROM recommended_offers ro
		INNER JOIN scrolls s
	    ON CAST(ro.userId AS STRING) = s.user_id_dehumanized
	    WHERE s.server_time >= ro.date
    )
    WHERE time_rank = 1
), viewed_recommended_and_booked AS (
    SELECT
        vro.user_id,
        vro.offer_id,
        bo.offer_type
    FROM booked_offers bo
    INNER JOIN viewed_recommended_offers vro
        ON vro.user_id = bo.user_id AND vro.offer_id = bo.offer_id
    WHERE bo.booking_creation_date > CAST(vro.reco_date AS DATETIME)
    GROUP BY vro.user_id, vro.offer_id, bo.offer_type
), number_types_booked_by_user AS (
  SELECT
      user_id,
      COUNT(DISTINCT(offer_type)) AS number_type_booked
  FROM viewed_recommended_and_booked
  GROUP BY user_id
)
SELECT
    AVG(number_type_booked) as average,
    MAX(number_type_booked) as max,
    MIN(number_type_booked) as min
FROM number_types_booked_by_user
