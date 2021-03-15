WITH scrolls AS (
    SELECT server_time, user_id_dehumanized
	FROM `passculture-data-prod.clean_prod.log_link_visit_action_preprocessed` llvap
	JOIN `passculture-data-prod.clean_prod.matomo_visits` lvp
	ON lvp.idvisit = llvap.idvisit
	WHERE llvap.idaction_event_action = 4394836                 --4394836 = AllModulesSeen
	AND (idaction_url=4394835 OR idaction_url=150307)           --4394835 & 150307 = page d'accueil
    AND llvap.server_time >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND llvap.server_time < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)        -- pour gérer la période d'AB testing
),
bookings AS (
	SELECT user_id, offer_id AS offerId, CAST(booking_creation_date AS TIMESTAMP) AS bookingDate
	FROM `passculture-data-prod.analytics_prod.applicative_database_booking`b
	JOIN `passculture-data-prod.analytics_prod.applicative_database_stock` s
	ON s.stock_id = b.stock_id
	WHERE bookingDate >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
	AND bookingDate < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)          -- pour gérer la période d'AB testing
),
recommended_offers AS (
	SELECT userId, offerId, date
	FROM `passculture-data-prod.raw_prod.past_recommended_offers`
),
offers AS (
    SELECT offer_id as offerId, offer_type as offerType, offer_url FROM `passculture-data-prod.analytics_prod.applicative_database_offer`
),
viewed_recos AS (
	SELECT * FROM (
	        SELECT CAST(userId AS STRING) AS userId, CAST(offerId AS STRING) AS offerId, date AS recoDate, server_time AS scrollTime,
	        RANK() OVER (PARTITION BY userId, date ORDER BY TIMESTAMP_DIFF(date, server_time, SECOND)) AS timeRank
	        FROM recommended_offers
		    INNER JOIN scrolls
	        ON CAST(recommended_offers.userId AS STRING) = scrolls.user_id_dehumanized
	        WHERE server_time >= date
    ) WHERE timeRank = 1
)
SELECT TIMESTAMP_DIFF(bookings.bookingDate, viewed_recos.recoDate, SECOND) AS time_between_reco_and_booking, viewed_recos.userId, viewed_recos.offerId, offerType, offer_url
FROM viewed_recos
INNER JOIN bookings
ON viewed_recos.userId = bookings.user_id AND viewed_recos.offerId = bookings.offerId
INNER JOIN offers
ON viewed_recos.offerId = offers.offerId
WHERE TIMESTAMP_DIFF(bookings.bookingDate, viewed_recos.recoDate, SECOND) >= 0;
