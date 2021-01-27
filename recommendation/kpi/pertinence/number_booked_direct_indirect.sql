WITH scrolls AS (SELECT server_time, user_id_dehumanized
	FROM `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action` a
	INNER JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_visit` b
	ON a.idvisit = b.idvisit
	WHERE idaction_event_action = 4394836
	AND (idaction_url=4394835 OR idaction_url=150307)
),
bookings AS (
	SELECT userId, id AS offerId, CAST(dateCreated AS TIMESTAMP) AS bookingDate
	FROM `pass-culture-app-projet-test.data_analytics.booking`
),
recommended_offers AS (
	SELECT userId, offerId, date
	FROM `pass-culture-app-projet-test.algo_reco_kpi_data.past_recommended_offers`
),
offers AS (
    SELECT id as offerId, type as offerType, url FROM `pass-culture-app-projet-test.data_analytics.offer`
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
SELECT TIMESTAMP_DIFF(bookings.bookingDate, viewed_recos.recoDate, SECOND) AS time_between_reco_and_booking, viewed_recos.userId, viewed_recos.offerId, offerType, url
FROM viewed_recos
INNER JOIN bookings
ON viewed_recos.userId = bookings.userId AND viewed_recos.offerId = bookings.offerId
INNER JOIN offers
ON viewed_recos.offerId = offers.offerId
WHERE TIMESTAMP_DIFF(bookings.bookingDate, viewed_recos.recoDate, SECOND) >= 0;
