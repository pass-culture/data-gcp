WITH scrolls AS (SELECT server_time, user_id,
	IF(
	    REGEXP_CONTAINS(user_id, r"^[A-Z0-9]{2,}") = True,
	    algo_reco_kpi_data.dehumanize_id(REGEXP_EXTRACT(user_id, r"^[A-Z0-9]{2,}")),
	    '') AS user_id_dehumanized
	FROM `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action` a
	LEFT JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_visit` b
	ON a.idvisit = b.idvisit
	WHERE idaction_event_action = 4394836
	AND (idaction_url=4394835 OR idaction_url=150307)
),
bookings AS (
	SELECT userId, id AS offerId, CAST(dateCreated AS TIMESTAMP) AS booking_date
	FROM `pass-culture-app-projet-test.applicative_database.booking`
),
recommended_offers AS (
	SELECT userId, offerId, date
	FROM `pass-culture-app-projet-test.algo_reco_kpi_data.past_recommended_offers`
),
offers AS (
    SELECT id as offerId, type as offerType, url FROM `pass-culture-app-projet-test.applicative_database.offer`
),
viewed_recos AS (
	SELECT * FROM (
	        SELECT CAST(userId AS STRING) AS userId, CAST(offerId AS STRING) AS offerId, date AS reco_date, server_time AS scroll_time,
	        RANK() OVER (PARTITION BY userId, date ORDER BY TIMESTAMP_DIFF(date, server_time, SECOND)) AS time_rank
	        FROM recommended_offers
		    LEFT JOIN scrolls
	        ON CAST(recommended_offers.userId AS STRING) = scrolls.user_id_dehumanized
	        WHERE server_time >= date
    ) WHERE time_rank = 1
)
SELECT TIMESTAMP_DIFF(bookings.booking_date, viewed_recos.reco_date, SECOND) AS time_between_reco_and_booking, viewed_recos.userId, viewed_recos.offerId, offerType, url
FROM viewed_recos
LEFT JOIN bookings
ON viewed_recos.userId = bookings.userId AND viewed_recos.offerId = bookings.offerId
LEFT JOIN offers
ON viewed_recos.offerId = offers.offerId
WHERE TIMESTAMP_DIFF(bookings.booking_date, viewed_recos.reco_date, SECOND) >= 0;
