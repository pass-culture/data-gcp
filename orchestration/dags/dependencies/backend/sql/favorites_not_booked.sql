WITH favorites as (
    SELECT DISTINCT
    favorite.userId,
        offerId,
        offer.offer_subcategoryId as subcat,
        (
            SELECT count(*)
            FROM `passculture-data-ehp.analytics_dev.enriched_booking_data`
            WHERE offer_subcategoryId = offer.offer_subcategoryId
            AND user_id = favorite.userId
        ) as user_bookings_for_this_subcat
    FROM `passculture-data-ehp.analytics_dev.applicative_database_favorite` as favorite
    LEFT JOIN `passculture-data-ehp.analytics_dev.enriched_booking_data` as booking
        ON favorite.userId = booking.user_id AND favorite.offerId = booking.offer_id
    JOIN `passculture-data-ehp.analytics_dev.enriched_offer_data` as offer
        ON favorite.offerId = offer.offer_id
    WHERE dateCreated < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND booking.offer_id IS NULL AND booking.user_id IS NULL
        AND offer.offer_is_bookable = True
)
SELECT
    CURRENT_DATE() as table_creation_day,
    userId,
    ARRAY_AGG(
        STRUCT(offerId, subcat, user_bookings_for_this_subcat)
        ORDER BY user_bookings_for_this_subcat DESC LIMIT 1
    )[OFFSET(0)].*
FROM favorites
GROUP BY userId