WITH favorites as (
    SELECT DISTINCT
    favorite.userId as user_id,
        offerId as offer_id,
        offer.offer_name,
        offer.offer_subcategoryId as subcategory,
        (
            SELECT count(*)
            FROM `{{ bigquery_analytics_dataset }}.enriched_booking_data`
            WHERE offer_subcategoryId = offer.offer_subcategoryId
            AND user_id = favorite.userId
        ) as user_bookings_for_this_subcat
    FROM `{{ bigquery_analytics_dataset }}.applicative_database_favorite` as favorite
    LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_booking_data` as booking
        ON favorite.userId = booking.user_id AND favorite.offerId = booking.offer_id
    JOIN `{{ bigquery_analytics_dataset }}.enriched_offer_data` as offer
        ON favorite.offerId = offer.offer_id
    WHERE dateCreated < DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
        AND booking.offer_id IS NULL AND booking.user_id IS NULL
        AND offer.offer_is_bookable = True
)
SELECT
    CURRENT_DATE() as table_creation_day,
    user_id,
    ARRAY_AGG(
        STRUCT(offer_id,offer_name, subcategory, user_bookings_for_this_subcat)
        ORDER BY user_bookings_for_this_subcat ASC LIMIT 1
    )[OFFSET(0)].*
FROM favorites
GROUP BY user_id