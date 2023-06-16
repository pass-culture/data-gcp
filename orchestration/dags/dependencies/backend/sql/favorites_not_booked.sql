WITH favorites as (
    SELECT
        DISTINCT favorite.userId as user_id,
        offerId as offer_id,
        offer.offer_name,
        offer.offer_subcategoryId as subcategory,
        (
            SELECT
                count(*)
            FROM
                `{{ bigquery_analytics_dataset }}.enriched_booking_data`
            WHERE
                offer_subcategoryId = offer.offer_subcategoryId
                AND user_id = favorite.userId
        ) as user_bookings_for_this_subcat,
    FROM
        `{{ bigquery_clean_dataset }}.applicative_database_favorite` as favorite
        LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_booking_data` as booking ON favorite.userId = booking.user_id
        AND favorite.offerId = booking.offer_id
        JOIN `{{ bigquery_analytics_dataset }}.enriched_offer_data` as offer ON favorite.offerId = offer.offer_id
        JOIN `{{ bigquery_clean_dataset }}.applicative_database_stock` as stock ON favorite.offerId = stock.offer_id
        JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` as enruser ON favorite.userId = enruser.user_id
        JOIN `{{ bigquery_analytics_dataset }}.subcategories` AS subcategories ON subcategories.id = offer.offer_subcategoryId

    WHERE
        dateCreated <= DATE_SUB("{{ yesterday() }}", INTERVAL 7 DAY)
        AND dateCreated > DATE_SUB("{{ yesterday() }}", INTERVAL 14 DAY)
        AND booking.offer_id IS NULL
        AND booking.user_id IS NULL
        AND offer.offer_is_bookable = True
        AND ( stock.stock_beginning_date > "{{ yesterday() }}" OR stock.stock_beginning_date is NULL)
        AND enruser.user_is_current_beneficiary = True
        AND enruser.last_booking_date >= DATE_SUB("{{ yesterday() }}", INTERVAL 7 DAY)
        AND (
            enruser.user_theoretical_remaining_credit
        ) > stock.stock_price
        AND (
                (subcategories.is_digital_deposit AND (100 - enruser.last_deposit_theoretical_amount_spent_in_digital_goods) > stock.stock_price)
                OR NOT subcategories.is_digital_deposit
            )
)
SELECT
    CAST("{{ today() }}" AS DATETIME) as execution_date,
    user_id,
    ARRAY_AGG(
        STRUCT(
            offer_id,
            offer_name,
            subcategory,
            user_bookings_for_this_subcat
        )
        ORDER BY
            user_bookings_for_this_subcat ASC
        LIMIT
            1
    ) [OFFSET(0)].*
FROM
    favorites
GROUP BY
    user_id