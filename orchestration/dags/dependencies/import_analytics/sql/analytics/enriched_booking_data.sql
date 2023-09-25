WITH booking_ranking_in_category_view AS (
    SELECT
        booking.booking_id,
        rank() OVER (
            PARTITION BY booking.user_id,
            offer.offer_subcategoryId
            ORDER BY
                booking.booking_creation_date
        ) AS same_category_booking_rank
    FROM
        `{{ bigquery_clean_dataset }}`.booking AS booking
        INNER JOIN `{{ bigquery_clean_dataset }}`.applicative_database_stock AS stock ON booking.stock_id = stock.stock_id
        INNER JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offer AS offer ON stock.offer_id = offer.offer_id
    ORDER BY
        booking.booking_id
)

SELECT
    booking.booking_id,
    booking.booking_creation_date,
    booking.booking_quantity,
    booking.booking_amount,
    booking.booking_status,
    booking.booking_is_cancelled,
    booking.booking_is_used,
    booking.booking_cancellation_date,
    booking.booking_cancellation_reason,
    stock.stock_beginning_date,
    stock.stock_id,
    offer.offer_id,
    offer.offer_subcategoryId,
    subcategories.category_id AS offer_category_id,
    offer.offer_name,
    venue.venue_name,
    venue_label.label as venue_label_name,
    venue.venue_type_code as venue_type_name,
    venue.venue_id,
    venue.venue_department_code,
    offerer.offerer_id,
    offerer.offerer_name,
    booking.user_id,
    booking.deposit_id,
    deposit.type AS deposit_type,
    user.user_department_code,
    user.user_creation_date,
    user.user_activity,
    booking.reimbursed,
    subcategories.is_physical_deposit as physical_goods,
    subcategories.is_digital_deposit digital_goods,
    subcategories.is_event as event,
    booking.booking_intermediary_amount,
    booking.booking_rank,
    booking_ranking_in_category_view.same_category_booking_rank,
    booking_used_date
FROM
    `{{ bigquery_clean_dataset }}`.booking AS booking
    INNER JOIN `{{ bigquery_clean_dataset }}`.applicative_database_stock AS stock ON booking.stock_id = stock.stock_id
    LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offer AS offer ON offer.offer_id = stock.offer_id
    INNER JOIN `{{ bigquery_clean_dataset }}`.applicative_database_venue AS venue ON venue.venue_id = offer.venue_id
    INNER JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offerer AS offerer ON venue.venue_managing_offerer_id = offerer.offerer_id
    INNER JOIN `{{ bigquery_clean_dataset }}`.user_beneficiary AS user ON user.user_id = booking.user_id
    INNER JOIN `{{ bigquery_clean_dataset }}`.applicative_database_deposit AS deposit ON deposit.id = booking.deposit_id
    LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_venue_label AS venue_label ON venue.venue_label_id = venue_label.venue_label_id
    INNER JOIN `{{ bigquery_analytics_dataset }}`.subcategories subcategories ON offer.offer_subcategoryId = subcategories.id
    LEFT JOIN booking_ranking_in_category_view ON booking_ranking_in_category_view.booking_id = booking.booking_id
;