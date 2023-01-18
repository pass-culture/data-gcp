{{ create_humanize_id_function() }} 

WITH booking_humanized_id AS (
SELECT
    booking_id,
    humanize_id(booking_id) AS humanized_id
FROM
    `{{ bigquery_analytics_dataset }}`.applicative_database_booking
WHERE
    booking_id is not NULL
),

booking_amount_view AS (
    SELECT
        booking.booking_id,
        coalesce(booking.booking_amount, 0) * coalesce(booking.booking_quantity, 0) AS booking_intermediary_amount
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
),

booking_ranking_view AS (
    SELECT
        booking.booking_id,
        rank() OVER (
            PARTITION BY booking.user_id
            ORDER BY
                booking.booking_creation_date
        ) AS booking_rank
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
),

booking_ranking_in_category_view AS (
    SELECT
        booking.booking_id,
        rank() OVER (
            PARTITION BY booking.user_id,
            offer.offer_subcategoryId
            ORDER BY
                booking.booking_creation_date
        ) AS same_category_booking_rank
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
        INNER JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON booking.stock_id = stock.stock_id
        INNER JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON stock.offer_id = offer.offer_id
    ORDER BY
        booking.booking_id
),

booking_intermediary_view AS (
    SELECT
        booking.booking_id,
        booking_amount_view.booking_intermediary_amount,
        booking_ranking_view.booking_rank,
        booking_ranking_in_category_view.same_category_booking_rank
    FROM
        `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
        LEFT JOIN booking_amount_view ON booking_amount_view.booking_id = booking.booking_id
        LEFT JOIN booking_ranking_view ON booking_ranking_view.booking_id = booking.booking_id
        LEFT JOIN booking_ranking_in_category_view ON booking_ranking_in_category_view.booking_id = booking.booking_id
)


SELECT
    booking.booking_id,
    booking.individual_booking_id,
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
    subcategories.category AS offer_category_id,
    offer.offer_name,
    venue.venue_name,
    venue_label.label as venue_label_name,
    venue.venue_type_code as venue_type_name,
    venue.venue_id,
    venue.venue_department_code,
    offerer.offerer_id,
    offerer.offerer_name,
    individual_booking.user_id,
    individual_booking.deposit_id,
    deposit.type AS deposit_type,
    user.user_department_code,
    user.user_creation_date,
    CASE
        WHEN user.user_activity in ("Alternant", "Apprenti", "Volontaire") THEN "Apprenti, Alternant, Volontaire en service civique rémunéré"
        WHEN user.user_activity in ("Inactif") THEN "Inactif (ni en emploi ni au chômage), En incapacité de travailler"
        WHEN user.user_activity in ("Étudiant") THEN "Etudiant"
        WHEN user.user_activity in ("Chômeur", "En recherche d'emploi ou chômeur") THEN "Chômeur, En recherche d'emploi"
        ELSE user.user_activity
    END AS user_activity,
    booking_intermediary_view.booking_intermediary_amount,
    CASE
        WHEN booking.booking_status = 'REIMBURSED' THEN True
        ELSE False
    END AS reimbursed,
    subcategories.is_physical_deposit as physical_goods,
    subcategories.is_digital_deposit digital_goods,
    subcategories.is_event as event,
    booking_intermediary_view.booking_rank,
    booking_intermediary_view.same_category_booking_rank,
    booking_used_date
FROM
    `{{ bigquery_analytics_dataset }}`.applicative_database_booking AS booking
    INNER JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock ON booking.stock_id = stock.stock_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer ON offer.offer_id = stock.offer_id
    AND offer.offer_subcategoryId NOT IN ('ACTIVATION_THING', 'ACTIVATION_EVENT')
    INNER JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_venue AS venue ON venue.venue_id = offer.venue_id
    INNER JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_offerer AS offerer ON venue.venue_managing_offerer_id = offerer.offerer_id
    INNER JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_individual_booking AS individual_booking ON individual_booking.individual_booking_id = booking.individual_booking_id
    INNER JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_user AS user ON user.user_id = individual_booking.user_id
    INNER JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_deposit AS deposit ON deposit.id = individual_booking.deposit_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_venue_label AS venue_label ON venue.venue_label_id = venue_label.id
    INNER JOIN `{{ bigquery_analytics_dataset }}`.subcategories subcategories ON offer.offer_subcategoryId = subcategories.id
    LEFT JOIN booking_humanized_id AS booking_humanized_id ON booking_humanized_id.booking_id = booking.booking_id
    LEFT JOIN booking_intermediary_view ON booking_intermediary_view.booking_id = booking.booking_id
;