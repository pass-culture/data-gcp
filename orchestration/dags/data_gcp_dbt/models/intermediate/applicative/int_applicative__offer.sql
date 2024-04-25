WITH stocks_grouped_by_offers AS (
        SELECT offer_id,
            SUM(available_stock) AS available_stock,
            MAX(is_bookable) AS is_bookable,
            SUM(total_bookings) AS total_bookings,
            SUM(total_individual_bookings) AS total_individual_bookings,
            SUM(total_non_cancelled_individual_bookings) AS total_non_cancelled_individual_bookings,
            SUM(total_used_individual_bookings) AS total_used_individual_bookings,
            SUM(total_individual_theoretic_revenue) AS total_individual_theoretic_revenue,
            SUM(total_individual_real_revenue) AS total_individual_real_revenue,
            MIN(first_individual_booking_date) AS first_individual_booking_date,
            MAX(last_individual_booking_date) AS last_individual_booking_date
        FROM {{ ref('int_applicative__stock') }}
        GROUP BY offer_id
),

item_ids AS (
    SELECT
        offer_id,
        CASE WHEN (
            offer_subcategoryId = 'LIVRE_PAPIER'
            AND isbn IS NOT NULL
            AND isbn <> ''
            ) THEN CONCAT('isbn-',isbn)
        WHEN (offer_subcategoryId = 'SEANCE_CINE'
            AND theater_movie_id IS NOT NULL
            AND theater_movie_id <> ''
            ) THEN CONCAT('movie_id-',theater_movie_id)
        WHEN (offer_subcategoryId = 'SUPPORT_PHYSIQUE_MUSIQUE_CD'
            AND ean IS NOT NULL
            AND ean <> ''
            ) THEN CONCAT('cd_id-',ean)
        WHEN (offer_subcategoryId = 'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE'
            AND ean IS NOT NULL
            AND ean <> ''
            ) THEN CONCAT('vinyle_id-',ean)
        ELSE CONCAT('offer-',offer_id) END AS item_id
    FROM {{ ref('int_applicative__extract_offer') }}
),

linked_offers_deduped AS (
    SELECT offer_id, item_linked_id
    FROM {{ source('analytics','linked_offers') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY offer_id ORDER BY rand()) = 1
)

SELECT
    o.offer_id,
    o.offer_id_at_providers,
    o.offer_modified_at_last_provider_date,
    DATE(o.offer_creation_date) AS offer_creation_date,
    o.offer_creation_date AS offer_created_at,
    o.offer_date_updated,
    o.offer_product_id,
    o.venue_id,
    o.offer_last_provider_id,
    o.booking_email,
    o.offer_is_active,
    o.offer_name,
    o.offer_description,
    o.offer_url,
    o.offer_duration_minutes,
    o.offer_is_national,
    o.offer_extra_data,
    o.offer_is_duo,
    o.offer_fields_updated,
    o.offer_withdrawal_details,
    o.offer_audio_disability_compliant,
    o.offer_mental_disability_compliant,
    o.offer_motor_disability_compliant,
    o.offer_visual_disability_compliant,
    o.offer_external_ticket_office_url,
    o.offer_validation,
    o.offer_last_validation_type,
    o.offer_subcategoryId,
    o.offer_withdrawal_delay,
    o.booking_contact,
    CASE WHEN (so.is_bookable = 1
        AND o.offer_is_active
        AND o.offer_validation = 'APPROVED') THEN 1 ELSE 0 END AS is_bookable,
    so.available_stock,
    so.total_bookings,
    so.total_individual_bookings,
    so.total_non_cancelled_individual_bookings,
    so.total_used_individual_bookings,
    so.total_individual_theoretic_revenue,
    so.total_individual_real_revenue,
    so.first_individual_booking_date,
    so.last_individual_booking_date,
    subcategories.is_physical_deposit as physical_goods,
    subcategories.is_digital_deposit digital_goods,
    subcategories.is_event as event,
    subcategories.category_id AS offer_category_id,
    CASE WHEN lo.item_linked_id is not null THEN REGEXP_REPLACE(
            lo.item_linked_id,
            r'[^a-zA-Z0-9\-\_]',
            '')
    ELSE REGEXP_REPLACE(ii.item_id, r'[^a-zA-Z0-9\-\_]', '') END as item_id
FROM {{ ref('int_applicative__extract_offer') }} AS o
LEFT JOIN item_ids AS ii ON ii.offer_id = o.offer_id
LEFT JOIN linked_offers_deduped AS lo ON lo.offer_id = o.offer_id
LEFT JOIN stocks_grouped_by_offers AS so ON so.offer_id = o.offer_id
LEFT JOIN {{ source('clean','subcategories') }} AS subcategories ON o.offer_subcategoryId = subcategories.id
WHERE o.offer_subcategoryid NOT IN ('ACTIVATION_THING', 'ACTIVATION_EVENT')
    AND (
        booking_email != 'jeux-concours@passculture.app'
        OR booking_email IS NULL
        )
