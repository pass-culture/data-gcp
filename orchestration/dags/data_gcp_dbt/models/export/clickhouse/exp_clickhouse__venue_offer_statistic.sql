WITH collective_offer_status AS (
    SELECT
        venue_id,
        COUNT(DISTINCT CASE WHEN collective_offer_is_active = true AND collective_offer_validation = 'APPROVED' THEN collective_offer_id END) AS total_active_collective_offers,
        COUNT(DISTINCT CASE WHEN collective_offer_is_active = true AND collective_offer_validation = 'PENDING' THEN collective_offer_id END) AS total_pending_collective_offers,
        COUNT(DISTINCT CASE WHEN collective_offer_is_active = false AND collective_offer_validation != 'REJECTED' THEN collective_offer_id END) AS total_inactive_non_rejected_collective_offers
    FROM {{ ref('int_applicative__collective_offer') }}
    GROUP BY venue_id
),

individual_offer_status AS (
    SELECT
        o.venue_id,
        COUNT(DISTINCT CASE WHEN o.is_active = true AND o.offer_validation = 'APPROVED' AND (s.booking_limit_datetime IS null OR s.booking_limit_datetime > CURRENT_TIMESTAMP()) THEN o.offer_id END) AS total_active_offers,
        COUNT(DISTINCT CASE WHEN o.is_active = true AND o.offer_validation = 'PENDING' THEN o.offer_id END) AS total_pending_offers,
        COUNT(DISTINCT CASE WHEN o.is_active = false AND o.offer_validation != 'REJECTED' THEN o.offer_id END) AS total_inactive_non_rejected_offers
    FROM {{ ref('int_applicative__offer') }} AS o
    LEFT JOIN {{ ref('int_applicative__stock') }} AS s
        ON o.offer_id = s.offer_id
        AND s.is_soft_deleted = false
    GROUP BY o.venue_id
)

SELECT
    v.venue_id,
    COALESCE(ios.total_active_offers, 0) AS total_active_offers,
    COALESCE(ios.total_pending_offers, 0) AS total_pending_offers,
    COALESCE(ios.total_inactive_non_rejected_offers, 0) AS total_inactive_non_rejected_offers,
    COALESCE(cos.total_active_collective_offers, 0) AS total_active_collective_offers,
    COALESCE(cos.total_pending_collective_offers, 0) AS total_pending_collective_offers,
    COALESCE(cos.total_inactive_non_rejected_collective_offers, 0) AS total_inactive_non_rejected_collective_offers
FROM {{ ref('int_applicative__venue') }} AS v
LEFT JOIN individual_offer_status AS ios ON v.venue_id = ios.venue_id
LEFT JOIN collective_offer_status AS cos ON v.venue_id = cos.venue_id
