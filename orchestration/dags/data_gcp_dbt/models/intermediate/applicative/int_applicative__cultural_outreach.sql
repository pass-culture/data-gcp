SELECT
    cultural_outreach_id,
    offer_id,
    cultural_outreach_claimed_at,
    cultural_outreach_status,
    lower(cultural_outreach_status) = 'qualified' AS offer_has_mediation
FROM {{ ref("raw_applicative__cultural_outreach") }}
