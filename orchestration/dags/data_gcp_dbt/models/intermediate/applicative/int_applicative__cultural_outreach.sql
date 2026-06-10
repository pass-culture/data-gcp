select
    cultural_outreach_id,
    offer_id,
    cultural_outreach_claimed_at,
    cultural_outreach_status,
    lower(cultural_outreach_status) = 'qualified' as offer_has_mediation
from {{ ref("raw_applicative__cultural_outreach") }}
