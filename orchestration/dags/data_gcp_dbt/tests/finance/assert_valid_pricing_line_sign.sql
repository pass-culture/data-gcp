-- Singular test: validates pricing line sign conventions
-- - "offerer revenue" should have amount <= 0 (Pass Culture pays offerer)
-- - "offerer contribution" should have amount >= 0 (offerer contributes)
-- Returns anomalous rows (test passes if 0 rows returned)

select
    id,
    pricingid,
    amount,
    category
from {{ ref("int_finance__pricing_line") }}
where
    (category = 'offerer revenue' and amount > 0)
    or (category = 'offerer contribution' and amount < 0)
