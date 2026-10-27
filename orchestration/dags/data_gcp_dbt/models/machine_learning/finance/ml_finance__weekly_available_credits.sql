-- total_available_credits is a stock (a balance at a point in time), not a flow
-- like total_pricing - summing it across the days of a week would inflate it
-- roughly 7x for no reason. Average the daily balance over each ISO week instead.
select
    date_trunc(pricing_day, isoweek) as pricing_week,
    avg(total_available_credits) as total_available_credits
from {{ ref("ml_finance__daily_available_credits") }}
group by pricing_week
order by pricing_week
