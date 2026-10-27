with
    pricing as (
        select date(valuedate) as pricing_day, sum(- amount / 100) as total_pricing
        from {{ ref("int_finance__pricing") }}
        where status like "invoiced" and bookingid is not null
        group by pricing_day
    )

select pricing.pricing_day, pricing.total_pricing, credits.total_available_credits
from pricing
left join
    {{ ref("ml_finance__daily_available_credits") }} as credits
    on pricing.pricing_day = credits.pricing_day
order by pricing.pricing_day
