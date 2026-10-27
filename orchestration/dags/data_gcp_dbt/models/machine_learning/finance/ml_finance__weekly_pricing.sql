with
    pricing as (
        select
            date(timestamp_trunc(valuedate, isoweek)) as pricing_week,
            sum(- amount / 100) as total_pricing
        from {{ ref("int_finance__pricing") }}
        where status like "invoiced" and bookingid is not null
        group by pricing_week
    )

select pricing.pricing_week, pricing.total_pricing, credits.total_available_credits
from pricing
left join
    {{ ref("ml_finance__weekly_available_credits") }} as credits
    on pricing.pricing_week = credits.pricing_week
order by pricing.pricing_week
