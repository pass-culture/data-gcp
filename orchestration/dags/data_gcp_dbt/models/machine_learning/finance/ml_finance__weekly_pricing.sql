select
    DATE(TIMESTAMP_TRUNC(valuedate, isoweek)) as pricing_week,
    SUM(- amount / 100) as total_pricing
from {{ ref("int_finance__pricing") }}
where status like "invoiced" and bookingid is not null
group by pricing_week
order by pricing_week
