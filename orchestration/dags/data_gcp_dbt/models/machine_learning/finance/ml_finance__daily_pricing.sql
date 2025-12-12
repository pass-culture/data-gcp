select date(valuedate) as pricing_day, sum(- amount / 100) as total_pricing
from {{ ref("int_finance__pricing") }}
where status like "invoiced" and bookingid is not null
group by pricing_day
order by pricing_day
