select timestamp_trunc(valuedate, week) as semaine, sum(- amount / 100) as total_pricing
from {{ ref("int_finance__pricing") }}
where status like "invoiced" and bookingid is not null
group by semaine
order by semaine
