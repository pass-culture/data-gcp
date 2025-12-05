select
    TIMESTAMP_TRUNC(valuedate, week) as semaine,
    SUM(-amount / 100) as total_pricing
from `passculture-data-prod.raw_prod.applicative_database_pricing`
where status like "invoiced" and bookingid is not null
group by
    semaine
order by
    semaine
;
