select
    date_trunc(deposit_active_date, isoweek) as pricing_week,
    sum(deposit_amount) as total_available_credits
from {{ ref("int_global__daily_deposit") }}
-- partition filter required on deposit_active_date (BigQuery partition elimination)
where deposit_active_date > date("2021-01-01")
group by pricing_week
order by pricing_week
