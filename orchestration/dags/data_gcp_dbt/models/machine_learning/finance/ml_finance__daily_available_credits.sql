select
    deposit_active_date as pricing_day, sum(deposit_amount) as total_available_credits
from {{ ref("int_global__daily_deposit") }}
-- partition filter required on deposit_active_date (BigQuery partition elimination)
where deposit_active_date > date("2021-01-01")
group by pricing_day
order by pricing_day
