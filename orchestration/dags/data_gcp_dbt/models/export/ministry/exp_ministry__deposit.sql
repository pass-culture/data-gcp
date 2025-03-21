{{ config(tags="monthly", labels={"schedule": "monthly"}) }}

select
    deposit_id,
    deposit_amount,
    user_id,
    deposit_source,
    deposit_creation_date,
    deposit_update_date,
    deposit_expiration_date,
    deposit_type,
    deposit_rank_asc,
    deposit_rank_desc,
    deposit_seniority,
    days_between_user_creation_and_deposit_creation
from {{ ref("mrt_global__deposit") }}
