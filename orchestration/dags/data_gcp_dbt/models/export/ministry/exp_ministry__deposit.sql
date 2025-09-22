select
    deposit_id,
    deposit_amount,
    user_id,
    deposit_source,
    deposit_creation_date,
    deposit_update_date,
    deposit_expiration_date,
    deposit_type,
    total_recredit,
    total_recredit_amount
from {{ ref("int_global__deposit") }}
