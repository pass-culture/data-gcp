select
    d.deposit_id,
    d.deposit_amount,
    d.user_id,
    d.deposit_source,
    d.deposit_creation_date,
    d.deposit_update_date,
    d.deposit_expiration_date,
    d.deposit_type,
    d.total_recredit,
    d.total_recredit_amount
from {{ ref("int_global__deposit") }} as d
inner join {{ ref("int_global__user") }} as u on d.user_id = u.user_id
