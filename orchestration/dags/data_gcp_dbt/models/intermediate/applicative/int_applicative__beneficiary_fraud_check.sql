select
    bfd.id,
    bfd.datecreated,
    bfd.user_id,
    bfd.type,
    bfd.reason,
    bfd.reasoncodes,
    bfd.status,
    bfd.eligibility_type,
    bfd.thirdpartyid,
    bfd.result_content,
    u.user_is_active,
    ah.action_history_reason
from {{ source("raw", "applicative_database_beneficiary_fraud_check") }} as bfd
left join {{ ref("int_applicative__user") }} as u on u.user_id = bfd.user_id
left join
    {{ ref("int_applicative__action_history") }} as ah
    on ah.user_id = u.user_id
    and action_history_rk = 1
