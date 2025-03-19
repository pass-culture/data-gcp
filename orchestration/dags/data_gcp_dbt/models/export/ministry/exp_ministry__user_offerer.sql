
select offerer_id, user_id, user_role, user_address, user_city
from {{ ref("mrt_global__user_offerer") }}
