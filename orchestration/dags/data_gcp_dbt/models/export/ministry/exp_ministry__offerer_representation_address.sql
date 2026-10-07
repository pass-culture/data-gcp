select oa.offerer_address_id, oa.offerer_address_label, oa.address_id, v.offerer_id,
from {{ ref("mrt_global__offerer_address") }} as oa
left join {{ ref("mrt_global__venue") }} as v on oa.venue_id = v.venue_id
