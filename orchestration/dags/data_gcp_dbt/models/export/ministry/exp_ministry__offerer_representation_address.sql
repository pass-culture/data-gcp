select offerer_address_id, offerer_address_label, address_id, offerer_id,
from {{ ref("mrt_global__offerer_address") }}
