select *
from {{ ref("snapshot_raw__collective_booking") }}
where {{ var("snapshot_filter") }}
