select *
from {{ ref('snapshot_source__offerer') }}
where {{ var('snapshot_filter') }}
