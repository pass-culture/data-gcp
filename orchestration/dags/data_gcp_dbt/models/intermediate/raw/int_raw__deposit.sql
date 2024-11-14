select * except (dateupdated), cast(dateupdated as datetime) as dateupdated
from {{ ref("snapshot_raw__deposit") }}
where {{ var("snapshot_filter") }}
