select
    id,
    amount,
    userid,
    source,
    datecreated,
    expirationdate,
    type,
    cast(dateupdated as datetime) as dateupdated
from {{ ref("snapshot_raw__deposit") }}
where {{ var("snapshot_filter") }}
