select
    id,
    amount,
    userid,
    source,
    datecreated,
    expirationDate,
    type,
    cast(dateupdated as datetime) as dateupdated
from {{ ref("snapshot_raw__deposit") }}
where {{ var("snapshot_filter") }}
