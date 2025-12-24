select * except (rnk)
from
    (
        select
            *, row_number() over (partition by siren order by update_date desc) as rnk
        from {{ source("clean", "siren_data") }}
    ) as inn
where rnk = 1
