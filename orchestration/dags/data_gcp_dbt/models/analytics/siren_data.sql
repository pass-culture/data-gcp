select * except (rnk)
from (
    select
        *,
        ROW_NUMBER() over (partition by siren order by update_date desc) as rnk
    from {{ source('clean','siren_data') }}
) inn
where rnk = 1
