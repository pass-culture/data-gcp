with
    qpi_v4 as (
        select user_id, submitted_at, subcat.category_id, subcategories
        from {{ ref("qpi_answers_v4") }} uqpi
        join
            {{ source("raw", "subcategories") }} subcat
            on subcat.id = uqpi.subcategories
    ),

    union_all as (
        select user_id, submitted_at, category_id, subcategory_id as subcategories
        from {{ source("raw", "qpi_answers_historical") }}
        union all
        select user_id, submitted_at, category_id, subcategories
        from qpi_v4
    )

select distinct *
from union_all
qualify rank() over (partition by user_id order by submitted_at desc) = 1
