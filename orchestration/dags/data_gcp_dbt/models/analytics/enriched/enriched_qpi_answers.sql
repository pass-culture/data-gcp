with
    qpi_v4 as (
        select uqpi.user_id, uqpi.submitted_at, subcat.category_id, uqpi.subcategories
        from {{ ref("int_applicative__qpi_answers_v4") }} as uqpi
        inner join
            {{ source("raw", "subcategories") }} as subcat
            on uqpi.subcategories = subcat.id
    ),

    union_all as (
        select
            qpi_historical.user_id,
            qpi_historical.submitted_at,
            qpi_historical.category_id,
            qpi_historical.subcategory_id as subcategories
        from {{ source("raw", "qpi_answers_historical") }} as qpi_historical
        union all
        select
            qpi_v4.user_id,
            qpi_v4.submitted_at,
            qpi_v4.category_id,
            qpi_v4.subcategories
        from qpi_v4
    )

select distinct *
from union_all
qualify rank() over (partition by user_id order by submitted_at desc) = 1
