with
    base_offers as (
        select offer_id, item_id
        from {{ ref("mrt_global__offer") }}
        where offer_is_bookable
    ),

    offer_quality as (
        select iq.*, bo.offer_id
        from base_offers as bo
        left join {{ ref("ml_metadata__item_quality") }} as iq using (item_id)
    )

select *
from offer_quality
