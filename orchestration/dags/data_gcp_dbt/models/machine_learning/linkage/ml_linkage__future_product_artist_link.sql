-- depends_on: {{ ref('ml_linkage__future_artist') }}
with
    product_artist_link_delta as (
        select offer_product_id, artist_id, artist_type, action
        from {{ ref("ml_linkage__delta_product_artist_link") }}
    ),

    future_product_artist_link as (
        select base.offer_product_id, base.artist_id, base.artist_type
        from {{ ref("int_applicative__product_artist_link") }} as base
        where
            not exists (
                select 1 as found
                from product_artist_link_delta as delta
                where
                    delta.action in ("remove", "update")
                    and delta.offer_product_id = base.offer_product_id
                    and delta.artist_id = base.artist_id
                    and delta.artist_type = base.artist_type
            )
        union all
        select offer_product_id, artist_id, artist_type
        from product_artist_link_delta
        where action in ("add", "update")
    )

-- distinct: a deduplication `add` can target a link that already exists in the
-- applicative table, which would otherwise appear twice
select distinct offer_product_id, artist_id, artist_type
from future_product_artist_link
