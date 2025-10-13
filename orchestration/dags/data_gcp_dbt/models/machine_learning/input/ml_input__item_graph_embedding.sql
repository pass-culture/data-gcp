with
    offers as (
        select offer_id, offer_product_id, item_id, offer_name, offer_subcategory_id
        from {{ ref("mrt_global__offer") }}
        where offer_category_id = "LIVRE"
    ),

    offers_with_best_metadata as (
        select
            offers.offer_id,
            offers.offer_product_id,
            offers.item_id,
            offers.offer_name,
            offers.offer_subcategory_id,
            offer_metadata.gtl_type,
            offer_metadata.gtl_label_level_1,
            offer_metadata.gtl_label_level_2,
            offer_metadata.gtl_label_level_3,
            offer_metadata.gtl_label_level_4,
            offer_metadata.author
        from offers
        left join
            {{ ref("mrt_global__offer_metadata") }} as offer_metadata using (offer_id)
        qualify
            row_number() over (
                partition by offers.item_id
                order by
                    (offer_metadata.gtl_label_level_1 is not null) desc, offers.offer_id  -- deterministic tie breaker
            )
            = 1
    ),

    artist_link_prepared as (
        select
            artist_id, artist_type, cast(offer_product_id as string) as offer_product_id
        from {{ source("raw", "applicative_database_product_artist_link") }}
    )

select
    omd.offer_id as example_offer_id,
    omd.offer_product_id,
    omd.item_id,
    omd.offer_name,
    omd.offer_subcategory_id,
    omd.gtl_type,
    omd.gtl_label_level_1,
    omd.gtl_label_level_2,
    omd.gtl_label_level_3,
    omd.gtl_label_level_4,
    omd.author,
    al.artist_id,
    a.artist_name,
    al.artist_type
from offers_with_best_metadata as omd
left join artist_link_prepared as al on omd.offer_product_id = al.offer_product_id
left join {{ source("ml_preproc", "artist") }} as a using (artist_id)
