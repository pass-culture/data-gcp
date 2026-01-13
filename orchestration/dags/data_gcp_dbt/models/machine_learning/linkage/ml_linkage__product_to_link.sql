with
    product_metadatas as (
        select
            offer_product_id,
            logical_or(offer_is_bookable) as has_bookable_offer,
            array_agg(offer_name order by total_individual_bookings desc limit 1)[
                offset(0)
            ] as offer_name,
            sum(coalesce(total_individual_bookings, 0)) as total_booking_count
        from {{ ref("mrt_global__offer") }}
        group by offer_product_id
    ),

    product_author as (
        select
            offer_product_id,
            author as artist_name,
            offer_category_id,
            "author" as artist_type
        from {{ ref("mrt_global__offer") }}
        where offer_product_id != ""
        group by offer_product_id, author, offer_category_id
    ),

    product_performer as (
        select
            offer_product_id,
            performer as artist_name,
            offer_category_id,
            "performer" as artist_type
        from {{ ref("mrt_global__offer") }}
        where offer_product_id != ""
        group by offer_product_id, performer, offer_category_id
    ),

    product_artist as (
        select offer_product_id, artist_name, offer_category_id, artist_type
        from product_author
        union all
        select offer_product_id, artist_name, offer_category_id, artist_type
        from product_performer
        where artist_name is not null and artist_name != ""
    )

select distinct
    product_artist.offer_product_id,
    product_artist.artist_name,
    product_artist.offer_category_id,
    product_artist.artist_type,
    product_metadatas.has_bookable_offer,
    product_metadatas.offer_name,
    product_metadatas.total_booking_count
from product_artist
left join
    product_metadatas
    on product_artist.offer_product_id = product_metadatas.offer_product_id
