with
    product_author as (
        select
            offer_product_id,
            author as artist_name,
            offer_category_id,
            "author" as artist_type,
            sum(coalesce(total_individual_bookings, 0)) as total_booking_count
        from {{ ref("mrt_global__offer") }}
        where offer_product_id != ""
        group by offer_product_id, author, offer_category_id
    ),

    product_performer as (
        select
            offer_product_id,
            performer as artist_name,
            offer_category_id,
            "performer" as artist_type,
            sum(coalesce(total_individual_bookings, 0)) as total_booking_count
        from {{ ref("mrt_global__offer") }}
        where offer_product_id != ""
        group by offer_product_id, performer, offer_category_id
    ),

    product_artist as (
        select
            offer_product_id,
            artist_name,
            offer_category_id,
            artist_type,
            total_booking_count
        from product_author
        union all
        select
            offer_product_id,
            artist_name,
            offer_category_id,
            artist_type,
            total_booking_count
        from product_performer
        where artist_name is not null and artist_name != ""
    )

select distinct
    product_artist.offer_product_id,
    product_artist.artist_name,
    product_artist.offer_category_id,
    product_artist.artist_type,
    product_artist.total_booking_count
from product_artist
