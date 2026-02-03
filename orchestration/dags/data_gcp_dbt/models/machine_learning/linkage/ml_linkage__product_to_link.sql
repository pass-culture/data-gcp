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

    product_actor as (
        select
            gof.offer_product_id,
            gof.offer_category_id,
            "actor" as artist_type,
            trim(casting_names, '""') as artist_name,
            sum(coalesce(gof.total_individual_bookings, 0)) as total_booking_count
        from
            {{ ref("mrt_global__offer") }} as gof,
            unnest(split(trim(gof.casting, "[]"), ",")) as casting_names
        where
            gof.offer_product_id != ""
            and gof.casting is not null
            and gof.casting != "[]"
        group by gof.offer_product_id, casting_names, gof.offer_category_id
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
        union all
        select
            offer_product_id,
            artist_name,
            offer_category_id,
            artist_type,
            total_booking_count
        from product_actor
        where artist_name is not null and artist_name != ""
    )

select distinct
    product_artist.offer_product_id,
    product_artist.artist_name,
    product_artist.offer_category_id,
    product_artist.artist_type,
    product_artist.total_booking_count
from product_artist
