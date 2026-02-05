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
            offer.offer_product_id,
            offer.offer_category_id,
            "film_actor" as artist_type,
            trim(casting_names, '""') as artist_name,
            sum(coalesce(offer.total_individual_bookings, 0)) as total_booking_count
        from
            {{ ref("mrt_global__offer") }} as offer,
            unnest(split(trim(offer.casting, "[]"), ",")) as casting_names
        where
            offer.offer_product_id != ""
            and offer.casting is not null
            and offer.casting != "[]"
        group by offer.offer_product_id, casting_names, offer.offer_category_id
    ),

    product_director as (
        select
            offer_product_id,
            offer_category_id,
            stage_director as artist_name,
            "film_director" as artist_type,
            sum(coalesce(total_individual_bookings, 0)) as total_booking_count
        from {{ ref("mrt_global__offer") }}
        where offer_product_id != "" and stage_director is not null
        group by offer_product_id, stage_director, offer_category_id
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
        union all
        select
            offer_product_id,
            artist_name,
            offer_category_id,
            artist_type,
            total_booking_count
        from product_director
        where artist_name is not null and artist_name != ""
    )

select distinct
    product_artist.offer_product_id,
    product_artist.artist_name,
    product_artist.offer_category_id,
    product_artist.artist_type,
    product_artist.total_booking_count
from product_artist
