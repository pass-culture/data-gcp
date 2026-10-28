with
    offer_author as (
        select
            offer_product_id,
            author,
            offer_category_id,
            sum(coalesce(total_individual_bookings, 0)) as total_booking_count
        from {{ ref("mrt_global__offer") }}
        where offer_product_id != ""
        group by offer_product_id, author, offer_category_id
    ),

    titelive_book_authors as (
        select distinct
            product.id as offer_product_id,
            lower(trim(json_value(contributor, "$.name"))) as artist_name
        from {{ ref("int_applicative__product") }} as product
        inner join
            {{ ref("snapshot_raw__titelive_products") }} as titelive
            on product.ean = titelive.ean
        cross join
            unnest(
                coalesce(
                    json_query_array(titelive.json_raw, "$.contributor"),
                    json_query_array(titelive.json_raw, "$.article[0].contributor")
                )
            ) as contributor
        where
            product.subcategoryid = "LIVRE_PAPIER"
            and product.ean is not null
            and titelive.dbt_valid_to is null
            and titelive.json_raw is not null
            and (
                json_value(contributor, "$.role.id") = "30"
                or exists (
                    select 1 as found
                    from unnest(json_query_array(contributor, "$.role")) as role_item
                    where json_value(role_item, "$.id") = "30"
                )
            )
    ),

    product_author as (
        select
            offer_author.offer_product_id,
            offer_author.offer_category_id,
            "author" as artist_type,
            coalesce(
                titelive_book_authors.artist_name, offer_author.author
            ) as artist_name,
            sum(offer_author.total_booking_count) as total_booking_count
        from offer_author
        left join
            titelive_book_authors
            on offer_author.offer_product_id = titelive_book_authors.offer_product_id
        group by
            offer_author.offer_product_id,
            coalesce(titelive_book_authors.artist_name, offer_author.author),
            offer_author.offer_category_id
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
        where artist_name is not null and artist_name != ""
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
