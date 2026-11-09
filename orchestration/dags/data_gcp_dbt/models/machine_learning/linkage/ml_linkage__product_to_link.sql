with
    offer_author as (
        select
            offer_product_id,
            author,
            offer_category_id,
            offer_subcategory_id,
            sum(coalesce(total_individual_bookings, 0)) as total_booking_count
        from {{ ref("mrt_global__offer") }}
        where offer_product_id != ""
        group by offer_product_id, author, offer_category_id, offer_subcategory_id
    ),

    titelive_role_mapping as (
        select role_id, artist_type
        from
            unnest(
                [
                    struct("1" as role_id, "scriptwriter" as artist_type),
                    struct("2" as role_id, "illustrator" as artist_type),
                    struct("3" as role_id, "colorist" as artist_type),
                    struct("4" as role_id, "illustrator" as artist_type),
                    struct("5" as role_id, "thesis_director" as artist_type),
                    struct("8" as role_id, "volume_editor" as artist_type),
                    struct("9" as role_id, "photographer" as artist_type),
                    struct("12" as role_id, "narrator" as artist_type),
                    struct("13" as role_id, "contributor" as artist_type),
                    struct("14" as role_id, "adapter" as artist_type),
                    struct("16" as role_id, "preface_author" as artist_type),
                    struct("17" as role_id, "afterword_author" as artist_type),
                    struct("18" as role_id, "foreword_author" as artist_type),
                    struct("19" as role_id, "translator" as artist_type),
                    struct("20" as role_id, "commentator" as artist_type),
                    struct("21" as role_id, "annotator" as artist_type),
                    struct("23" as role_id, "editorial_coordinator" as artist_type),
                    struct("24" as role_id, "inker" as artist_type),
                    struct("26" as role_id, "translator" as artist_type),
                    struct("28" as role_id, "prologue_author" as artist_type),
                    struct("29" as role_id, "epilogue_author" as artist_type),
                    struct("30" as role_id, "author" as artist_type),
                    struct("32" as role_id, "introduction_author" as artist_type),
                    struct("33" as role_id, "librettist" as artist_type),
                    struct("34" as role_id, "original_author" as artist_type),
                    struct("36" as role_id, "librettist" as artist_type),
                    struct("37" as role_id, "main_subject" as artist_type),
                    struct("38" as role_id, "collection_director" as artist_type),
                    struct("39" as role_id, "posthumous_coauthor" as artist_type),
                    struct("40" as role_id, "creator" as artist_type),
                    struct("41" as role_id, "paper_engineer" as artist_type)
                ]
            )
    ),

    titelive_book_contributors as (
        select distinct
            product.id as offer_product_id,
            titelive_role_mapping.artist_type,
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
        cross join
            unnest(
                coalesce(
                    json_query_array(contributor, "$.role"),
                    [json_query(contributor, "$.role")]
                )
            ) as role_item
        inner join
            titelive_role_mapping
            on json_value(role_item, "$.id") = titelive_role_mapping.role_id
        where
            product.subcategoryid = "LIVRE_PAPIER"
            and product.ean is not null
            and titelive.dbt_valid_to is null
            and titelive.json_raw is not null
    ),

    product_author as (
        select
            offer_author.offer_product_id,
            offer_author.offer_category_id,
            titelive_book_contributors.artist_type,
            titelive_book_contributors.artist_name,
            sum(offer_author.total_booking_count) as total_booking_count
        from offer_author
        inner join
            titelive_book_contributors
            on offer_author.offer_product_id
            = titelive_book_contributors.offer_product_id
        where offer_author.offer_subcategory_id = "LIVRE_PAPIER"
        group by
            offer_author.offer_product_id,
            titelive_book_contributors.artist_name,
            titelive_book_contributors.artist_type,
            offer_author.offer_category_id
        union all
        select
            offer_author.offer_product_id,
            offer_author.offer_category_id,
            "author" as artist_type,
            offer_author.author as artist_name,
            sum(offer_author.total_booking_count) as total_booking_count
        from offer_author
        where offer_author.offer_subcategory_id != "LIVRE_PAPIER"
        group by
            offer_author.offer_product_id,
            offer_author.author,
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
