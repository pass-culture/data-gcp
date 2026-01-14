{{
    config(
        materialized="table",
        sql_header="""
    -- Smooth ponderation function: goes from 0, growing quickly at the beginning, then
    -- slowing down to reach a maximum of 2 when x -> +infinity
    create temp function smoothponderation(x float64)
    returns float64 as (
        if(x >= 1, (2 * sqrt(x)) / (sqrt(x) + 1), 0.0)
    );""",
    )
}}

with
    raw_artist as (
        select
            *,
            array_to_string(
                array(
                    select part
                    from
                        unnest(
                            split(
                                regexp_replace(
                                    lower(
                                        -- 1. Clean accents
                                        regexp_replace(
                                            normalize(artist_name, nfd), r'\p{M}', ''
                                        )
                                    -- 2. Replace spaces or hyphens by a unique
                                    -- separator
                                    ),
                                    r'[\s-]+',
                                    ' '
                                ),
                                ' '
                            )
                        ) as part
                    -- 3. Filter to avoid empty strings if the name starts/ends with a
                    -- hyphen
                    where part != ''
                    order by part
                ),
                '|'
            ) as normalized_artist_name
        from {{ source("raw", "applicative_database_artist") }}
    ),

    raw_product_artist_link as (
        select distinct offer_product_id, artist_id
        from {{ source("raw", "applicative_database_product_artist_link") }}
    ),

    raw_product as (
        select
            cast(offer_product_id as int64) as offer_product_id,
            logical_or(offer_is_bookable) as has_bookable_offer,
            array_agg(offer_name order by total_individual_bookings desc limit 1)[
                offset(0)
            ] as offer_name,
            sum(coalesce(total_individual_bookings, 0)) as total_booking_count
        from {{ ref("mrt_global__offer") }}
        group by offer_product_id
    ),

    product_artist_link as (
        select
            raw_product.offer_product_id,
            raw_product_artist_link.artist_id,
            raw_product.has_bookable_offer,
            raw_product.offer_name,
            raw_product.total_booking_count
        from raw_product_artist_link
        left join
            raw_product
            on raw_product_artist_link.offer_product_id = raw_product.offer_product_id
    ),

    artist_statistics as (
        select
            artist_id,
            count(distinct offer_product_id) as artist_product_count,
            sum(total_booking_count) as artist_booking_count,
            sum(cast(has_bookable_offer as int)) as artist_bookable_product_count
        from product_artist_link
        group by artist_id
    ),

    artist_with_stats as (
        select
            raw_artist.*,
            coalesce(artist_statistics.artist_product_count, 1) as artist_product_count,
            coalesce(artist_statistics.artist_booking_count, 0) as artist_booking_count,
            coalesce(
                artist_statistics.artist_bookable_product_count, 0
            ) as artist_bookable_product_count
        from raw_artist
        left join
            artist_statistics on raw_artist.artist_id = artist_statistics.artist_id
    ),

    artist_with_feature_score as (
        select
            artist_with_stats.*,
            cast(artist_with_stats.wikidata_id is not null as int64) as wikidata_score,
            cast(
                artist_with_stats.wikidata_image_file_url is not null as int64
            ) as image_score,
            cast(
                artist_with_stats.artist_bookable_product_count >= 1 as int64
            ) as bookable_score,
            smoothponderation(artist_with_stats.artist_booking_count) as booking_score,
            smoothponderation(
                artist_with_stats.artist_product_count - 1
            ) as product_score
        from artist_with_stats
    ),

    artist_with_raw_score as (
        select
            artist_with_feature_score.*,
            round(
                5 * wikidata_score
                + image_score
                + bookable_score
                + booking_score
                + product_score,
                2
            ) as artist_raw_score
        from artist_with_feature_score
    ),

    namesake_computations as (
        select
            *,
            -- We check if for a group of same normalized_artist_name, there is ANYONE
            -- with a wikidata_id
            logical_or(wikidata_id is not null) over (
                partition by normalized_artist_name
            ) as group_has_wiki,

            -- We rank artists within their group by their raw score (higher is better)
            row_number() over (
                partition by normalized_artist_name
                order by artist_raw_score desc, artist_id asc
            ) as rank_in_group
        from artist_with_raw_score
    ),

    artist_with_final_score as (
        select
            *,
            case
                -- IF the group has at least one Wiki ID
                when group_has_wiki
                then cast(wikidata_id is not null as int64)
                -- ELSE, we convert the condition (is it the first?) to 1 or 0
                else cast(rank_in_group = 1 as int64)
            end as namesake_score
        from namesake_computations
    )

select *, (artist_raw_score * namesake_score * bookable_score) as artist_score
from artist_with_final_score
order by normalized_artist_name asc, artist_score desc
