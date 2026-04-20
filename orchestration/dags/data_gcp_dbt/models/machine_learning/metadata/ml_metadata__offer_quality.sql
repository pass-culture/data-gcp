{{
    config(
        cluster_by="offer_id",
        tags="weekly",
        labels={"schedule": "weekly"},
    )
}}

-- Factorized variable
{% set required_creator_metadata = [
    "'LIVRE_PAPIER'",
    "'SUPPORT_PHYSIQUE_MUSIQUE_CD'",
    "'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE'",
] %}

with
    offers_with_metadata as (
        select
            offer.offer_id,
            offer.offer_product_id,
            offer.offer_subcategory_id,
            trim(cast(offer.offer_name as string)) as offer_name,
            trim(cast(offer.offer_description as string)) as offer_description,
            trim(cast(offer.author as string)) as author,
            trim(cast(offer.performer as string)) as performer,
            trim(cast(offer.titelive_gtl_id as string)) as titelive_gtl_id,
            trim(cast(offer_metadata.image_url as string)) as image_url,
            trim(cast(offer_metadata.offer_video_url as string)) as offer_video_url
        from {{ ref("mrt_global__offer") }} as offer
        left join
            {{ ref("mrt_global__offer_metadata") }} as offer_metadata using (offer_id)
        where offer.offer_is_bookable
    ),

    artists_per_product as (
        select offer_product_id, string_agg(artist_id, ',') as artist_id
        from {{ ref("int_applicative__product_artist_link") }}
        group by offer_product_id
    ),

    offers_with_artists as (
        select offers_with_metadata.*, artists.artist_id
        from offers_with_metadata
        left join
            artists_per_product as artists
            on offers_with_metadata.offer_product_id = artists.offer_product_id
    ),

    offers_score as (
        select
            *,
            -- FILM: Title does NOT contain support format
            coalesce(
                offer_subcategory_id = 'SUPPORT_PHYSIQUE_FILM'
                and not regexp_contains(
                    offer_name, r'(?i)\b(DVD|Blu-ray|VHS|4K|Digital)\b'
                ),
                true
            ) as film_titre_format_support,
            -- Absence of image
            coalesce((image_url is null or image_url = ''), true) as absence_image,
            -- Absence of video
            coalesce(
                (offer_video_url is null or offer_video_url = ''), true
            ) as absence_video,
            -- Absence of description (<= 30 chars)
            coalesce((length(offer_description) <= 30), true) as absence_description,
            -- LIVRE, CD, VINYLE: missing artist
            coalesce(
                (
                    offer_subcategory_id
                    in ({{ required_creator_metadata | map("string") | join(", ") }})
                    and (artist_id is null or artist_id = '')
                ),
                true
            ) as livre_cd_vinyle_artiste_manquant,
            -- LIVRE, CD, VINYLE: missing GTL
            coalesce(
                (
                    offer_subcategory_id
                    in ({{ required_creator_metadata | map("string") | join(", ") }})
                    and (titelive_gtl_id is null or titelive_gtl_id = '')
                ),
                true
            ) as livre_cd_vinyle_gtl_manquant,
            -- LIVRE, CD, VINYLE: all creator fields missing
            coalesce(
                (
                    offer_subcategory_id
                    in ({{ required_creator_metadata | map("string") | join(", ") }})
                    and (author is null or author = '')
                    and (performer is null or performer = '')
                ),
                true
            ) as livre_cd_vinyle_createur_manquant
        from offers_with_artists
    ),

    offers_with_completion_score as (
        select
            -- Metadata Columns
            offer_id,
            offer_product_id,
            offer_subcategory_id,
            offer_name,
            offer_description,
            author,
            performer,
            titelive_gtl_id,
            image_url,
            offer_video_url,
            artist_id,
            -- Metadata Scores
            absence_image,
            absence_video,
            absence_description,
            livre_cd_vinyle_artiste_manquant,
            livre_cd_vinyle_gtl_manquant,
            livre_cd_vinyle_createur_manquant,
            -- Weighted completion score using macro
            {{
                completion_score(
                    "film_titre_format_support",
                    "absence_image",
                    "absence_video",
                    "absence_description",
                    "livre_cd_vinyle_artiste_manquant",
                    "livre_cd_vinyle_gtl_manquant",
                    "livre_cd_vinyle_createur_manquant",
                )
            }} as completion_score
        from offers_score
    )

select *
from offers_with_completion_score
