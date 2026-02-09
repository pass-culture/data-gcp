{% set required_creator_metadata = [
    "'LIVRE_PAPIER'",
    "'SUPPORT_PHYSIQUE_MUSIQUE_CD'",
    "'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE'",
] %}
with
    base_items as (
        select item_id, offer_product_id
        from {{ ref("mrt_global__offer") }}
        group by item_id, offer_product_id
    ),

    unique_artists as (
        select offer_product_id, string_agg(artist_id, ',') as artist_id
        from {{ ref("int_applicative__product_artist_link") }}
        group by offer_product_id
    ),

    item_metadata_enriched as (
        select
            bi.offer_product_id,
            im.item_id,
            im.offer_subcategory_id,
            im.offer_category_id,
            im.offer_type_domain,
            im.gtl_label_level_1,
            im.gtl_label_level_2,
            im.gtl_label_level_3,
            im.gtl_label_level_4,
            im.gtl_type,
            pal.artist_id,
            trim(cast(im.offer_name as string)) as offer_name,
            trim(cast(im.offer_description as string)) as offer_description,
            trim(cast(im.author as string)) as author,
            trim(cast(im.performer as string)) as performer,
            trim(cast(im.titelive_gtl_id as string)) as titelive_gtl_id,
            trim(cast(im.image_url as string)) as image_url,
            trim(cast(im.offer_video_url as string)) as offer_video_url
        from base_items as bi
        inner join {{ ref("ml_input__item_metadata") }} as im using (item_id)
        left join unique_artists as pal on bi.offer_product_id = pal.offer_product_id
    ),

    item_metadata_score as (
        select
            iwa.*,
            -- FILM: Title does NOT contain support format
            coalesce(
                iwa.offer_subcategory_id = 'SUPPORT_PHYSIQUE_FILM'
                and not regexp_contains(
                    iwa.offer_name, r'(?i)\b(DVD|Blu-ray|VHS|4K|Digital)\b'
                ),
                true
            ) as film_titre_format_support,
            -- Absence of image
            coalesce(
                (iwa.image_url is null or iwa.image_url = ''), true
            ) as absence_image,
            -- Absence of video
            coalesce(
                (iwa.offer_video_url is null or iwa.offer_video_url = ''), true
            ) as absence_video,
            -- Absence of description (<= 30 chars)
            coalesce(
                (length(iwa.offer_description) <= 30), true
            ) as absence_description,
            -- LIVRE, CD, VINYLE: missing artist
            coalesce(
                (
                    iwa.offer_subcategory_id
                    in ({{ required_creator_metadata | map("string") | join(", ") }})
                    and (iwa.artist_id is null or iwa.artist_id = '')
                ),
                true
            ) as livre_cd_vinyle_artiste_manquant,
            -- LIVRE, CD, VINYLE: missing GTL
            coalesce(
                (
                    iwa.offer_subcategory_id
                    in ({{ required_creator_metadata | map("string") | join(", ") }})
                    and (iwa.titelive_gtl_id is null or iwa.titelive_gtl_id = '')
                ),
                true
            ) as livre_cd_vinyle_gtl_manquant,
            -- LIVRE, CD, VINYLE: all creator fields missing
            coalesce(
                (
                    iwa.offer_subcategory_id
                    in ({{ required_creator_metadata | map("string") | join(", ") }})
                    and (iwa.author is null or iwa.author = '')
                    and (iwa.performer is null or iwa.performer = '')
                ),
                true
            ) as livre_cd_vinyle_createur_manquant
        from item_metadata_enriched as iwa
    ),

    final as (
        select
            ims.*,
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
        from item_metadata_score as ims
    )

select *
from final
