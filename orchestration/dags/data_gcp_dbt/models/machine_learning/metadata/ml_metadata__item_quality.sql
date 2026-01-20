-- Step 1: Enrich items with artist metadata
with
    base_items as (
        select item_id, offer_product_id
        from {{ ref("mrt_global__offer") }}
        group by item_id, offer_product_id
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
        left join
            {{ ref("int_applicative__product_artist_link") }} as pal
            on bi.offer_product_id = pal.offer_product_id
    ),

    final as (
        select
            iwa.*,
            -- FILM: Title does NOT contain support format
            case
                when iwa.offer_subcategory_id = 'SUPPORT_PHYSIQUE_FILM'
                then
                    not regexp_contains(
                        iwa.offer_name, r'(?i)\\b(DVD|Blu-ray|VHS|4K|Digital)\\b'
                    )
                else false
            end as film_titre_format_support,

            -- Absence of image
            (iwa.image_url is null or iwa.image_url = '') as absence_image,

            (iwa.offer_video_url is null or iwa.offer_video_url = '') as absence_video,

            -- Absence of description (<= 30 chars)
            (length(iwa.offer_description) <= 30) as absence_description,

            -- LIVRE, CD, VINYLE: missing artist
            case
                when
                    iwa.offer_subcategory_id in (
                        'LIVRE_PAPIER',
                        'SUPPORT_PHYSIQUE_MUSIQUE_CD',
                        'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE'
                    )
                then (iwa.artist_id is null or iwa.artist_id = '')
                else false
            end as livre_cd_vinyle_artiste_manquant,

            -- LIVRE, CD, VINYLE: missing GTL
            case
                when
                    iwa.offer_subcategory_id in (
                        'LIVRE_PAPIER',
                        'SUPPORT_PHYSIQUE_MUSIQUE_CD',
                        'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE'
                    )
                then (iwa.titelive_gtl_id is null or iwa.titelive_gtl_id = '')
                else false
            end as livre_cd_vinyle_gtl_manquant,

            -- LIVRE, CD, VINYLE: all creator fields missing
            case
                when
                    iwa.offer_subcategory_id in (
                        'LIVRE_PAPIER',
                        'SUPPORT_PHYSIQUE_MUSIQUE_CD',
                        'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE'
                    )
                then
                    (iwa.author is null or iwa.author = '')
                    and (iwa.performer is null or iwa.performer = '')
                else false
            end as livre_cd_vinyle_createur_manquant,

            -- Weighted completion score: 1 - weighted sum of failed checks / total
            -- weight
            round(
                1 - (
                    (
                        case
                            when iwa.offer_subcategory_id = 'SUPPORT_PHYSIQUE_FILM'
                            then
                                case
                                    when
                                        not regexp_contains(
                                            iwa.offer_name,
                                            r'(?i)\\b(DVD|Blu-ray|VHS|4K|Digital)\\b'
                                        )
                                    then 1.0
                                    else 0.0
                                end
                            else 0.0
                        end
                    )
                    * 1.0
                    + (
                        case
                            when
                                (
                                    iwa.image_url is null
                                    or trim(cast(iwa.image_url as string)) = ''
                                )
                            then 1.0
                            else 0.0
                        end
                    )
                    * 2.0
                    + (
                        case
                            when
                                (
                                    iwa.offer_video_url is null
                                    or trim(cast(iwa.offer_video_url as string)) = ''
                                )
                            then 1.0
                            else 0.0
                        end
                    )
                    * 1.0
                    + (
                        case
                            when (length(cast(iwa.offer_description as string)) <= 30)
                            then 1.0
                            else 0.0
                        end
                    )
                    * 1.5
                    + (
                        case
                            when
                                iwa.offer_subcategory_id in (
                                    'LIVRE_PAPIER',
                                    'SUPPORT_PHYSIQUE_MUSIQUE_CD',
                                    'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE'
                                )
                                and (
                                    iwa.artist_id is null
                                    or trim(cast(iwa.artist_id as string)) = ''
                                )
                            then 1.0
                            else 0.0
                        end
                    )
                    * 1.0
                    + (
                        case
                            when
                                iwa.offer_subcategory_id in (
                                    'LIVRE_PAPIER',
                                    'SUPPORT_PHYSIQUE_MUSIQUE_CD',
                                    'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE'
                                )
                                and (
                                    iwa.titelive_gtl_id is null
                                    or trim(cast(iwa.titelive_gtl_id as string)) = ''
                                )
                            then 1.0
                            else 0.0
                        end
                    )
                    * 1.0
                    + (
                        case
                            when
                                iwa.offer_subcategory_id in (
                                    'LIVRE_PAPIER',
                                    'SUPPORT_PHYSIQUE_MUSIQUE_CD',
                                    'SUPPORT_PHYSIQUE_MUSIQUE_VINYLE'
                                )
                                and (
                                    (
                                        iwa.author is null
                                        or trim(cast(iwa.author as string)) = ''
                                    )
                                    and (
                                        iwa.performer is null
                                        or trim(cast(iwa.performer as string)) = ''
                                    )
                                )
                            then 1.0
                            else 0.0
                        end
                    )
                    * 1.0
                )
                / (1.0 + 2.0 + 1.0 + 1.5 + 1.0 + 1.0 + 1.0),
                1
            ) as completion_score
        from item_metadata_enriched as iwa
    )

select *
from final
