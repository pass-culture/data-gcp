{{ config(cluster_by="product_subcategory_id") }}

with
    product as (
        select
            id as product_id,
            ean as product_ean,
            name as product_name,
            description as product_description,
            subcategoryid as product_subcategory_id,
            cast(lastproviderid as string) as product_last_provider_id,
            datemodifiedatlastprovider as product_modified_at_last_provider_date,
            is_mediation = 1 as product_has_image,
            safe.parse_json(product_extra_data) as extra_data
        from {{ ref("int_applicative__product") }}
    ),

    product_offers as (
        select
            offer_product_id as product_id,
            count(*) as product_total_offers,
            countif(offer_is_bookable) as product_total_bookable_offers,
            count(distinct venue_id) as product_total_venues,
            min(offer_creation_date) as product_first_offer_creation_date,
            sum(coalesce(total_individual_bookings, 0)) as total_individual_bookings,
            sum(
                coalesce(total_cancelled_individual_bookings, 0)
            ) as total_cancelled_individual_bookings
        from {{ ref("int_global__offer") }}
        where offer_product_id is not null
        group by offer_product_id
    ),

    product_metadata as (
        select
            product.* except (extra_data),
            json_value(extra_data.author) as product_author,
            json_value(extra_data.performer) as product_performer,
            json_value(extra_data.editeur) as product_publisher,
            json_value(extra_data.music_label) as product_music_label,
            json_value(extra_data.distributeur) as product_distributor,
            json_value(extra_data.collection) as product_collection,
            json_value(extra_data.bookformat) as product_book_format,
            json_value(extra_data.langueiso) as product_language_iso,
            json_value(extra_data.code_clil) as product_clil_code,
            json_value(extra_data.rayon) as product_rayon,
            -- Titelive sends the release date in two formats.
            coalesce(
                safe.parse_date("%Y-%m-%d", json_value(extra_data.date_parution)),
                safe.parse_date("%d/%m/%Y", json_value(extra_data.date_parution)),
                safe.parse_date(
                    "%Y-%m-%d", left(json_value(extra_data.date_parution), 10)
                ),
                safe.parse_date("%Y-%m-%d", json_value(extra_data.releasedate))
            ) as product_release_date,
            safe_cast(
                coalesce(
                    json_value(extra_data.prix_livre),
                    json_value(extra_data.prix_musique)
                ) as numeric
            ) as product_price,
            json_value(extra_data.gtl_id) as product_gtl_id,
            case
                when product_subcategory_id = "LIVRE_PAPIER"
                then "BOOK"
                when
                    product_subcategory_id in (
                        "SUPPORT_PHYSIQUE_MUSIQUE_CD", "SUPPORT_PHYSIQUE_MUSIQUE_VINYLE"
                    )
                then "MUSIC"
            end as product_gtl_type,
            json_value(extra_data.allocineid) as product_allocine_id,
            json_value(extra_data.visa) as product_movie_visa,
            safe_cast(json_value(extra_data.runtime) as int64) as product_movie_runtime,
            safe_cast(
                json_value(extra_data.productionyear) as int64
            ) as product_movie_production_year
        from product
    )

select
    pm.product_id,
    pm.product_ean,
    pm.product_name,
    pm.product_description,
    pm.product_subcategory_id,
    subcategories.category_id as product_category_id,
    pm.product_last_provider_id,
    provider.provider_name as product_last_provider_name,
    pm.product_modified_at_last_provider_date,
    pm.product_has_image,
    pm.product_author,
    pm.product_performer,
    pm.product_publisher,
    pm.product_music_label,
    pm.product_distributor,
    pm.product_collection,
    pm.product_book_format,
    pm.product_language_iso,
    pm.product_clil_code,
    pm.product_rayon,
    pm.product_release_date,
    pm.product_price,
    pm.product_gtl_id,
    gtl.gtl_type,
    gtl.gtl_label_level_1,
    gtl.gtl_label_level_2,
    gtl.gtl_label_level_3,
    gtl.gtl_label_level_4,
    pm.product_allocine_id,
    pm.product_movie_visa,
    pm.product_movie_runtime,
    pm.product_movie_production_year,
    po.product_first_offer_creation_date,
    coalesce(po.product_total_offers, 0) as product_total_offers,
    coalesce(po.product_total_bookable_offers, 0) as product_total_bookable_offers,
    coalesce(po.product_total_venues, 0) as product_total_venues,
    coalesce(po.total_individual_bookings, 0) as product_total_individual_bookings,
    coalesce(po.total_individual_bookings, 0) - coalesce(
        po.total_cancelled_individual_bookings, 0
    ) as product_total_non_cancelled_individual_bookings
from product_metadata as pm
left join product_offers as po on pm.product_id = po.product_id
left join
    {{ source("raw", "subcategories") }} as subcategories
    on pm.product_subcategory_id = subcategories.id
left join
    {{ source("raw", "applicative_database_provider") }} as provider
    on pm.product_last_provider_id = cast(provider.provider_id as string)
left join
    {{ ref("int_applicative__titelive_gtl") }} as gtl
    on pm.product_gtl_id = gtl.gtl_id
    and pm.product_gtl_type = gtl.gtl_type
