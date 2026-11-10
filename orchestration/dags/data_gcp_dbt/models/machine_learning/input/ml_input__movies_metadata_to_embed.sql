{{ config(materialized="view") }}

-- Movies subset of the semantic base. Genres come from the AlloCiné-sourced
-- product_extra_data on the catalogue product, matched on the shared
-- product-{id} item_id (theater_movie_id lives only on screening offers, not
-- on these product items, so it can't be used here).
with
    product_metadata as (
        -- All AlloCiné-sourced fields unpacked from the product_extra_data JSON
        -- (keyed on the shared product-{id} item_id). Array fields are flattened
        -- to comma-separated strings; null elements are filtered so the array
        -- constructors never build a null-containing array.
        select
            -- cast to string so it renders as "2024", not "2024.0", in the prompt.
            cast(
                safe_cast(
                    json_value(product_extra_data, '$.productionYear') as int64
                ) as string
            ) as production_year,
            concat('product-', id) as item_id,
            json_value(product_extra_data, '$.title') as product_title,
            json_value(product_extra_data, '$.originalTitle') as original_title,
            json_value(product_extra_data, '$.type') as product_type,
            json_value(product_extra_data, '$.visa') as visa,
            json_value(product_extra_data, '$.synopsis') as synopsis,
            json_value(product_extra_data, '$.backlink') as backlink_url,
            json_value(product_extra_data, '$.posterUrl') as poster_url,
            json_value(product_extra_data, '$.allocineId') as allocine_id,
            json_value(product_extra_data, '$.releaseDate') as release_date,
            safe_cast(json_value(product_extra_data, '$.runtime') as int64) as runtime,
            nullif(
                array_to_string(
                    json_extract_string_array(product_extra_data, '$.genres'), ', '
                ),
                ''
            ) as allocine_genres_concat,
            nullif(
                array_to_string(
                    json_extract_string_array(product_extra_data, '$.cast'), ', '
                ),
                ''
            ) as cast_concat,
            nullif(
                array_to_string(
                    json_extract_string_array(product_extra_data, '$.countries'), ', '
                ),
                ''
            ) as countries_concat,
            nullif(
                array_to_string(
                    array(
                        select json_value(company, '$.name') as company_name
                        from
                            unnest(
                                json_query_array(product_extra_data, '$.companies')
                            ) as company
                        where json_value(company, '$.name') is not null
                    ),
                    ', '
                ),
                ''
            ) as companies_concat,
            nullif(
                array_to_string(
                    array(
                        select
                            trim(
                                concat(
                                    coalesce(
                                        json_value(credit, '$.person.firstName'), ''
                                    ),
                                    ' ',
                                    coalesce(
                                        json_value(credit, '$.person.lastName'), ''
                                    )
                                )
                            ) as director_name
                        from
                            unnest(
                                json_query_array(product_extra_data, '$.credits')
                            ) as credit
                        where
                            json_value(credit, '$.position.name') = 'DIRECTOR'
                            and trim(
                                concat(
                                    coalesce(
                                        json_value(credit, '$.person.firstName'), ''
                                    ),
                                    ' ',
                                    coalesce(
                                        json_value(credit, '$.person.lastName'), ''
                                    )
                                )
                            )
                            != ''
                    ),
                    ', '
                ),
                ''
            ) as directors_concat
        from {{ ref("int_applicative__product") }}
    )

select
    prod.production_year,
    prod.product_title,
    prod.original_title,
    prod.product_type,
    prod.visa,
    prod.synopsis,
    prod.backlink_url,
    prod.poster_url,
    prod.allocine_id,
    prod.release_date,
    prod.runtime,
    prod.allocine_genres_concat,
    prod.cast_concat,
    prod.countries_concat,
    prod.companies_concat,
    prod.directors_concat,
    base.item_id,
    base.subcategory_id,
    base.category_id,
    base.offer_name,
    base.offer_description,
    base.image,
    base.offer_creation_date,
    base.content_hash,
    base.to_embed,
    base.author_concat
from {{ ref("ml_input__all_items_metadata_to_embed") }} as base
left join product_metadata as prod on base.item_id = prod.item_id
where
    starts_with(base.item_id, 'product')
    and (
        (
            base.category_id = 'CINEMA'
            and base.subcategory_id
            in ('SEANCE_CINE', 'CINE_PLEIN_AIR', 'EVENEMENT_CINE', 'FESTIVAL_CINE')
        )
        or (
            base.category_id = 'FILM'
            and base.subcategory_id
            in ('SUPPORT_PHYSIQUE_FILM', 'VOD', 'AUTRE_SUPPORT_NUMERIQUE')
        )
    )
