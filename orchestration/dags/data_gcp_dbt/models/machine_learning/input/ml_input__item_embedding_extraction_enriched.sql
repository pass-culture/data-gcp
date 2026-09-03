{{
    config(
        **custom_table_config(
            materialized="table",
            cluster_by=["item_id"],
        )
    )
}}

-- Enriches ml_input__item_embedding_extraction with category-specific
-- semantic metadata (AlloCiné genres for movies, Titelive GTL classification
-- for books), packaged as a single extra_semantic_metadata JSON string
-- column shared across categories. This is the uniform envelope shape
-- consumed by jobs/ml_jobs/item_embedding's preprocessing.py
-- (format_movie_genres / format_book_classification):
-- {"movies": {"genres": [...]}}                                for category_id in
-- (FILM, CINEMA)
-- {"books": {"gtl1": ..., "gtl2": ..., "gtl3": ..., "gtl4": ...}} for category_id =
-- LIVRE
-- null                                                          otherwise
with
    allocine_dedup as (
        select movie_id, genres
        from {{ ref("snapshot_raw__allocine_movie") }}
        qualify
            row_number() over (partition by movie_id order by dbt_valid_from desc) = 1
    ),

    -- Aggregate offer-level provider ids per item_id to preserve item_id's
    -- primary key constraint (several offers/providers can share one item).
    item_provider_ids as (
        select
            link.item_id,
            max(
                case
                    when go.theater_movie_id is not null
                    then split(go.offer_id_at_providers, '%')[safe_offset(0)]
                end
            ) as allocine_movie_id,
            max(
                case
                    when go.offer_type_domain = 'BOOK'
                    then safe_cast(go.titelive_gtl_id as string)
                end
            ) as gtl_id
        from {{ ref("int_applicative__offer_item_id") }} as link
        inner join {{ ref("mrt_global__offer") }} as go on link.offer_id = go.offer_id
        group by link.item_id
    ),

    base_enriched as (
        select
            base.*,
            -- Keep the Allociné genre list only for FILM/CINEMA items.
            case
                when base.category_id in ('FILM', 'CINEMA') then allocine.genres
            end as allocine_genres,
            -- Keep the Titelive GTL id only for LIVRE items.
            case when base.category_id = 'LIVRE' then provider_ids.gtl_id end as gtl_id
        from {{ ref("ml_input__item_embedding_extraction") }} as base
        left join
            item_provider_ids as provider_ids on base.item_id = provider_ids.item_id
        left join
            allocine_dedup as allocine
            on provider_ids.allocine_movie_id = safe_cast(allocine.movie_id as string)
    ),

    cleaned as (
        select
            b.*,
            t.gtl_label_level_1 as gtl1,
            t.gtl_label_level_2 as gtl2,
            t.gtl_label_level_3 as gtl3,
            t.gtl_label_level_4 as gtl4
        from base_enriched as b
        left join
            {{ ref("int_applicative__titelive_gtl") }} as t
            on b.gtl_id = t.gtl_id
            and t.gtl_type = 'BOOK'
    )

select
    * except (allocine_genres, gtl_id, gtl1, gtl2, gtl3, gtl4),
    case
        when category_id in ('FILM', 'CINEMA')
        then to_json_string(struct(struct(allocine_genres as genres) as movies))
        when category_id = 'LIVRE'
        then
            to_json_string(
                struct(
                    struct(
                        gtl1 as gtl1, gtl2 as gtl2, gtl3 as gtl3, gtl4 as gtl4
                    ) as books
                )
            )
    end as extra_semantic_metadata
from cleaned
