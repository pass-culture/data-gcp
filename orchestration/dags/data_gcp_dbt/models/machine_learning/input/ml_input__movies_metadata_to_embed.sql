{{ config(materialized="view") }}

-- Movies Genres come from AlloCiné.
with
    allocine_dedup as (
        select movie_id, genres
        from {{ ref("snapshot_raw__allocine_movie") }}
        qualify
            row_number() over (partition by movie_id order by dbt_valid_from desc) = 1
    ),

    item_allocine_ids as (
        select
            link.item_id,
            max(
                case
                    when go.theater_movie_id is not null
                    then split(go.offer_id_at_providers, '%')[safe_offset(0)]
                end
            ) as allocine_movie_id
        from {{ ref("int_applicative__offer_item_id") }} as link
        inner join {{ ref("mrt_global__offer") }} as go on link.offer_id = go.offer_id
        group by link.item_id
    )

select
    base.item_id,
    base.subcategory_id,
    base.category_id,
    base.offer_name,
    base.offer_description,
    base.image,
    base.offer_creation_date,
    base.content_hash,
    base.to_embed,
    base.author_concat,
    nullif(array_to_string(allocine.genres, ', '), '') as allocine_genres_concat
from {{ ref("ml_input__all_items_metadata_to_embed") }} as base
left join item_allocine_ids as ids on base.item_id = ids.item_id
left join
    allocine_dedup as allocine
    on ids.allocine_movie_id = safe_cast(allocine.movie_id as string)
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
