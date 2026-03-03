with
    artist_with_bio as (
        select
            artist_id,
            artist_name,
            artist_biography,
            wikidata_id,
            artist_app_search_score
        from {{ ref("int_applicative__artist") }}
        where
            artist_biography is not null
            and artist_biography != ''
            and artist_app_search_score > 0
    ),

    products_on_artists_with_bio as (
        select
            artist_with_bio.artist_id,
            pal.offer_product_id,
            concat('product-', pal.offer_product_id) as item_id
        from artist_with_bio
        inner join
            {{ ref("int_applicative__product_artist_link") }} as pal using (artist_id)
    ),

    unnested_embeddings_per_artist_with_bio as (
        select
            products.artist_id,
            offset_val,  -- noqa: RF01, RF02
            avg(embedding_component) as avg_embedding_component
        from products_on_artists_with_bio as products
        inner join
            {{ ref("ml_feat__two_tower_last_item_embedding") }} as e using (item_id),
            unnest(e.item_embedding) as embedding_component
        with
        offset as offset_val
        group by products.artist_id, offset_val  -- noqa: RF01, RF02
    ),

    tt_embeddings_per_artist_with_bio as (
        select
            artist_id,
            array_agg(
                avg_embedding_component order by offset_val
            ) as mean_tt_item_embedding
        from unnested_embeddings_per_artist_with_bio
        group by artist_id
    )

select
    artist_with_bio.artist_id,
    artist_with_bio.artist_name,
    artist_with_bio.artist_biography,
    artist_with_bio.wikidata_id,
    artist_with_bio.artist_app_search_score,
    tt.mean_tt_item_embedding
from artist_with_bio
left join tt_embeddings_per_artist_with_bio as tt using (artist_id)
