with
    recommendable_item_embedding as (
        select item_embeddings.item_id, item_embeddings.hybrid_embedding,
        from {{ source("ml_preproc", "item_embedding_reduced_32") }} item_embeddings
        inner join
        from
            {{ ref("ml_reco__recommendable_item") }} recommendable_item
            on recommendable_item.item_id = item_embeddings.item_id
    ),
    recommendable_hybrid_embedding as (
        select
            item_id,
            array(
                select cast(e as float64)
                from
                    unnest(
                        split(substr(hybrid_embedding, 2, length(hybrid_embedding) - 2))
                    ) e
            ) as embedding,
        from recommendable_item_embedding
    )
select item_id, embedding
from recommendable_hybrid_embedding
