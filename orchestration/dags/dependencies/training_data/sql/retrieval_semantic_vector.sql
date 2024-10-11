with
    k as (
        select ie.item_id, ie.hybrid_embedding,
        from `{{ bigquery_ml_preproc_dataset }}.item_embedding_reduced_32` ie
        inner join
            `{{ bigquery_ml_reco_dataset }}.recommendable_item` ri
            on ri.item_id = ie.item_id
    ),
    z as (
        select
            item_id,
            array(
                select cast(e as float64)
                from
                    unnest(
                        split(substr(hybrid_embedding, 2, length(hybrid_embedding) - 2))
                    ) e
            ) as embedding,
        from k
    )
select item_id, embedding
from z
