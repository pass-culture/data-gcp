-- Matryoshka (MRL) truncation of the 768-dim embedding-gemma-300m vector to its
-- first 128 dimensions. array_slice end_offset is INCLUSIVE, so (0, 127) yields
-- exactly 128 elements
select
    ie.item_id,
    ie.content_hash,
    ie.mlflow_run_id,
    ie.embedding_model,
    array_slice(ie.embedding, 0, 127) as all_items_metadata_embedding_128
from {{ ref("ml_semantic_embedding__all_items_metadata") }} as ie
