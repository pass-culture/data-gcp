-- Matryoshka (MRL) truncation of the 768-dim embedding-gemma-300m vector to its
-- first 128 dimensions. array_slice end_offset is INCLUSIVE, so (0, 127) yields
-- exactly 128 elements
select
    ie.item_id,
    ie.content_hash,
    array_slice(ie.semantic_content, 0, 127) as semantic_content_128
from {{ ref("ml_feat__item_embedding_refactor") }} as ie
