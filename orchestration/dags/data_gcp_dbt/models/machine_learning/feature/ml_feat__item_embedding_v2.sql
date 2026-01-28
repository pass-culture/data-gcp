select
    ie.item_id,
    ie.image_embedding,
    ie.name_embedding,
    ie.description_embedding,
    ie.semantic_content_embedding,
    ie.semantic_content_hybrid_embedding,
    ie.label_embedding,
    ie.label_hybrid_embedding,
    ie.extraction_date,
    ie.extraction_datetime
from {{ source("ml_preproc", "item_embedding_extraction_v2") }} ie
inner join {{ ref("ml_input__item_metadata") }} im on ie.item_id = im.item_id

qualify
    row_number() over (partition by ie.item_id order by ie.extraction_datetime desc) = 1
