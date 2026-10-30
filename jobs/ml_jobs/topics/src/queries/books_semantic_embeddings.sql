select
    item_id,
    array_slice(books_metadata_embedding, 0, 127) as embedding_128
from {project}.{embedding_dataset}.books_metadata
where item_id in unnest(@item_ids)
