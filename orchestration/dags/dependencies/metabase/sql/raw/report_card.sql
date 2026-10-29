select
    id,
    created_at,
    updated_at,
    archived,
    name as card_name,
    description as card_description,
    creator_id as card_creator_id,
    collection_id as card_collection_id,
    document_id as card_document_id,
    query_type,
    dataset_query
from public.report_card
