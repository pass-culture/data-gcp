select
    id,
    created_at,
    updated_at,
    archived,
    archived_directly,
    name as document_name,
    creator_id as document_creator_id,
    collection_id as document_collection_id,
    content_type,
    last_viewed_at,
    view_count
from public.document
