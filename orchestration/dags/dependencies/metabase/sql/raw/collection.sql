select
    id as collection_id,
    name as collection_name,
    description,
    location,
    archived,
    personal_owner_id,
    slug
from public.collection
