-- Deprecated: will be removed once all models are migrated in Metabase
select
    tag_id,
    tag_name,
    entry_id,
    tag_key,
    tag_value,
    playlist_type
from {{ ref("int_contentful__tag") }}
