select *
from {{ source('raw', 'applicative_database_offerer_tag_category_mapping') }}
