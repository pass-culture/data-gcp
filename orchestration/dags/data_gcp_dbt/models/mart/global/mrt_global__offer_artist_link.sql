{{ config(**custom_table_config(cluster_by="offer_id")) }}

select offer_artist_link_id, offer_id, artist_id, artist_type, artist_name
from {{ ref("int_applicative__offer_artist_link") }}
