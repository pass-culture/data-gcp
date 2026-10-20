{{ config(materialized="view") }}

select artist_id, main_search_group_name, main_search_group_name_has_bookable_offer
from {{ ref("mrt_global__artist") }}
