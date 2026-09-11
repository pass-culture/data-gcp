{{ config(materialized="view") }}

select offer_id, search_group_name
from {{ ref("mrt_global__offer_metadata") }}
