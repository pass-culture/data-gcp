{{ config(materialized="view") }}

select artist_id, search_group_name, score_name, score_value, category_rank
from {{ ref("ml_reco__artist_top_by_category") }}
