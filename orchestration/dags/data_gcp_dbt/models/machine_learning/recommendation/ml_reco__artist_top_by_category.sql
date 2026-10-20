{{ config(materialized="table") }}

-- Top 20 artists per search_group_name category, ranked by the "trending"
-- score from ml_metadata__artist_score. `score_name` is kept as a dimension
-- so the reco API can switch ranking strategies without a schema change;
-- add new variants as extra `union all` branches in
-- `artist_trending_score_by_score_name`.
-- Only artists with a bookable offer in their main category are eligible.
with
    artist_eligible_main_search_group_name_for_recommendation as (
        select artist_id, main_search_group_name as search_group_name
        from {{ ref("mrt_global__artist") }}
        where
            main_search_group_name is not null
            and main_search_group_name_has_bookable_offer
    ),

    artist_trending_score_by_score_name as (
        select
            artist_id,
            "consultations_plus_main_category_bookings_last_30d" as score_name,
            artist_score_log_consultations_plus_bookings_main_category_last_30d
            as score_value
        from {{ ref("ml_metadata__artist_score") }}
    ),

    artist_ranked_by_trending_score_within_category_and_score_name as (
        select
            ac.artist_id,
            ac.search_group_name,
            asr.score_name,
            asr.score_value,
            row_number() over (
                partition by ac.search_group_name, asr.score_name
                order by asr.score_value desc, ac.artist_id asc
            ) as category_rank
        from artist_eligible_main_search_group_name_for_recommendation as ac
        inner join
            artist_trending_score_by_score_name as asr on ac.artist_id = asr.artist_id
    )

select artist_id, search_group_name, score_name, score_value, category_rank
from artist_ranked_by_trending_score_within_category_and_score_name
where category_rank <= 20
order by search_group_name, score_name, category_rank
