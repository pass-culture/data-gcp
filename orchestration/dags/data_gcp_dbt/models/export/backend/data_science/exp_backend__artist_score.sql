with
    new_ml_scores as (
        select artist_id, artist_app_search_score, artist_pro_search_score
        from {{ ref("ml_metadata__artist_score") }}
    ),

    current_applicative_scores as (
        select artist_id, artist_app_search_score, artist_pro_search_score
        from {{ ref("int_applicative__artist") }}
    )

select
    new_ml_scores.artist_id,
    new_ml_scores.artist_app_search_score,
    new_ml_scores.artist_pro_search_score
from new_ml_scores
inner join
    current_applicative_scores
    on new_ml_scores.artist_id = current_applicative_scores.artist_id
where
    new_ml_scores.artist_app_search_score
    is distinct from current_applicative_scores.artist_app_search_score
    or new_ml_scores.artist_pro_search_score
    is distinct from current_applicative_scores.artist_pro_search_score
