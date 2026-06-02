with
    new_ml_scores as (
        select offer_id, completion_score from {{ ref("ml_metadata__offer_quality") }}
    ),

    current_applicative_scores as (
        select offer_id, completion_score
        from {{ source("raw", "applicative_database_offer_quality") }}
    )

select
    new_ml_scores.offer_id,
    new_ml_scores.completion_score,
    if(current_applicative_scores.offer_id is not null, 'update', 'add') as action
from new_ml_scores
left join
    current_applicative_scores
    on new_ml_scores.offer_id = current_applicative_scores.offer_id
where
    current_applicative_scores.offer_id is null
    or new_ml_scores.completion_score != current_applicative_scores.completion_score
