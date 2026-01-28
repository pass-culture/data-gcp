select offer_id, completion_score from {{ ref("ml_metadata__offer_quality") }}
