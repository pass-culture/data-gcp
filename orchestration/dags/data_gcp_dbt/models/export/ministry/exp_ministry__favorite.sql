SELECT
    favorite_id,
    favorite_creation_date,
    user_id,
    offer_id
FROM {{ ref("mrt_global__favorite") }}
