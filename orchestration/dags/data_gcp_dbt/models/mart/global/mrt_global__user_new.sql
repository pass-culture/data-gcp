SELECT *
FROM {{ ref('mrt_global__user_unverified') }}
WHERE is_beneficiary = TRUE