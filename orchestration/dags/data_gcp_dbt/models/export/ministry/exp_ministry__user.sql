SELECT
    user_id,
    user_activity,
    user_civility,
    user_school_type,
    user_iris_internal_id,
FROM
    {{ ref('mrt_global__user') }}
TABLESAMPLE SYSTEM (1 PERCENT)
