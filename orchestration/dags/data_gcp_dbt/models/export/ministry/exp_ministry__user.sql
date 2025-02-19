select user_id, user_activity, user_civility, user_school_type, user_iris_internal_id,
from {{ ref("mrt_global__user") }} tablesample system(1 percent)
