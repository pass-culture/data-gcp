SELECT
    *
FROM  {{ source("clean", "dms_pro_cleaned") }}