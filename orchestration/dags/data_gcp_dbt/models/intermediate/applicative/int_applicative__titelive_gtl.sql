select
    case
        when LENGTH(CAST(gtl_id as string)) = 7 then CONCAT('0', CAST(gtl_id as string))
        else CAST(gtl_id as string)
    end as gtl_id,
    UPPER(gtl_type) as gtl_type,
    gtl_label_level_1,
    gtl_label_level_2,
    gtl_label_level_3,
    gtl_label_level_4
from {{ source('raw','applicative_database_titelive_gtl') }}
