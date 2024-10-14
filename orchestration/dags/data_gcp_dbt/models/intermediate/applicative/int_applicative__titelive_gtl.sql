select
    case
        when length(cast(gtl_id as string)) = 7
        then concat('0', cast(gtl_id as string))
        else cast(gtl_id as string)
    end as gtl_id,
    upper(gtl_type) as gtl_type,
    gtl_label_level_1,
    gtl_label_level_2,
    gtl_label_level_3,
    gtl_label_level_4
from {{ source("raw", "applicative_database_titelive_gtl") }}
