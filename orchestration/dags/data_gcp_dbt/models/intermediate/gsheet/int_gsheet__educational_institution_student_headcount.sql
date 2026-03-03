select
    school_year as scholar_year,
    ministry,
    uai as institution_id,
    class,
    safe_cast(is_provisional as bool) as is_provisional,
    safe_cast(amount_per_student as float64) as amount_per_student,
    safe_cast(headcount as float64) as headcount
from {{ source("raw", "gsheet_educational_institution_student_headcount") }}
qualify
    row_number() over (
        partition by ministry, uai, class
        order by
            cast(split(school_year, "-")[0] as int) desc,
            cast(is_provisional as int) desc
    )
    = 1
