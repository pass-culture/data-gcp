select
    replace('nan', null, school_year) as school_year,
    ministry,
    uai as institution_id,
    safe_cast(is_provisional as bool) as is_provisional,
    class,
    safe_cast(amount_per_student as float64) as amount_per_student,
    safe_cast(headcount as float64) as headcount,
from {{ source("raw", "gsheet_educational_institution_student_headcount") }}
qualify
    row_number() over (
        partition by ministry, uai, class
        order by
            cast(split(school_year, "-")[0] as int) desc,
            cast(is_provisional as int) desc
    )
    = 1
