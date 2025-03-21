select
    r.end_date as response_date,
    v.venue_id,
    r.response_id,
    v.venue_region_name,
    v.venue_type_label,
    v.venue_is_permanent,
    safe_cast(r.answer as int64) as response_rating,
    date_diff(date(r.end_date), v.venue_creation_date, day) as venue_seniority
from {{ ref("int_qualtrics__nps_venue_answer") }} as r
inner join {{ ref("mrt_global__venue") }} as v on r.venue_id = v.venue_id

where r.is_nps_question = true
