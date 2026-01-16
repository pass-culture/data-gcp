select
    ed.educational_deposit_id,
    ed.institution_id,
    ed.educational_year_id,
    ed.scholar_year,
    ed.calendar_year,
    ed.educational_deposit_period,
    ei.institution_department_code,
    ei.institution_academy_name,
    ed.educational_deposit_amount,
    ed.is_current_scholar_year,
    ed.is_current_calendar_year,
    ed.is_current_deposit
from {{ ref("int_applicative__educational_deposit") }} as ed
left join
    {{ ref("mrt_global__educational_institution") }} as ei
    on ed.institution_id = ei.institution_id
