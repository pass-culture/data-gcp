select
    ed.institution_id,
    ed.scholar_year,
    ed.calendar_year,
    ei.institution_department_code,
    ei.institution_academy_name,
    ed.educational_deposit_period,
    ed.educational_deposit_amount
from {{ ref("int_applicative__educational_deposit") }} as ed
left join
    {{ ref("mrt_global__educational_institution") }} as ei
    on ed.institution_id = ei.institution_id
