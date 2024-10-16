select
    ed.institution_id,
    ed.educational_year_id,
    ei.institution_department_code,
    ei.institution_academy_name
from {{ ref("int_applicative__educational_deposit") }} as ed
left join
    {{ ref("mrt_global__educational_institution") }} as ei
    on ed.institution_id = ei.institution_id
