select
    vl.venue_id,
    vl.venue_street,
    vl.venue_latitude,
    vl.venue_longitude,
    vl.venue_department_code,
    vl.venue_postal_code,
    vl.venue_city,
    vl.venue_region_name,
    vl.qpv_code,
    vl.qpv_name,
    vl.venue_iris_internal_id
from {{ ref("int_geo__venue_location") }} as vl
inner join {{ ref("int_global__venue") }} as v on vl.venue_id = v.venue_id
