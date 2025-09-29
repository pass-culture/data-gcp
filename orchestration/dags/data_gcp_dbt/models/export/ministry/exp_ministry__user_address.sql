select
    u.user_id,
    ul.user_address_geocode_type,
    ul.user_address_raw,
    ul.user_latitude as user_address_latitude,
    ul.user_longitude as user_address_longitude,
    ul.user_academy_name,
    ul.user_department_code,
    ul.user_region_name,
    ul.qpv_code as user_qpv_code,
    ul.qpv_name as user_qpv_name,
    ul.user_postal_code,
    ul.user_iris_internal_id
from {{ ref("int_geo__user_location") }} as ul
inner join {{ ref("int_global__user_beneficiary") }} as u on ul.user_id = u.user_id
