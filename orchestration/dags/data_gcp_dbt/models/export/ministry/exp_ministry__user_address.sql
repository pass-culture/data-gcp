select
    u.user_id,
    ul.user_address_geocode_type,
    ul.user_address_raw,
    ul.user_latitude as user_address_latitude,
    ul.user_longitude as user_address_longitude,
    ul.user_academy_name,
    ul.user_department_code,
    ul.user_region_name,
    ul.user_qpv_code,
    ul.user_qpv_name,
    ul.user_postal_code

from {{ ref("int_geo__user_location") }} as ul
inner join {{ ref("mrt_global__user") }} as u on ul.user_id = u.user_id
