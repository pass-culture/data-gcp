select
    user_id,
    user_address,
    user_city,
    user_postal_code,
    user_department_code,
    longitude,
    latitude,
    city_code,
    qpv_name,
    code_qpv,
    zrr,
    date_updated,
    user_iris_internal_id,
    user_region_name,
    user_epci,
    user_academy_name,
    user_density_label,
    user_macro_density_label
from {{ ref("int_api_gouv__address_user_geo_iris") }}
qualify ROW_NUMBER() over (partition by user_id order by date_updated desc) = 1
