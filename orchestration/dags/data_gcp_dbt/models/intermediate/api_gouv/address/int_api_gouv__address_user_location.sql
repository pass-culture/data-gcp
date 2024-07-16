SELECT
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
FROM {{ ref("int_api_gouv__address_user_geo_iris") }}
QUALIFY ROW_NUMBER() over (PARTITION BY user_id ORDER BY date_updated DESC) = 1
