SELECT
    user_id,
    user_address,
    user_city,
    user_postal_code,
    user_department_code,
    longitude,
    latitude,
    city_code,
    api_adresse_city,
    code_epci,
    epci_name,
    qpv_communes,
    qpv_name,
    code_qpv,
    zrr,
    date_updated,
    user_iris_internal_id,
    user_region_name,
FROM {{ ref("int_api_gouv__address_user_geo_iris") }}
QUALIFY ROW_NUMBER() over (PARTITION BY user_id ORDER BY date_updated DESC) = 1