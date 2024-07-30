{{
    config(
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "date_updated", "data_type": "datetime", "granularity" : "day"},
        on_schema_change = "sync_all_columns",
    )
) }}

WITH users_updated AS (
SELECT user_id,
    user_address,
    user_postal_code,
    user_department_code,
    longitude,
    latitude,
    city_code,
    api_adresse_city,
    qpv_name,
    code_qpv,
    zrr,
    date_updated
FROM {{ source("raw", "user_locations") }}
{% if is_incremental() %}
WHERE date_updated >= DATE("{{ ds() }}") - 1
{% endif %}
),

users_with_geo_candidates AS (
    SELECT
    u.*,
    gi.iris_internal_id AS user_iris_internal_id,
    gi.region_name AS user_region_name,
    gi.city_label AS user_city,
    gi.epci_label AS user_epci,
    gi.academy_name AS user_academy_name,
    gi.density_label AS user_density_label,
    gi.density_macro_level AS user_macro_density_label,
    gi.iris_shape
FROM users_updated AS u
LEFT JOIN {{ source("clean", "geo_iris") }} AS gi
    ON u.longitude BETWEEN gi.min_longitude AND gi.max_longitude
       AND u.latitude BETWEEN gi.min_latitude AND gi.max_latitude
)

SELECT
    * EXCEPT(iris_shape)
FROM users_with_geo_candidates
WHERE ST_CONTAINS(
            iris_shape,
            ST_GEOGPOINT(longitude, latitude)
    )
