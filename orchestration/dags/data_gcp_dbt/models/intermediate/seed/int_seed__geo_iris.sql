{{ config(**custom_table_config()) }}

SELECT
    iris_code,
    iris_label,
    city_code,
    city_label,
    territorial_authority_code,
    district_code,
    sub_district_code,
    epci_code,
    epci_label,
    sub_district_label,
    district_label,
    department_code,
    department_name,
    region_code,
    region_name,
    timezone,
    academy_name,
    territorial_authority_label,
    has_qpv,
    density_level,
    density_label,
    density_macro_level,
    geo_code,
    rural_city_type_data.geo_type as rural_city_type,
    iris_internal_id,
    ST_GEOGPOINT(
        CAST(SPLIT(REPLACE(iris_centroid, 'POINT(', ''), ' ')[ORDINAL(1)] AS FLOAT64),
        CAST(SPLIT(REPLACE(REPLACE(iris_centroid, 'POINT(', ''), ')', ''), ' ')[ORDINAL(2)] AS FLOAT64)
    ) AS iris_centroid,
    ST_GEOGFROMTEXT(iris_shape) AS iris_shape,
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT(iris_shape)).xmin min_longitude,
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT(iris_shape)).xmax AS max_longitude,
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT(iris_shape)).ymin AS min_latitude,
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT(iris_shape)).ymax AS max_latitude
FROM {{ source('seed', 'geo_iris') }} gi
LEFT JOIN {{ source('seed', 'rural_city_type_data') }} using(geo_code)
