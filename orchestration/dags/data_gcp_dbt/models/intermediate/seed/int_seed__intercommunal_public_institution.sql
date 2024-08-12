{{ config(**custom_table_config()) }} 

SELECT 
    bv2012_name AS living_area_2012_name,
    reg_name AS region_name,
    dep_code AS department_code,
    com_uu2010_code AS urban_unit_2010_code,
    com_type AS municipality_type,
    year AS year,
    com_cv_code AS canton_code,
    com_cataeu2010_name AS urban_area_2010_name,
    ze2010_name AS employment_zone_2010_name,
    com_code AS municipality_code,
    dep_name AS department_name,
    com_tduu2017_code AS urban_district_2017_code,
    com_name_upper AS municipality_name_uppercase,
    com_tuu2017_name AS urban_district_2017_name,
    arrdep_code AS district_code,
    ze2010_code AS employment_zone_2010_code,
    com_cataeu2010_code AS urban_area_2010_code,
    com_is_mountain_area AS is_mountain_area,
    com_tau2017_code AS urban_area_2017_code,
    com_area_code AS municipality_area_code,
    com_current_code AS current_municipality_code,
    epci_code,
    epci_name,
    ST_GEOGFROMTEXT(geo_shape) AS geo_shape,
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT(geo_shape)).xmin AS min_longitude,
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT(geo_shape)).xmax AS max_longitude,
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT(geo_shape)).ymin AS min_latitude,
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT(geo_shape)).ymax AS max_latitude
FROM {{ source('seed', 'epci') }}