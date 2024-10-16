{{ config(**custom_table_config()) }}

select
    bv2012_name as living_area_2012_name,
    reg_name as region_name,
    dep_code as department_code,
    com_uu2010_code as urban_unit_2010_code,
    com_type as municipality_type,
    year as year,
    com_cv_code as canton_code,
    com_cataeu2010_name as urban_area_2010_name,
    ze2010_name as employment_zone_2010_name,
    com_code as municipality_code,
    dep_name as department_name,
    com_tduu2017_code as urban_district_2017_code,
    com_name_upper as municipality_name_uppercase,
    com_tuu2017_name as urban_district_2017_name,
    arrdep_code as district_code,
    ze2010_code as employment_zone_2010_code,
    com_cataeu2010_code as urban_area_2010_code,
    com_is_mountain_area as is_mountain_area,
    com_tau2017_code as urban_area_2017_code,
    com_area_code as municipality_area_code,
    com_current_code as current_municipality_code,
    epci_code,
    epci_name,
    st_geogfromtext(geo_shape) as geo_shape,
    st_boundingbox(st_geogfromtext(geo_shape)).xmin as min_longitude,
    st_boundingbox(st_geogfromtext(geo_shape)).xmax as max_longitude,
    st_boundingbox(st_geogfromtext(geo_shape)).ymin as min_latitude,
    st_boundingbox(st_geogfromtext(geo_shape)).ymax as max_latitude
from {{ source("seed", "epci") }}
