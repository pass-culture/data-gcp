SELECT 
bv2012_name,
reg_name,
dep_code,
com_uu2010_code,
com_type,
year,
com_cv_code,
com_cataeu2010_name,
ze2010_name,
com_code,
dep_name,
com_tduu2017_code,
com_name_upper,
com_tuu2017_name,
arrdep_code,
ze2010_code,
com_cataeu2010_code,
com_is_mountain_area,
com_tau2017_code,
com_area_code,
com_current_code,
epci_code,
epci_name,
    ST_GEOGFROMTEXT(geo_shape) AS geo_shape
FROM {{ source('seed', 'epci') }}