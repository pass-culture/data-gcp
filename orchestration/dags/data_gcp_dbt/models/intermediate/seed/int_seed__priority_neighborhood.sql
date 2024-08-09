{{ config(**custom_table_config()) }} 

SELECT 
    departement AS department_code,
    code_quartier AS code_qpv,
    commune_qp AS qpv_communes,
    noms_des_communes_concernees AS qpv_name,
    nom_reg AS region_name,
    nom_dep AS department_name,
    quartier_prioritaire AS priority_neighborhood,
    code_insee AS insee_code,
    ST_GEOGFROMTEXT(geoshape) AS geo_shape,
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT(geoshape)).xmin AS min_longitude,
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT(geoshape)).xmax AS max_longitude,
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT(geoshape)).ymin AS min_latitude,
    ST_BOUNDINGBOX(ST_GEOGFROMTEXT(geoshape)).ymax AS max_latitude
FROM {{ source('seed', 'qpv') }}

   