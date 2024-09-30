{{ config(**custom_table_config()) }} 


with user_epci as (
    {{ generate_seed_geolocation_query(
        source_table='int_api_gouv__user_address', 
        referential_table='int_seed__intercommunal_public_institution', 
        id_column='user_id',
        prefix_name='user',
        columns=['epci_code', 'epci_name']
        )
    }}
),

user_qpv as (
    {{ generate_seed_geolocation_query(
        source_table='int_api_gouv__user_address', 
        referential_table='int_seed__priority_neighborhood', 
        id_column='user_id',
        prefix_name='user',
        columns=['code_qpv', 'qpv_name', 'qpv_communes']
        )
    }}
),

user_zrr as (
    {{ generate_seed_geolocation_query(
        source_table='int_api_gouv__user_address', 
        referential_table='int_seed__rural_revitalization_zone',
        id_column='user_id',
        prefix_name='user',
        columns=['zrr_level', 'zrr_level_detail']
    )
    }}
),

user_geo_iris as (
    {{ generate_seed_geolocation_query(
        source_table='int_api_gouv__user_address', 
        referential_table='int_seed__geo_iris',
        id_column='user_id',
        prefix_name='user',
        columns=['iris_internal_id','region_name','city_label','city_code','rural_city_type','academy_name', department_name','density_label','density_macro_level','density_level'],
        geo_shape='iris_shape'
    )
    }}
)


select
    user.user_id,
    user.user_address,
    user.user_postal_code,
    user.user_department_code,
    user.user_longitude,
    user.user_latitude,
    user_geo_iris.iris_internal_id as user_iris_internal_id,
    user_geo_iris.city_label as user_city,
    user_geo_iris.city_code as user_city_code,
    user_geo_iris.rural_city_type as user_rural_city_type,
    user_geo_iris.density_label as user_density_label,
    user_geo_iris.density_macro_level as user_macro_density_label,
    user_geo_iris.density_level AS user_density_level,
    user_geo_iris.academy_name as user_academy_name,
    user_geo_iris.department_name AS user_department_name,
    user_geo_iris.region_name as user_region_name,
    user_epci.epci_name as user_epci,
    user_epci.epci_code,
    user_qpv.code_qpv,
    user_qpv.qpv_name,
    user_qpv.qpv_communes,
    user_zrr.zrr_level,
    user_zrr.zrr_level_detail,
    case
        when 
            code_qpv is NULL and
            user_latitude is NULL and 
            user_longitude is NULL 
        then NULL
    else code_qpv is not NULL
    end as user_is_in_qpv,
    date_updated,

FROM {{ ref("int_api_gouv__user_address") }} user
left join user_epci on user.user_id = user_epci.user_id
left join user_qpv on user.user_id = user_qpv.user_id
left join user_zrr on user.user_id = user_zrr.user_id
left join user_geo_iris on user.user_id = user_geo_iris.user_id

