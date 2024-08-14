{{ config(**custom_table_config()) }} 

with venue_epci as (
    {{ generate_seed_geolocation_query(
        source_table=['raw', 'applicative_database_venue'], 
        referential_table='int_seed__intercommunal_public_institution', 
        id_column='venue_id',
        prefix_name='venue',
        columns=['epci_code', 'epci_name']
        )
    }}
),

venue_qpv as (
    {{ generate_seed_geolocation_query(
        source_table=['raw', 'applicative_database_venue'], 
        referential_table='int_seed__priority_neighborhood', 
        id_column='venue_id',
        prefix_name='venue',
        columns=['code_qpv', 'qpv_name', 'qpv_communes']
        )
    }}
),

venue_zrr as (
    {{ generate_seed_geolocation_query(
        source_table=['raw', 'applicative_database_venue'], 
        referential_table='int_seed__rural_revitalization_zone',
        id_column='venue_id',
        prefix_name='venue',
        columns=['zrr_level', 'zrr_level_detail', 'is_in_zrr']
    )
    }}
),

venue_geo_iris as (
    {{ generate_seed_geolocation_query(
        source_table=['raw', 'applicative_database_venue'], 
        referential_table='int_seed__geo_iris',
        id_column='venue_id',
        prefix_name='venue',
        columns=['iris_internal_id','region_name','city_label','city_code','rural_city_type','academy_name','density_label','density_macro_level','density_level'],
        geo_shape='iris_shape'
    )
    }}
)

select
    venue.venue_id,
    venue.venue_postal_code,
    venue.venue_department_code,
    venue.venue_latitude,
    venue.venue_longitude,
    venue_geo_iris.iris_internal_id as venue_iris_internal_id,
    venue_geo_iris.city_label as venue_city,
    venue_geo_iris.city_code as venue_city_code,
    venue_geo_iris.rural_city_type as venue_rural_city_type,
    venue_geo_iris.density_label as venue_density_label,
    venue_geo_iris.density_macro_level as venue_macro_density_label,
    venue_geo_iris.density_level AS venue_density_level,
    venue_geo_iris.academy_name as venue_academy_name,
    venue_geo_iris.region_name as venue_region_name,
    venue_epci.epci_name as venue_epci,
    venue_epci.epci_code,
    venue_qpv.code_qpv,
    venue_qpv.qpv_name,
    venue_qpv.qpv_communes,
    venue_zrr.zrr_level,
    venue_zrr.zrr_level_detail,
    venue_zrr.is_in_zrr as venue_in_zrr,
    case
        when 
            code_qpv is NULL and
            venue_latitude is NULL and 
            venue_longitude is NULL 
        then NULL
    else code_qpv is not NULL
    end as venue_in_qpv,
    
    
from
    {{ source('raw', 'applicative_database_venue') }} venue
    left join venue_epci on venue.venue_id = venue_epci.venue_id
    left join venue_qpv on venue.venue_id = venue_qpv.venue_id
    left join venue_zrr on venue.venue_id = venue_zrr.venue_id
    left join venue_geo_iris on venue.venue_id = venue_geo_iris.venue_id
where
    venue.venue_is_virtual is false
