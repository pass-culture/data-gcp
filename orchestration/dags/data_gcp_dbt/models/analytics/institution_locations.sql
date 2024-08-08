with qpv as (
    select
        code_quartier as code_qpv,
        noms_des_communes_concernees as qpv_name,
        commune_qp as qpv_communes,
        geoshape
    from
        {{ ref('int_seed__qpv') }}
    where geoshape is not NULL
),

institutions as (
    select
        id_etablissement as institution_id,
        nom_etablissement as institution_name,
        code_postal as institution_postal_code,
        code_departement as institution_department_code,
        CAST(latitude as float64) as institution_latitude,
        CAST(longitude as float64) as institution_longitude
    from
        {{ source('seed', 'institution_metadata') }}
),

institutions_qpv as (
    select
        institution_id,
        institution_name,
        institution_postal_code,
        institution_department_code,
        institution_latitude,
        institution_longitude,
        code_qpv as institution_qpv_code,
        qpv_name as institution_qpv_name,
        case
            when code_qpv is NULL and institution_latitude is NULL and institution_longitude is NULL then NULL
            else
                case
                    when code_qpv is not NULL then 1
                    else 0
                end
        end as institution_in_qpv
    from institutions
        left join qpv
            on ST_CONTAINS(
                    qpv.geoshape,
                    ST_GEOGPOINT(institution_longitude, institution_latitude)
                )
)

select
    iq.*,
    gi.city_label as institution_city,
    gi.region_name as institution_region_name,
    gi.epci_label as institution_epci,
    gi.academy_name as institution_academy_name,
    gi.density_label as institution_density_label,
    gi.density_macro_level as institution_macro_density_label,
    iris_internal_id as institution_internal_iris_id
from institutions_qpv as iq
    left join {{ ref('int_seed__geo_iris') }} as gi
        on ST_CONTAINS(
                gi.iris_shape,
                ST_GEOGPOINT(iq.institution_longitude, iq.institution_latitude)
            )
