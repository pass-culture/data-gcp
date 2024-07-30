with venue_epci as (
    select
        venue_id,
        epci_code,
        epci_name
    from
        {{ ref('venue') }} venue
        join (
            select
                epci.epci_code,
                epci.epci_name,
                geo_shape
            from
                {{ source('analytics','epci') }} epci
        ) c on ST_CONTAINS(
                c.geo_shape,
                ST_GEOGPOINT(venue.venue_longitude, venue.venue_latitude)
            )
),

venue_qpv as (
    select
        venue_id,
        code_qpv,
        qpv_name,
        qpv_communes
    from
        {{ ref('venue') }} venue
        join (
            select
                code_quartier as code_qpv,
                noms_des_communes_concernees as qpv_name,
                commune_qp as qpv_communes,
                geoshape
            from
                {{ source('analytics','QPV') }}
        ) b on ST_CONTAINS(
                b.geoshape,
                ST_GEOGPOINT(venue.venue_longitude, venue.venue_latitude)
            )
),

venue_zrr as (
    select
        venue_id,
        codgeo,
        libgeo,
        zrr_simp,
        zonage_zrr,
        code_postal,
        geo_shape_insee
    from
        {{ ref('venue') }} venue
        join (
            select
                zrr.codgeo,
                zrr.libgeo,
                zrr.zrr_simp,
                zrr.zonage_zrr,
                zrr.code_postal,
                zrr.geo_shape_insee
            from
                {{ source('analytics','ZRR') }} zrr
        ) d on ST_CONTAINS(
                d.geo_shape_insee,
                ST_GEOGPOINT(venue.venue_longitude, venue.venue_latitude)
            )
)

select
    venue.venue_id,
    venue.venue_city,
    venue.venue_postal_code,
    venue.venue_department_code,
    venue.venue_latitude,
    venue.venue_longitude,
    venue_epci.epci_name,
    venue_epci.epci_code,
    venue_qpv.code_qpv,
    venue_qpv.qpv_name,
    venue_qpv.qpv_communes,
    venue_zrr.codgeo,
    venue_zrr.libgeo,
    venue_zrr.zrr_simp,
    venue_zrr.zonage_zrr,
    venue_zrr.code_postal
from
    {{ ref('venue') }} venue
    left join venue_epci on venue.venue_id = venue_epci.venue_id
    left join venue_qpv on venue.venue_id = venue_qpv.venue_id
    left join venue_zrr on venue.venue_id = venue_zrr.venue_id
where
    venue.venue_is_virtual is false
