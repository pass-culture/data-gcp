with venue_epci AS (
    SELECT
        venue_id,
        epci_code,
        epci_name
    FROM
        `{{ bigquery_clean_dataset }}.applicative_database_venue` venue
        JOIN (
            SELECT
                epci.epci_code,
                epci.epci_name,
                geo_shape
            FROM
                `{{ bigquery_analytics_dataset }}.epci` epci
        ) c on ST_CONTAINS(
            c.geo_shape,
            ST_GEOGPOINT(venue.venue_longitude, venue.venue_latitude)
        )
),
venue_qpv AS (
    SELECT
        venue_id,
        code_qpv,
        qpv_name,
        qpv_communes
    FROM
        `{{ bigquery_clean_dataset }}.applicative_database_venue` venue
        JOIN (
            SELECT
                code_quartier as code_qpv,
                noms_des_communes_concernees as qpv_name,
                commune_qp as qpv_communes,
                geoshape
            FROM
                `{{ bigquery_analytics_dataset }}.QPV`
        ) b ON ST_CONTAINS(
            b.geoshape,
            ST_GEOGPOINT(venue.venue_longitude, venue.venue_latitude)
        )
),
venue_zrr AS (
    SELECT
        venue_id,
        CODGEO,
        LIBGEO,
        ZRR_SIMP,
        ZONAGE_ZRR,
        Code_Postal,
        geo_shape_insee
    FROM
        `{{ bigquery_clean_dataset }}.applicative_database_venue` venue
        JOIN (
            SELECT
                ZRR.CODGEO,
                ZRR.LIBGEO,
                ZRR.ZRR_SIMP,
                ZRR.ZONAGE_ZRR,
                ZRR.Code_Postal,
                ZRR.geo_shape_insee
            FROM
                `{{ bigquery_analytics_dataset }}.ZRR` ZRR
        ) d on ST_CONTAINS(
            d.geo_shape_insee,
            ST_GEOGPOINT(venue.venue_longitude, venue.venue_latitude)
        )
)
SELECT
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
    venue_zrr.CODGEO,
    venue_zrr.LIBGEO,
    venue_zrr.ZRR_SIMP,
    venue_zrr.ZONAGE_ZRR,
    venue_zrr.Code_Postal
FROM
    `{{ bigquery_clean_dataset }}.applicative_database_venue` venue
    LEFT JOIN venue_epci ON venue.venue_id = venue_epci.venue_id
    LEFT JOIN venue_qpv ON venue.venue_id = venue_qpv.venue_id
    LEFT JOIN venue_zrr ON venue.venue_id = venue_zrr.venue_id
WHERE
    venue.venue_is_virtual is false;