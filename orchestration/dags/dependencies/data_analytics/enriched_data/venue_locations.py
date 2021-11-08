def define_table_venue_locations(dataset, table_prefix=""):
    return f"""
    SELECT 
            venue.venue_id,
            venue.venue_city,
            venue.venue_postal_code,
            venue.venue_department_code,
            venue.venue_latitude,
            venue.venue_longitude,
            code_quartier as code_qpv,
            noms_des_communes_concernees as qpv_name,
            commune_qp as qpv_communes,
            epci.epci_code,
            epci.epci_name,
            ZRR.CODGEO,
            ZRR.LIBGEO,
            ZRR.ZRR_SIMP,
            ZRR.ZONAGE_ZRR,
        FROM `{dataset}.QPV`, `{dataset}.{table_prefix}venue` venue, `{dataset}.epci` epci, `{dataset}.ZRR` ZRR,`{dataset}.correspondance_code_insee` table_insee
        where ST_CONTAINS(geoshape, ST_GEOGPOINT(venue.venue_longitude,venue.venue_latitude)) is true 
              and venue.venue_latitude>-90 and venue.venue_latitude <90 and venue.venue_longitude >-90 
              and venue.venue_longitude <90 and ST_CONTAINS(epci.geo_shape, ST_GEOGPOINT(venue.venue_longitude,venue.venue_latitude)) is true
              and ST_CONTAINS(table_insee.geo_shape_insee, ST_GEOGPOINT(venue.venue_longitude,venue.venue_latitude)) is true
              and cast(ZRR.CODGEO as string) = table_insee.insee_com;
    """
