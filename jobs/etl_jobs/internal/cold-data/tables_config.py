ref_tables = {
    "macro_rayons": {
        "file_type": "csv",
        "schema": {"index": "INTEGER", "macro_rayon": "STRING", "rayon": "STRING"},
    },
    "eac_cash_in": {
        "file_type": "csv",
        "schema": {
            "ministry": "STRING",
            "date_update": "DATE",
            "cash_in": "FLOAT",
        },
    },
    "titelive_isbn_weight": {
        "file_type": "csv",
        "schema": {
            "EAN13": "STRING",
            "POIDS": "INTEGER",
            "LONGUEUR": "INTEGER",
            "LARGEUR": "INTEGER",
        },
    },
    "institutional_partners": {
        "file_type": "csv",
        "schema": {
            "institutional_offerer_id": "STRING",
            "zendesk_id": "STRING",
            "institutional_offerer_name": "STRING",
            "city": "STRING",
            "region_name": "STRING",
            "institutional_offerer_type": "STRING",
            "related_team_pass": "STRING",
        },
    },
    "festival_increments": {
        "file_type": "csv",
        "schema": {
            "offerer_id": "STRING",
            "festival_cnt": "INTEGER",
            "offerer_tag": "STRING",
        },
    },
    "propilote_zones_ref": {
        "file_type": "csv",
        "schema": {
            "zone_id": "STRING",
            "nom": "STRING",
            "zone_code": "STRING",
            "zone_type": "STRING",
            "zone_parent": "STRING",
        },
    },
    "forbidden_query_recommendation": {
        "file_type": "csv",
        "schema": {
            "subcategory_id": "STRING",
            "query": "STRING",
        },
    },
    "forbidden_offers_recommendation": {
        "file_type": "csv",
        "schema": {
            "product_id": "STRING",
        },
    },
    "departmental_objectives": {
        "file_type": "csv",
        "schema": {
            "objective_name": "STRING",
            "objective_type": "STRING",
            "region_name": "STRING",
            "department_code": "STRING",
            "objective": "INTEGER",
        },
    },
    "agg_partner_cultural_sector": {
        "file_type": "csv",
        "schema": {
            "partner_type": "STRING",
            "cultural_sector": "STRING",
        },
    },
    "pilote_geographic_standards": {
        "file_type": "csv",
        "schema": {
            "zone_id": "STRING",
            "nom": "STRING",
        },
    },
    "rural_city_type_data": {
        "file_type": "csv",
        "schema": {
            "geo_code": "STRING",
            "geo_type": "STRING",
        },
    },
    "eple": {
        "file_type": "csv",
        "schema": {
            "id_etablissement": "STRING",
            "nom_etablissement": "STRING",
            "type_etablissement": "STRING",
            "statut_public_prive": "STRING",
            "adresse_1": "STRING",
            "adresse_2": "STRING",
            "adresse_3": "STRING",
            "code_postal": "STRING",
            "code_commune": "STRING",
            "nom_commune": "STRING",
            "code_departement": "STRING",
            "code_academie": "STRING",
            "code_region": "STRING",
            "ecole_maternelle": "BOOLEAN",
            "ecole_elementaire": "BOOLEAN",
            "voie_generale": "BOOLEAN",
            "voie_technologique": "BOOLEAN",
            "voie_professionnelle": "BOOLEAN",
            "telephone": "STRING",
            "mail": "STRING",
            "ulis": "BOOLEAN",
            "apprentissage": "STRING",
            "segpa": "BOOLEAN",
            "section_arts": "BOOLEAN",
            "section_cinema": "BOOLEAN",
            "section_theatre": "BOOLEAN",
            "section_sport": "BOOLEAN",
            "section_internationale": "BOOLEAN",
            "section_europeenne": "BOOLEAN",
            "lycee_agricole": "BOOLEAN",
            "lycee_militaire": "BOOLEAN",
            "lycee_des_metiers": "BOOLEAN",
            "post_bac": "BOOLEAN",
            "appartenance_education_prioritaire": "STRING",
            "greta": "STRING",
            "siren_siret": "STRING",
            "nb_eleves": "INTEGER",
            "type_contrat_prive": "STRING",
            "libelle_departement": "STRING",
            "libelle_academie": "STRING",
            "libelle_region": "STRING",
            "latitude": "STRING",
            "longitude": "STRING",
            "date_ouverture": "DATE",
            "date_maj_ligne": "DATE",
            "etat": "STRING",
            "ministere_tutelle": "STRING",
            "code_nature": "STRING",
            "libelle_nature": "STRING",
            "code_type_contrat_prive": "STRING",
        },
    },
    "institution_scholar_level ": {
        "file_type": "csv",
        "schema": {
            "n_ms4_id": "STRING",
            "n_ms4_cod": "STRING",
            "n_ms4_lib": "STRING",
        },
    },
}
